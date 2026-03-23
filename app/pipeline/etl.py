from app.extract.specialisterne import SpecAPI
from app.extract.new_specialisterne import NewSpecAPI
from app.extract.dmi import DMIAPI
from app.transform.transform import SpecDataTransformer, DMIDataTransformer
from app.load.db.CRUD import CRUD
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
import threading


class ETLProcess:
    def __init__(self, docker: bool = False):
        self.crud = CRUD(docker)


    def docker_etl_background(self, interval_minutes: int = 10):
        interval_seconds = interval_minutes * 60

        print(f"ETL running every {interval_minutes} minutes.")

        while True:
            try:
                print("Starting ETL run.")
                self.update_database()
            except Exception as e:
                print("ETL error:", e)

            time.sleep(interval_seconds)

    def update_database(self):
        """This method handles all the various ETLs"""
        times_dict = self.get_start_times()

        # This handles the old specialisterne etl
        # self.spec_etl(from_time=times_dict["spec"])

        # The rest handles the dmi etl
        stations_file = "station_ids.json"
        base_path = Path(__file__).resolve().parent
        file_path = (base_path / ".." / "load" / "schemas" / stations_file).resolve()
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                stations = json.load(f)
        except FileNotFoundError:
            raise RuntimeError(f"Required file missing: {stations_file} in path {file_path}")

        params = [
            "temp_dry",
            "humidity",
            "pressure"
        ]
        for station in stations:
            for parameterId in params:
                self.dmi_etl(stations[station], parameterId, from_time=times_dict["DMI"][parameterId])

        times_dict = self.get_start_times()
        #self.new_spec_etl(from_time=times_dict["spec"])


    def dmi_etl(self, station_id, parameter_id, from_time: str = "2026-03-09T00:00:00Z", max_pulls: int = None,
                limit: int = 5000):
        api = DMIAPI()
        start_time = time.time()
        total_pulls = 0
        offset = 0
        transformer = DMIDataTransformer()

        print(f"Pulling {parameter_id} data with stationid {station_id} from the DMI API")
        while True:
            pull_time, records = api.pull_datetime(station_id=station_id, parameter_id=parameter_id, limit=limit,
                                                   start_time=from_time, offset=offset)
            if not records:
                print("No more new records.")
                break

            # We set an offset. The DMI api pulls newest record first,
            offset += limit
            # This from_time is to be stored in an external JSON. It will be used for telling the etl process where to start next time it is run.
            from_time = self.advance_timestamp(max(r["properties"]["observed"] for r in records))

            db_dict = transformer.dmi_data_to_db_dict(pull_time, records)
            self.crud.create_mult_rows("DMI", db_dict, commit=True, close=False)

            elapsed_time = time.time() - start_time
            print(f"{limit} records pulled. Exporting pull times to json. Elapsed time: {elapsed_time}")

            self.export_start_times(from_time, "DMI", parameter_id)

            elapsed_time = time.time() - start_time
            print(f"json exported. Pulling next {limit}. Elapsed time: {elapsed_time}")
            total_pulls += 1
            if self.check_max_vs_total_pulls(max_pulls,total_pulls,start_time):
                break
        self.crud.db.close()

    def spec_etl(self, from_time: str = "2026-03-09T00:00:00Z", max_pulls: int = None, limit: int = 5000):
        api = SpecAPI()
        start_time = time.time()
        total_pulls = 0
        avoid_ids = set()
        transformer = SpecDataTransformer()

        print("Pulling data from the Specialisterne API")
        while True:
            pull_time, records = api.pull_from(limit=limit, from_time=from_time)

            if avoid_ids:
                records = self.remove_rows_by_id(records, avoid_ids)
            if not records:
                print("No more new records.")
                break
            # We create a new timestamp and will pull the next entries from there.
            # The timestamps of the BME280 and DS18B20 do not fully line up.
            # So we take the min of these and increment by a millisecond.
            # This means we will get a duplicate row (one entry per reader) when making the next pull.

            last_bme, last_ds = self.get_last_bme_and_ds(records)
            from_time = self.advance_timestamp(min(last_bme["timestamp"],last_ds["timestamp"]))

            # We explicitly make sure to remove those duplicates.
            # This is not necessary for the database, as the create_mult_rows method skips duplicates.
            # HOWEVER, it is necessary to ensure the process stops.
            # Indeed, if we don't remove them, there records will always have at least two records, and the process will hang.
            avoid_ids = {last_bme["id"], last_ds["id"]}

            db_dict = transformer.spec_data_to_db_dict(pull_time, records)
            for table_name in db_dict:
                self.crud.create_mult_rows(table_name, db_dict[table_name], commit=True, close=False)

            elapsed_time = time.time() - start_time
            print(f"5000 records pulled. Exporting pull times to json. Elapsed time: {elapsed_time}")

            self.export_start_times(from_time, "spec")

            elapsed_time = time.time() - start_time
            print(f"json exported. Pulling next 5000. Elapsed time: {elapsed_time}")
            total_pulls += 1
            if self.check_max_vs_total_pulls(max_pulls,total_pulls,start_time):
                break
        self.crud.db.close()

    def advance_timestamp(self, ts):
        """This function exists to increment timestamps by a single microsecond.
        This is to avoid duplicates in our pull requests."""
        dt = datetime.fromisoformat(ts[:-1]) + timedelta(microseconds=1)
        return dt.isoformat(timespec='microseconds') + "Z"

    def get_last_bme_and_ds(self,records):
        last_bme = None
        last_ds = None

        for r in records:
            if "BME280" in r["reading"]:
                last_bme = r
            else:
                last_ds = r

        return last_bme, last_ds

    def check_max_vs_total_pulls(self,max_pulls,total_pulls,start_time):
        if max_pulls is not None and total_pulls >= max_pulls:
            elapsed_time = time.time() - start_time
            print(f"Reached maximum number of pulls. Aborting ETL. Elapsed time: {elapsed_time}")
            return True
        return False

    def remove_rows_by_id(self, data, row_ids):
        """This function is specifically set up to remove multiple rows from the data from the Specialisterne Api."""
        return [row for row in data if row['id'] not in row_ids]

    def get_start_times(self):
        """This function grabs the start time from the file etl_times.json if they exist.
        Otherwise, it outputs a dictionary with default start time.
        The default time is the start date of this project.
        The keys of the dict
        """
        try:
            with open("etl_times.json", "r", encoding="utf-8") as f:
                times_dict = json.load(f)
        except FileNotFoundError:
            times_dict = {
                "DMI": {
                    "temp_dry": "2026-03-09T00:00:00Z",
                    "humidity": "2026-03-09T00:00:00Z",
                    "pressure": "2026-03-09T00:00:00Z"},
                "spec": "2026-03-09T00:00:00Z"
            }

        return times_dict

    def export_start_times(self, from_time, api: str, parameter_id=None):
        """This function handles exporting the time metadata to a JSON file.
        The times exported are used as starting points for any new pull requests sent to the two APIs we are working with.
        from_time is the time label
        api is the api used. the function expects 'DMI' or 'spec'. """

        times_dict = self.get_start_times()

        if api == "DMI":
            if parameter_id not in ["temp_dry", "humidity", "pressure"]:
                raise ValueError("""No or incorrect parameterId provided. 
            You must provide a parameter id from the list 'temp_dry, humidity, pressure' when the API is DMI.""")
            times_dict["DMI"][parameter_id] = from_time
        elif api == "spec":
            times_dict["spec"] = from_time
        else:
            raise ValueError("Incorrect API name given. The API must be 'DMI' or 'spec'.")

        with open("etl_times.json", "w", encoding="utf-8") as f:
            json.dump(times_dict, f, indent=4)
            print("etl_times_json exported")


    def user_controlled_update(self):
        print("""Initializing ETL process. You have two options:
         1. The program pulls data once
         2. The program pulls data and then auto-updates at a user-defined interval
         """)
        print("Which option do you want?")
        x = input().strip()
        while x not in {"1","2"}:
            print("You must input 1 or 2")
            x = input().strip()
        if x == "1":
            self.update_database()
        if x == "2":
            default_interval = 10
            try:
                y = int(input("Define interval (1-60), default 10: ").strip())
                y = y if 1 <= y <= 60 else default_interval
            except ValueError:
                y = default_interval
            self.start_etl_background(interval_minutes=y)
            # Keep the main thread alive
            print("ETL running in background. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("Stopping ETL process...")

    def run_etl_periodically(self,interval_seconds: int = 600):
        while True:
            try:
                print("Starting ETL run.")
                self.update_database()
            except Exception as e:
                print("ETL error:", e)
            time.sleep(interval_seconds)

    def start_etl_background(self,interval_minutes: int = 10):
        interval_seconds = interval_minutes * 60
        thread = threading.Thread(target=self.run_etl_periodically, args=(interval_seconds,), daemon=True)
        thread.start()
        print(f"ETL daemon started, running every {interval_minutes} minutes.")


    def new_spec_etl(self, from_time: str = "2026-03-17T00:00:00Z", max_pulls: int = None, limit: int = 5000):
        api = NewSpecAPI()
        start_time = time.time()
        total_pulls = 0
        avoid_ids = set()
        transformer = SpecDataTransformer()

        print("Pulling data from the New Specialisterne API")
        while True:
            pull_time, records = api.pull_from(limit=limit, from_time=from_time)

            if avoid_ids:
                records = self.remove_rows_by_id(records, avoid_ids)
            if not records:
                print("No more new records.")
                break
            # We create a new timestamp and will pull the next entries from there.
            # The timestamps of the readers do not fully line up.
            # So we take the min of these and increment by a millisecond.
            # This means we will get a duplicate row (one entry per reader) when making the next pull.

            last_readings = self.get_last_readings(records)
            from_time = self.advance_timestamp(min([last_readings[key]["timestamp"] for key in last_readings]))

            # We explicitly make sure to remove those duplicates.
            # This is not necessary for the database, as the create_mult_rows method skips duplicates.
            # HOWEVER, it is necessary to ensure the process stops.
            # Indeed, if we don't remove them, there records will always have at least one record per reader, and the process will hang.
            avoid_ids = [last_readings[key]["id"] for key in last_readings]

            db_dict = transformer.new_spec_data_to_db_dict(pull_time, records)
            for table_name in db_dict:
                self.crud.create_mult_rows(table_name, db_dict[table_name], commit=True, close=False)

            elapsed_time = time.time() - start_time
            print(f"5000 records pulled. Exporting pull times to json. Elapsed time: {elapsed_time}")

            self.export_start_times(from_time, "spec")

            elapsed_time = time.time() - start_time
            print(f"json exported. Pulling next 5000. Elapsed time: {elapsed_time}")
            total_pulls += 1
            if self.check_max_vs_total_pulls(max_pulls,total_pulls,start_time):
                break
        self.crud.db.close()

    def get_last_readings(self, records):
        last_readings = {}

        for r in records:
            if "BME280" in r["reading"] and r['location']['value'] == '00000000adae116e-percepter-ballerup-in':
                last_readings["last_bme_in"] = r
            elif "BME280" in r["reading"] and r['location']['value'] == '000000005b900eb3-percepter-ballerup-out':
                last_readings["last_bme_out"] = r
            elif "DS18B20" in r['reading'] and r['location']['value'] == '00000000adae116e-percepter-ballerup-in':
                last_readings["last_ds_in"] = r
            elif "DS18B20" in r['reading'] and r['location']['value'] == '000000005b900eb3-percepter-ballerup-out':
                last_readings["last_ds_out"] = r
            else:
                last_readings["last_scd"] = r

        return last_readings