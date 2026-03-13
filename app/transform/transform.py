
class DMIDataTransformer:
    def __init__(self):
        pass

    def dmi_data_to_db_dict(self, pull_time, data):
        filtered_data = [{
            "dmi_id": feature["id"],
            "parameter_id": feature["properties"]["parameterId"],
            "value": feature["properties"]["value"],
            "observed_at": feature["properties"]["observed"],
            "pulled_at": pull_time,
            "station_id": feature["properties"]["stationId"]
        } for feature in data]
        return filtered_data



class SpecDataTransformer:
    def __init__(self):
        pass

    def bme_record_to_dict(self,record):
        db_dict = {}
        db_dict["reader_id"] = record['id']
        db_dict["location"] = "outside"
        read_dict = record.get("reading").get("BME280")
        for col in ["humidity","pressure","temperature"]:
            db_dict[col] = read_dict[col]
        db_dict["observed_at"] = record["timestamp"]

        return db_dict

    def ds_record_to_dict(self,record):
        db_dict = {}
        db_dict["reader_id"] = record['id']
        if record["reading"]["DS18B20"]["device_name"] == "28-0000003e33d5":
            db_dict["location"] = "outside"
        read_dict = record.get("reading").get("DS18B20")
        db_dict["temperature"] = read_dict["raw_reading"]/1000
        db_dict["observed_at"] = record["timestamp"]

        return db_dict

    def spec_data_to_db_dict(self,pull_time, data):
        """This method takes the recorded data from the specialisterne API, and transforms it to a dict of two lists.
        The keys of the dict are the table names for the lists to be put into"""
        bme_db_list = []
        ds_db_list = []
        db_dict = {"BME280": bme_db_list, "DS18B20": ds_db_list}
        for record in data:
            device = list(record.get("reading").keys())[0]
            if device == "BME280":
                bme_dict = self.bme_record_to_dict(record)
                bme_dict["pulled_at"] = pull_time
                bme_db_list.append(bme_dict)
            if device =="DS18B20":
                ds_dict = self.ds_record_to_dict(record)
                ds_dict["pulled_at"] = pull_time
                ds_db_list.append(ds_dict)
        return db_dict
