import requests
from datetime import datetime, timezone

class DMIAPI:
    def __init__(self):
        self.base_url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"

    def pull_datetime(self, station_id, parameter_id, limit: int = 5000, offset: int = 0,  start_time: str = "2026-03-09T00:00:00Z", end_time: str ="2030-01-01T00:00:00Z"):
        """The default end time is set far in the future to artificially create a "no end time" query."""
        parameters = {
            "limit": limit,
            "parameterId": parameter_id,
            "stationId": station_id,
            "datetime": start_time,
            "offset": offset
                      }
        if end_time is not None:
            parameters["datetime"] += f"/{end_time}"

        pull_time = datetime.now(timezone.utc)
        pull_time = pull_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = requests.get(self.base_url, params=parameters)

        resp.raise_for_status()
        records = resp.json()
        return pull_time, records["features"]

#These variables store the station ids for the stations closes to the specialisterne offices
station_id_ballerup = "06181" #jæbersborg station
station_id_odense = "06126" #Årslev station
station_id_aarhus = "06072" #Ødum station
start_time= "2026-03-09T00:00:00Z"
end_time = ".."
parameter_id = "temp_dry"
pull_time = datetime.now(timezone.utc)
pull_time = pull_time.strftime("%Y-%m-%dT%H:%M:%SZ")

if __name__ == "__main__":
    resp = requests.get(f"""https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?parameterId={parameter_id}&limit=3&stationId={station_id_ballerup}&datetime={start_time}/{end_time}&sortorder=observed,DESC""")
    data = resp.json()
    temperature_values = []
    dates = []
    count=0

    filtered_data = [{
        "id": feature["id"],
        "value": feature["properties"]["value"],
        "observed": feature["properties"]["observed"],
        "pulled": pull_time
                      } for feature in data["features"]]

    for observation in filtered_data:
         print(observation)

    #API = DMIAPI()
    #x, data = API.pull_datetime(station_id_ballerup, "temp_dry",limit=1, start_time=start_time)
    #print(data)

    # for feature in data["features"]:
    #     temperature = feature["properties"]["value"]
    #     temperature_values.append(temperature)
    #     date = feature["properties"]["observed"]
    #     dates.append(date)
    #     count+=1

    # plt.figure(figsize=(14, 7))
    # plt.scatter(dates, temperature_values, alpha=0.6)
    # plt.show()
