import requests
from datetime import datetime, timezone

class SpecAPI:
    def __init__(self):
        self.base_url = "https://climate.spac.dk/api/records"
        self.token = "pZdnVxD2V8kyR2o2EyJLqdNXzfct1vFc-Y7pnrIh6_k"
        self.header = {"Authorization": f"BEARER {self.token}"}

    def pull_year_month_day(self, limit: int = 5000, year: str = "2026", month: str = "03", day: str = "01"):
        parameters = {
            "limit": limit,  # max number of records to fetch
            "from": f"{year}-{month}-{day}T00:00:00Z"  # optional start timestamp
        }
        pull_time = datetime.now(timezone.utc)
        pull_time = pull_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = requests.get(self.base_url, headers=self.header, params=parameters)
        resp.raise_for_status()
        data = resp.json()
        records = data.get("records", [])
        return pull_time, records

    def pull_from(self, limit: int = 5000, from_time: str = "2026-03-09T00:00:00Z"):
        parameters = {
            "limit": limit,  # max number of records to fetch
            "from": from_time  # optional start timestamp
        }
        pull_time = datetime.now(timezone.utc)
        pull_time = pull_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = requests.get(self.base_url, headers=self.header, params=parameters)

        resp.raise_for_status()
        data = resp.json()
        records = data.get("records", [])
        return pull_time, records

    def pull_all(self, limit: int = 5000, year: str = "2026", month: str = "03", day: str = "01" ):
        """This method is designed to pull everything it can from the specialisterne API.
        The output is a list of dicts.
        Each dict has a pull_time and the records pulled at that time"""
        all_records = []
        from_time = f"{year}-{month}-{day}T00:00:00Z"

        while True:
            pull_time, records = self.pull_datetime(limit,from_time)
            if not records:
                break
            all_records.append({
                "pull_time": pull_time,
                "records": records
            })
            from_time = records[-1]["timestamp"]

        return all_records


if __name__ == "__main__":
    base_url = "https://climate.spac.dk/api/records"
    token = "pZdnVxD2V8kyR2o2EyJLqdNXzfct1vFc-Y7pnrIh6_k"
    header = {"Authorization": f"BEARER {token}"}
    params = {
        "limit": 10,  # max number of records to fetch
        "from": f"2026-03-01T00:00:00Z"  # optional start timestamp
    }
    response = requests.get(base_url, headers=header, params=params)
    if response.status_code == 200:
        data = response.json()
        for record in data["records"]:
            print(record)
    else:
        print("Error:", response.status_code, response.text)
    print(data)