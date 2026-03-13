from load.db.initialize import DatabaseInitializer
from load.db.CRUD import CRUD
from pipeline.etl import spec_etl, dmi_etl



def main():
    print("hej")
    initializer = DatabaseInitializer()
    initializer.create_db()
    initializer.initialize_db()
    crud = CRUD()
    crud.cleanse_db()
    spec_etl()


    #API = SpecAPI()
    #pull_time, records = API.pull_year_month_day(2,"2026","03","10")
    #for record in records:
        #print(record)

    #transformer = SpecDataTransformer()
    #db_dict = transformer.spec_data_to_db_dict(pull_time, records)
    #for table_name in db_dict:
        #crud.create_mult(table_name,db_dict[table_name])
    #spec_etl(max_pulls=2)
    # stations = {
    #     "station_id_ballerup": "06181",
    #     "station_id_odense": "06126",
    #     "station_id_aarhus": "06072"
    # }
    #
    # params = ["temp_dry","humidity","pressure"]
    #
    # for station in stations:
    #     for parameterId in params:
    #         dmi_etl(stations[station],parameterId)



if __name__ == "__main__":
    main()