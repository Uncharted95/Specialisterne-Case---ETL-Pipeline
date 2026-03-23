# ETL Intro

This program's purpose is to extract, transform and load data from external APIs to an internal database.

## Table of Contents
1. [Description](#description)
2. [Database Structure](#database-structure)
3. [Getting Started](#getting-started)
   - [Requirements](#requirements)
   - [Dependencies](#dependencies)
   - [Environment Variables](#environment-variables)
   - [Initial Set-up](#initial-set-up)
   - [Adding Data Sources (DMI)](#adding-data-sources-dmi)
   - [Executing Program](#executing-program)
4. [Help](#help)
    - [Resetting the Database](#resetting-the-database)
5. [Authors](#authors)
6. [Version History](#version-history)

## Description
The program's main functionalities are:
* Extract weather data from the APIs at DMI and Specialisterne ApS.
* Transform it into generally uniform data structures (millicelsius to celsius etc.) 
* Load it into a custom-built SQL database with a user-defined name.

As is, the program pulls data from 3 measuring stations at DMI: Jæbersborg, Ødum and Årslev.
When run, the program pulls data from the APIs in this order:
1. The 'old' Specialisterne API (purely an archive - no new readings are made)
2. DMI
3. The New specialisterne API

The program can run in docker, or it can create and write to a local PostgreSQL database. 
Furthermore, the program has two modes: pull data once or at set time intervals while running. 
By design, the program is OS-agnostic but has only been tested on Windows.

During the extraction process, the program will create/write to a JSON file ("etl_times.json") which holds the latest timestamps of each data source.
The program will then use these as a start point for pulls the next time it runs.

The work here was a 2-week project during my course at Specialisterne ApS. 


### Database structure
After initializing, the database will have 4 ordinary tables "DMI", "BME280", "DS18B20" and "SCD41" which store data from each respective source. Furthermore, 3 view tables will be created containing all the databases' temperature, humidity and pressure data respectively. 
The structure of these tables can be found in SQL files in /app/sql.

The datatypes of the table columns are defined in load/schemas/table_schema.py. Here is an overview of the tables, with columns and datatypes:

**DMI Table:**

| Column       | Datatype                 | content                                                                                                 | 
|--------------|--------------------------|---------------------------------------------------------------------------------------------------------|
| id           | integer, primary key     | the database id                                                                                         |
| dmi_id       | UUID, foreign key        | the uuid of the reading in the dmi database                                                             |
| parameter_id | varchar(50)              | temperature, humidity or pressure                                                                       |
| value        | numeric(20,13)           | degree celsius for temperature, % for humidity and hPa for pressure                                     |
| observed_at  | timestamp with time zone | the date when the data was observed                                                                     |
| pulled_at    | timestamp with time zone | the date at which the data was pulled from the API                                                      |
| station_id   | integer                  | the dmi id for the station where the reading was made. 06181 - Jæbersborg; 06126 - Årslev; 06072 - Ødum |


**BME280 Table:**

| Column      | Datatype                 | content                                                    | 
|-------------|--------------------------|------------------------------------------------------------|
| id          | integer, primary key     | the database id                                            |
| reader_id   | UUID, foreign key        | the uuid of the reading in the dmi database                |
| location    | varchar(7) NOT NULL      | where the sensor is stored (must be 'inside' or 'outside') |
| humidity    | numeric(20,13)           | %                                                          |
| pressure    | numeric(20,13)           | hPa                                                        |
| temperature | numeric(20,13)           | celsius                                                    |
| observed_at | timestamp with time zone | the date when the data was observed                        |
| pulled_at   | timestamp with time zone | the date at which the data was pulled from the API         |

**DS18B20 Table:**

| Column      | Datatype                 | content                                                    | 
|-------------|--------------------------|------------------------------------------------------------|
| id          | integer, primary key     | the database id                                            |
| reader_id   | UUID, foreign key        | the uuid of the reading in the dmi database                |
| location    | varchar(7) NOT NULL      | where the sensor is stored (must be 'inside' or 'outside') |
| temperature | numeric(20,13)           | degree celsius                                             |
| observed_at | timestamp with time zone | the date when the data was observed                        |
| pulled_at   | timestamp with time zone | the date at which the data was pulled from the API         |

**SCD41 Table:**

|  Column     | Datatype                 | content                                            | 
|-------------|--------------------------|----------------------------------------------------|
| id          | integer, primary key     | the database id                                    |
| reader_id   | UUID, foreign key        | the uuid of the reading in the dmi database        |
| co2         | INT                      | ppm                                                |
| humidity    | numeric(20,13)           | %                                                  |
| temperature | numeric(20,13)           | degree celsius                                     |
| observed_at | timestamp with time zone | the date when the data was observed                |
| pulled_at   | timestamp with time zone | the date at which the data was pulled from the API |



## Getting Started

### Requirements
Before installing or running the program, make sure your system meets these prerequisites:

* Python 3.12 or higher
* Docker Desktop (if you want to run the program in Docker)
* PostgreSQL (if running outside Docker with a local database)
* Internet connection to access the external APIs

### Dependencies
If running in Docker, the docker container will install dependencies automatically. 
Otherwise, you will need the modules listed in requirements.txt (common ones include psycopg2 and requests).
You can get these by downloading requirements.txt and running
```
pip install -r requirements.txt
```

### Environment Variables
This is the full list of environment variables used by the program. 
```
SPEC_TOKEN=your_api_token_here        # Token from Specialisterne API
DB_USER=your_docker_db_user           # Docker DB username
DB_PASSWORD=your_docker_db_password   # Docker DB password
DB_NAME=your_docker_db_name           # Docker DB name
ETL_MODE=once                         # Pull mode ('interval' or 'once')
ETL_INTERVAL=10                       # Minutes between intervals
LOCAL_USER=your_local_db_user         # Local PostgreSQL username
LOCAL_PASSWORD=your_local_db_password # Local PostgreSQL password
LOCAL_DB=your_local_db_name           # Local PostgreSQL database name
```

### Initial Set-up
1. Download the app folder and .env.template. Place them in the same project directory.
2. Rename .env.template to .env
3. Get a token for the new Specialisterne API
   * Go to https://herodot.spac.dk/ and register
   * Log in and click on "API Keys" at the top
   * Set a name, like 'auth_token' and press "Generate Key"
   * Copy the token and go to the .env file. Paste the token in place of "your_api_token_here"

4. Get a token for the old Specialisterne API. Alternatively, go to the update_database method in /app/pipeline/etl.py and remove the self.spec_etl call. 
   * Go to https://climate.spac.dk/ and register
   * Log in and click on "API Keys" at the top
   * Set a name, like 'auth_token' and press "Create"
   * Copy the token and go to the .env file. Paste the token in place of "your_api_token_here"

The rest of the setup depends on whether you are running in Docker or with a local database. 

If running in Docker:
1. Download compose.yaml and Dockerfile. Place them next to the app folder and .env.
2. Now go to .env and specify a Docker username, password and database name of your choice. See environment variables above.
3. If you want the program to pull data only once, change ETL_mode to 'once' in .env.

If running outside docker with a local database: 
1. Edit the variable 'docker' in config.py to 'False'. 
2. Go to .env and fill in your PostgreSQL username and password. By default, the user in PostgreSQL is "postgres". You should also specify a database name of your choice. See environment variables above.

### Adding Data Sources (DMI)
By default, the DMI data is pulled from three set DMI stations. You can edit these in /app/load/schemas/station_ids.json.
New stations can be added by going to the DMI webpage, finding their list of stations and grabbing a stationID. 
Then simply add this to the JSON file.

### Executing program

If running in Docker:
1. Open docker desktop
2. Navigate to the folder containing compose.yaml, Dockerfile and the app folder in terminal 
3. On first run, run the following. 
```
docker compose up --build
```
Subsequently, if you need to access the database after closing, you can simply run
```
docker compose up -d
```
4. Now that the database is set up, you connect to the PostgreSQL server by running
```
docker exec -it specialisternecase-etlpipeline-db-1 psql -U your_db_user_here -d your_db_name_here
```
You can then run SQL queries in the command line. Example:
```
SELECT * FROM "DMI";
```
To get a summary of the tables in the database, write
```
\dt
```
5. To exit, first exit PostgreSQL and then docker by writing
```
\q
docker compose down
```

If running outside docker with a local database: 
1. Run main.py 
2. Answer the inputs
3. Use your favorite method to query and view the data in the database (such as pgadmin4 or terminal).


## Help
When running in docker, if there are issues during the build, ensure that in requirements.txt you have 
```
psycopg2-binary
```
with no version rather than something like psycopg2==2.9.11. Alternatively, you can go to "Dockerfile" and change the line
```
FROM python:3.12-slim
```
to something like
```
FROM python:3.12
```
or 3.13. This will extend the build time, as docker now runs a full python distribution.

If you run in to other issues using docker, it might help to reset the database using the instructions above.

### Resetting the database
In all cases, you should delete etl_times.json. This is to reset the starting time of the pull requests made.

If running in docker, you can run the following to reset the entire database.
```
docker compose down -v
```
This will delete all persistent volumes.

The program also includes functionality for nuking tables or the database.
If you wish to reset the database, simply un-comment the following lines in main.py
```
crud = CRUD()
crud.reset_everything()
```
NB: This does not clear the internal ids of the tables. So, for example, if you have 145 rows in the "DMI" table with ids 1-145, the first row you insert will receive the internal id 146. You can clear the ids by feeding reset_everything the optional argument True.

If you wish to delete the rows of a specific table, you can call the delete_all_rows method of the CRUD class. 
This method requires a table name as argument. For example, the following will delete all rows of the "DMI" table:
```
crud = CRUD()
crud.delete_all_rows("DMI")
```
NB: Note that as with the cleanse_db method, this does not clear the internal ids of the tables.


## Authors

@NervousPapaya

## Version History

* 1.0 First fully functioning version with README