# This script is for setting up the variables used to connect to the database.
# They are used by the script initialize.py
#There are two schemas. One is for working with a local database in postgres.
#The other is for running in docker.
#local database
database_schema = {"database": "weather_db",
                   "user":"postgres",
                   "password":"Hestehop11235!",
                   "host": "localhost"}




#Docker
#database_schema = {"database": "weather_db", "user":"weather_app","password":"Hestehop11235!", "host": "db"}
