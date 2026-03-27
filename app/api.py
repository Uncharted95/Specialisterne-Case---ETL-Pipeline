from fastapi import FastAPI, Query
from app.load.db.connection import Connector
from app.config import docker_database_schema
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

connector = Connector(**docker_database_schema)
connector.connect()

@app.get("/stations")
def get_stations():
    rows = connector.query("""
        SELECT DISTINCT station_id 
        FROM "DMI"
        ORDER BY station_id
    """)
    print("Stations rows:", rows)
    return [{"station_id": row[0]} for row in rows]


@app.get("/stations/{station_id}/measurements")
def get_measurements(
    station_id: int,
    parameter_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    query = """SELECT parameter_id, value, observed_at, station_id 
               FROM "DMI" WHERE station_id = %s"""
    params = [station_id]

    if parameter_id:
        query += " AND parameter_id = %s"
        params.append(parameter_id)
    if start_date:
        query += " AND observed_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND observed_at <= %s"
        params.append(end_date)

    query += " ORDER BY observed_at DESC"

    rows = connector.query(query, params)
    return [
        {"parameter_id": r[0], "value": r[1], "observed_at": r[2], "station_id": r[3]}
        for r in rows
    ]


@app.get("/stations/latest")
def get_latest():
    rows = connector.query("""
        SELECT DISTINCT ON (station_id, parameter_id)
            station_id, parameter_id, value, observed_at
        FROM "DMI"
        ORDER BY station_id, parameter_id, observed_at DESC
    """)
    return [
        {"station_id": r[0], "parameter_id": r[1], "value": r[2], "observed_at": r[3]}
        for r in rows
    ]


@app.get("/compare")
def compare_stations(
    parameter_id: str = Query(..., description="e.g. temp_dry, humidity, pressure"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    query = """SELECT station_id, value, observed_at 
               FROM "DMI" WHERE parameter_id = %s"""
    params = [parameter_id]

    if start_date:
        query += " AND observed_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND observed_at <= %s"
        params.append(end_date)

    query += " ORDER BY observed_at DESC"

    rows = connector.query(query, params)
    return [
        {"station_id": r[0], "value": r[1], "observed_at": r[2]}
        for r in rows
    ]