from fastapi import FastAPI
from app.load.db.connection import Connector
from app.config import docker_database_schema

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello from FastAPI!"}