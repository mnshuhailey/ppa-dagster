from dagster import resource
import pyodbc
import psycopg2

# Airbyte resource configuration
@resource(config_schema={"host": str, "port": str, "username": str, "password": str})
def airbyte_resource(context):
    config = context.resource_config
    return {
        "host": config["host"],
        "port": config["port"],
        "username": config["username"],
        "password": config["password"],
    }

# PostgreSQL resource configuration
@resource
def postgres_db_resource(context):
    return psycopg2.connect(
        host="192.168.10.177",
        port=5432,
        user="postgres",
        password="secret123",
        database="postgres_db",
    )

# SQL Server resource configuration
@resource
def sqlserver_db_resource(context):
    # conn_str = (
    #     "DRIVER={ODBC Driver 17 for SQL Server};"
    #     "SERVER=10.10.1.199;"
    #     "DATABASE=PPA;"
    #     "UID=noor.shuhailey;"
    #     "PWD=Lzs.user831;"
    #     "Trust_Connection=yes;"
    # )
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=192.168.0.14;"
        "DATABASE=Test;"
        "UID=sa;"
        "PWD=123qwe;"
        "Trust_Connection=yes;"
    )
    try:
        connection = pyodbc.connect(conn_str)
        return connection
    except pyodbc.Error as ex:
        context.log.error(f"SQL Server connection failed: {ex}")
        raise
