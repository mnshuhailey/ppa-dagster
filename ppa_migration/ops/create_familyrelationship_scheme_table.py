import os
from dagster import op
from ppa_migration.resources import sqlserver_db_resource

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

@op(required_resource_keys={"sqlserver_db"})
def create_familyrelationship_scheme_table(context):
    base_dir = os.path.dirname(os.path.realpath(__file__))
    sql_file_path = os.path.join(base_dir, '../sql/create_familyrelationship_scheme_table.sql')
    context.log.info(f"SQL file path: {sql_file_path}")

    # Check if the file exists before attempting to read it
    if not os.path.exists(sql_file_path):
        context.log.error(f"SQL file not found at {sql_file_path}")
        raise FileNotFoundError(f"SQL file not found at {sql_file_path}")

    # Load SQL query from file
    create_table_query = read_sql_file(sql_file_path)

    sqlserver_conn = context.resources.sqlserver_db
    cursor = sqlserver_conn.cursor()
    
    try:
        context.log.info("Creating table if not exists.")
        cursor.execute(create_table_query)
        sqlserver_conn.commit()
        context.log.info("Table created or already exists.")
    except Exception as e:
        context.log.error(f"Error creating table: {e}")
        raise
    finally:
        cursor.close()
        sqlserver_conn.close()
