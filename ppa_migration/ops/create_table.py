from dagster import op
from ppa_migration.resources import sqlserver_db_resource

# Utility function to read SQL file
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

@op(required_resource_keys={"sqlserver_db"})
def create_table_if_not_exists(context):
    sql_file_path = 'sql/create_table.sql'  # Path to your SQL file
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
