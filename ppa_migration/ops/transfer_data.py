from dagster import op
from ppa_migration.resources import postgres_db_resource, sqlserver_db_resource

# Utility function to read SQL file
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

@op(required_resource_keys={"postgres_db", "sqlserver_db"})
def transfer_data_to_sqlserver(context):
    # Load SQL queries from files
    fetch_data_query_path = 'sql/fetch_data.sql'  # Path to your fetch data SQL file
    merge_data_query_path = 'sql/merge_data.sql'  # Path to your merge data SQL file
    fetch_data_query = read_sql_file(fetch_data_query_path)
    merge_data_query = read_sql_file(merge_data_query_path)
    
    postgres_conn = context.resources.postgres_db
    sqlserver_conn = context.resources.sqlserver_db
    cursor_pg = postgres_conn.cursor()
    cursor_sql = sqlserver_conn.cursor()
    
    try:
        context.log.info("Reading data from PostgreSQL.")
        cursor_pg.execute(fetch_data_query)
        rows = cursor_pg.fetchall()
        
        if not rows:
            context.log.warning("No data fetched from PostgreSQL.")
        else:
            context.log.info(f"Fetched {len(rows)} rows from PostgreSQL.")
            context.log.info("Inserting data into SQL Server.")
            for row in rows:
                cursor_sql.execute(merge_data_query, row)
            sqlserver_conn.commit()
            context.log.info("Data transfer to SQL Server completed successfully.")
    except Exception as e:
        context.log.error(f"Error transferring data: {e}")
        raise
    finally:
        cursor_pg.close()
        cursor_sql.close()
