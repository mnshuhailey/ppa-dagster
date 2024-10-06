import os
from dagster import op, In, Out, Output, Nothing
from ppa_migration.resources import postgres_db_resource

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

@op(ins={"after_insert_study_data": In(Nothing)}, required_resource_keys={"postgres_db"}, out=Out(list))
def transform_school_data(context):
    base_dir = os.path.dirname(os.path.realpath(__file__))
    fetch_data_query_path = os.path.join(base_dir, '../sql/transform_school_data.sql')
    
    # Load SQL query from file
    fetch_data_query = read_sql_file(fetch_data_query_path)
    
    postgres_conn = context.resources.postgres_db
    with postgres_conn.cursor() as cursor_pg:
        try:
            context.log.info("Reading data from PostgreSQL.")
            cursor_pg.execute(fetch_data_query)
            rows = cursor_pg.fetchall()
            
            if not rows:
                context.log.warning("No data fetched from PostgreSQL.")
                return Output(value=[], metadata={"status": "empty"})
            else:
                context.log.info(f"Fetched {len(rows)} rows from PostgreSQL.")
                return Output(value=rows, metadata={"status": "success"})
        except Exception as e:
            context.log.error(f"Error fetching data: {e}")
            raise
