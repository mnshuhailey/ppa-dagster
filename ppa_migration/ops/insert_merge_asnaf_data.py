import os
from dagster import op, In
from ppa_migration.resources import sqlserver_db_resource

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

@op(required_resource_keys={"sqlserver_db"}, ins={"transformed_data": In(list)})
def insert_merge_asnaf_data(context, transformed_data):
    if not transformed_data:
        context.log.warning("No data to insert or merge.")
        return

    base_dir = os.path.dirname(os.path.realpath(__file__))
    merge_data_query_path = os.path.join(base_dir, '../sql/insert_merge_asnaf_data.sql')
    
    # Load SQL query from file
    merge_data_query = read_sql_file(merge_data_query_path)

    sqlserver_conn = context.resources.sqlserver_db
    cursor_sql = sqlserver_conn.cursor()
    
    try:
        context.log.info("Inserting or merging data into SQL Server.")
        for row in transformed_data:
            cursor_sql.execute(merge_data_query, row)
        sqlserver_conn.commit()
        context.log.info("Data transfer to SQL Server completed successfully.")
    except Exception as e:
        context.log.error(f"Error inserting or merging data: {e}")
        raise
    finally:
        cursor_sql.close()
        sqlserver_conn.close()
