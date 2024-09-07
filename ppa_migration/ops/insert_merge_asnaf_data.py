import os
from datetime import datetime
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
    
    context.log.info("Inserting or merging data into SQL Server.")
    
    error_log = []
    
    for row in transformed_data:
        try:
            cursor_sql.execute(merge_data_query, row)
            
            # Retrieve metadata after executing the query
            field_names = [desc[0] for desc in cursor_sql.description] if cursor_sql.description else ["Unknown"]
            
        except Exception as e:
            # Ensure we handle cases where field names are unavailable or lengths do not match
            if not cursor_sql.description:
                field_names = ["Unknown"]
            else:
                field_names = [desc[0] for desc in cursor_sql.description]
            
            context.log.info(f"Length of field_names: {len(field_names)}, Length of row: {len(row)}")

            # Adjust error handling to avoid IndexError
            error_details = {field_names[i] if i < len(field_names) else f"Field_{i}": row[i] for i in range(len(row))}
            error_message = f"Error inserting or merging data for row: {error_details}, Error: {e}"
            context.log.error(error_message)
            error_log.append(error_message)
            continue  # Continue with the next row
    
    # Commit only if there were no errors
    if not error_log:
        sqlserver_conn.commit()
        context.log.info("Data transfer to SQL Server completed successfully.")
    else:
        context.log.warning(f"Data transfer completed with errors. Check the log for details.")

    cursor_sql.close()
    sqlserver_conn.close()

    if error_log:
        # Generate the error log file name with the current datetime
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_file_name = f"error_insert_migrate_data_{current_datetime}.txt"
        error_log_path = os.path.join(base_dir, '../logs', error_log_file_name)
        
        # Ensure the logs directory exists
        os.makedirs(os.path.dirname(error_log_path), exist_ok=True)

        # Write error log to the file
        with open(error_log_path, 'a') as error_file:
            error_file.write("\n".join(error_log) + "\n")
        context.log.info(f"Error log written to {error_log_path}.")
