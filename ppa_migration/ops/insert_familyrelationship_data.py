import os
from datetime import datetime
from dagster import op, In
from ppa_migration.resources import sqlserver_db_resource

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

@op(required_resource_keys={"sqlserver_db"}, ins={"transformed_data": In(list)})
def insert_familyrelationship_data(context, transformed_data):
    if not transformed_data:
        context.log.warning("No data to insert or merge.")
        return

    base_dir = os.path.dirname(os.path.realpath(__file__))
    merge_data_query_path = os.path.join(base_dir, '../sql/insert_familyrelationship_data.sql')
    
    # Load SQL query from file
    merge_data_query = read_sql_file(merge_data_query_path)

    sqlserver_conn = context.resources.sqlserver_db
    context.log.info("Inserting data into SQL Server.")
    
    error_log = []
    successful_inserts = False  # Flag to track if any rows were inserted

    with sqlserver_conn.cursor() as cursor_sql:
        # Step 1: Retrieve field names before executing merge operations
        try:
            cursor_sql.execute("SELECT TOP 1 * FROM familyrelationship_transformed_v7")
            field_names = [desc[0] for desc in cursor_sql.description if desc[0].lower() != 'idno'] if cursor_sql.description else ["Unknown"]
            context.log.info(f"Retrieved field names: {field_names}")
        except Exception as e:
            context.log.error(f"Error retrieving field names: {e}")
            field_names = ["Unknown"]

        # Step 2: Insert data row by row
        for index, row in enumerate(transformed_data):
            context.log.info(f"Processing row {index+1}: {row}")

            try:
                # Insert the entire row into the SQL query without excluding any column
                cursor_sql.execute(merge_data_query, row)
                successful_inserts = True  # Mark that at least one row was inserted successfully
            except Exception as e:
                context.log.error(f"Error executing query for row {index+1}: {row}. Error: {e}")
                error_log.append(f"Error executing query for row {index+1}: {row}. Error: {e}")
                continue  # Continue to the next row

        # Commit the data insertions if no errors occurred
        if not error_log and successful_inserts:
            sqlserver_conn.commit()
            context.log.info("Data transfer to SQL Server completed successfully.")
        else:
            context.log.warning("Data transfer completed with errors or no successful inserts. Check the log for details.")

    # Write logs if there were any errors
    write_log_file(context, error_log, 'error_insert_familyrelationship_data', base_dir)


def write_log_file(context, log_entries, log_name, base_dir):
    if log_entries:
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_name = f"{log_name}_{current_datetime}.txt"
        log_file_path = os.path.join(base_dir, '../logs', log_file_name)
        
        # Ensure the logs directory exists
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        # Write log to the file
        with open(log_file_path, 'a') as log_file:
            log_file.write("\n".join(log_entries) + "\n")
        context.log.info(f"Log written to {log_file_path}.")
