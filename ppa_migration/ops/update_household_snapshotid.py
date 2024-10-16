import os
from dagster import op, In, Out, Nothing

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

@op(ins={"after_insert_familyrelationship_data": In(Nothing)}, required_resource_keys={"sqlserver_db"}, out=Out(Nothing))
def update_household_snapshotid(context):
    # Base directory to read SQL file
    base_dir = os.path.dirname(os.path.realpath(__file__))
    update_query_path = os.path.join(base_dir, '../sql/update_household_snapshotid.sql')

    # Load SQL query from file (this should be your update query)
    update_query = read_sql_file(update_query_path)

    sqlserver_conn = context.resources.sqlserver_db
    context.log.info("Updating Household SnapshotID based on Familyrelationship SnapshotID.")

    error_log = []
    successful_updates = False
    total_updated = 0  # To track the total number of updated rows

    with sqlserver_conn.cursor() as cursor_sql:
        try:
            # Perform the update using the SQL update query
            context.log.info("Executing the update query.")
            cursor_sql.execute(update_query)
            sqlserver_conn.commit()
            total_updated = cursor_sql.rowcount
            successful_updates = True
            context.log.info(f"Updated SnapshotID for {total_updated} rows successfully.")
        except Exception as e:
            context.log.error(f"Error executing update. Error: {e}")
            error_log.append(f"Error during update operation: {e}")

    if successful_updates:
        context.log.info(f"Household SnapshotID update completed successfully. Total rows updated: {total_updated}")
    else:
        context.log.warning("Household SnapshotID update completed with errors. Check the error log for details.")

    # Write logs if there were any errors
    write_log_file(context, error_log, 'error_update_household_snapshotid', base_dir)

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
