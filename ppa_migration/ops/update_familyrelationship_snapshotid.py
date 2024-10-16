import os
from datetime import datetime
from dagster import op, In
from ppa_migration.resources import sqlserver_db_resource

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Helper function to split lists into batches
def chunked_list(input_list, chunk_size):
    for i in range(0, len(input_list), chunk_size):
        yield input_list[i:i + chunk_size]

@op(required_resource_keys={"sqlserver_db"}, ins={"transformed_data": In(list)})
def update_familyrelationship_snapshotid(context, transformed_data):
    if not transformed_data:
        context.log.warning("No data to update SnapshotID.")
        return

    base_dir = os.path.dirname(os.path.realpath(__file__))
    update_query_path = os.path.join(base_dir, '../sql/update_familyrelationship_snapshotid.sql')
    
    # Load SQL query from file
    update_query = read_sql_file(update_query_path)

    sqlserver_conn = context.resources.sqlserver_db
    context.log.info("Updating Familyrelationship SnapshotID based on Household SnapshotID.")

    error_log = []
    batch_size = 1000  # Define batch size for processing data in chunks
    successful_updates = False
    total_updated = 0  # To track the total number of updated rows

    with sqlserver_conn.cursor() as cursor_sql:
        # Process data in batches
        for data_batch in chunked_list(transformed_data, batch_size):
            if not data_batch:
                continue  # Skip empty batches

            try:
                context.log.info(f"Processing batch of {len(data_batch)} rows for SnapshotID update.")
                cursor_sql.executemany(update_query, data_batch)  # Execute batch update
                sqlserver_conn.commit()
                total_updated += len(data_batch)
                successful_updates = True
                context.log.info(f"Updated SnapshotID for {len(data_batch)} rows successfully.")
            except Exception as e:
                context.log.error(f"Error executing batch update. Error: {e}")
                # Log errors for each row in the failed batch
                for row in data_batch:
                    error_log.append(f"Error updating row: {row}. Error: {e}")

    if successful_updates:
        context.log.info(f"SnapshotID update completed successfully. Total rows updated: {total_updated}")
    else:
        context.log.warning("SnapshotID update completed with errors. Check the error log for details.")

    # Write logs if there were any errors
    write_log_file(context, error_log, 'error_update_familyrelationship_snapshotid', base_dir)


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
