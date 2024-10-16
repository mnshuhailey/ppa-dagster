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

# Function to fetch the current prefix, year, and running_no from RunningNo table
def get_running_no(cursor_sql, context):
    cursor_sql.execute("SELECT prefix, year, running_no FROM RunningNo WHERE idno = 8")
    result = cursor_sql.fetchone()
    if result:
        prefix, year, running_no = result
        context.log.info(f"Fetched running_no: {running_no}, prefix: {prefix}, year: {year}")
        return prefix, year, running_no
    else:
        raise Exception("Unable to fetch the running_no from RunningNo table.")

# Function to update the running_no in the RunningNo table
def update_running_no(cursor_sql, new_running_no, context):
    cursor_sql.execute("UPDATE RunningNo SET running_no = ? WHERE idno = 8", (new_running_no,))
    context.log.info(f"Updated running_no to {new_running_no} in RunningNo table.")

@op(required_resource_keys={"sqlserver_db"}, ins={"transformed_data": In(list)})
def insert_school_data(context, transformed_data):
    if not transformed_data:
        context.log.warning("No data to insert or merge.")
        return

    base_dir = os.path.dirname(os.path.realpath(__file__))
    merge_data_query_path = os.path.join(base_dir, '../sql/insert_school_data.sql')
    
    # Load SQL query from file
    merge_data_query = read_sql_file(merge_data_query_path)

    sqlserver_conn = context.resources.sqlserver_db
    context.log.info("Starting data insertion/merging into SQL Server.")
    
    error_log = []
    batch_size = 1000  # Define batch size for processing data in chunks
    successful_inserts = False
    total_inserted = 0

    with sqlserver_conn.cursor() as cursor_sql:
        # Fetch current prefix, year, and running_no from RunningNo table
        prefix, year, running_no = get_running_no(cursor_sql, context)

        for data_batch in chunked_list(transformed_data, batch_size):
            updated_data_batch = []
            try:
                for row in data_batch:
                    # Generate new value for row[1] in the format prefix-year-latest_running_no
                    new_value = f"{prefix}-{year}-{str(running_no).zfill(8)}"
                    updated_row = (row[0], new_value) + row[2:]  # Update row[1] with new value
                    updated_data_batch.append(updated_row)
                    
                    # Increment running_no for each row
                    running_no += 1

                # Insert the batch of data into the School table
                if updated_data_batch:
                    cursor_sql.executemany(merge_data_query, updated_data_batch)
                    sqlserver_conn.commit()
                    total_inserted += len(updated_data_batch)
                    context.log.debug(f"Inserted {len(updated_data_batch)} rows successfully.")

            except Exception as e:
                context.log.error(f"Error executing batch insert. Error: {e}")
                error_log.extend([f"Error inserting row: {row}. Error: {e}" for row in data_batch])

        # Update the latest running_no in the RunningNo table
        update_running_no(cursor_sql, running_no, context)

    if total_inserted > 0:
        context.log.info(f"Data transfer to SQL Server completed successfully. Total rows inserted: {total_inserted}")
        successful_inserts = True
    else:
        context.log.warning("Data transfer completed with errors. Check the error log for details.")

    # Write logs if there were any errors
    write_log_file(context, error_log, 'error_insert_school_data', base_dir)

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
