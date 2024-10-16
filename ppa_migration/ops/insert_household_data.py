import os
from datetime import datetime
from dagster import op, In
from ppa_migration.resources import sqlserver_db_resource

def read_sql_file(file_path):
    """Reads a SQL file from the given path."""
    with open(file_path, 'r') as file:
        return file.read()

# Helper function to split lists into batches
def chunked_list(input_list, chunk_size):
    """Splits a list into smaller chunks."""
    for i in range(0, len(input_list), chunk_size):
        yield input_list[i:i + chunk_size]

def fetch_running_no(cursor_sql, context):
    """Fetch prefix, year, and running_no from RunningNo table where idno is 2."""
    cursor_sql.execute("SELECT prefix, year, running_no FROM RunningNo WHERE idno = 2")
    result = cursor_sql.fetchone()
    if result:
        return result  # prefix, year, running_no
    else:
        raise Exception("Unable to fetch the running_no from RunningNo table.")

def update_running_no(cursor_sql, new_running_no, context):
    """Update the running_no in the RunningNo table where idno is 2."""
    cursor_sql.execute("UPDATE RunningNo SET running_no = ? WHERE idno = 2", (new_running_no,))
    context.log.info(f"Updated running_no to {new_running_no} in RunningNo table.")

@op(required_resource_keys={"sqlserver_db"}, ins={"transformed_data": In(list)})
def insert_household_data(context, transformed_data):
    """Insert household data into SQL Server."""
    if not transformed_data:
        context.log.warning("No data to insert or merge.")
        return

    base_dir = os.path.dirname(os.path.realpath(__file__))
    merge_data_query_path = os.path.join(base_dir, '../sql/insert_household_data.sql')
    
    # Load SQL query from file
    merge_data_query = read_sql_file(merge_data_query_path)

    sqlserver_conn = context.resources.sqlserver_db
    context.log.info("Starting data insertion into SQL Server.")
    
    error_log = []
    batch_size = 5000  # Process data in smaller batches
    param_chunk_size = 2000  # Limit parameter chunk size to avoid SQL Server errors
    total_inserted = 0  # Track total rows inserted

    with sqlserver_conn.cursor() as cursor_sql:
        prefix, year, running_no = fetch_running_no(cursor_sql, context)

        # Collect all AsnafIDs
        all_asnaf_ids = [row[5] for row in transformed_data if row[5]]
        asnaf_snapshot_map = {}

        # Fetch SnapshotIDs in batches
        if all_asnaf_ids:
            try:
                for asnaf_id_chunk in chunked_list(all_asnaf_ids, param_chunk_size):
                    cursor_sql.execute(
                        f"""
                        SELECT UPPER(AsnafID), CONVERT(UNIQUEIDENTIFIER, SnapshotID) 
                        FROM asnaf 
                        WHERE AsnafID IN ({",".join(["?"] * len(asnaf_id_chunk))})
                        """,
                        asnaf_id_chunk
                    )
                    snapshot_results = cursor_sql.fetchall()
                    asnaf_snapshot_map.update({row[0]: row[1] for row in snapshot_results})

            except Exception as e:
                context.log.error(f"Error fetching SnapshotIDs: {e}")
                return

        # Process and insert data in batches
        for data_batch in chunked_list(transformed_data, batch_size):
            try:
                updated_data_batch = []
                for row in data_batch:
                    asnaf_id = str(row[5]).upper()  # Assuming row[5] contains AsnafID

                    # Get SnapshotID for the corresponding AsnafID or NULL if not found
                    snapshot_id = asnaf_snapshot_map.get(asnaf_id, None)

                    # Replace row[1] with SnapshotID or NULL if not found
                    updated_row = row[:1] + (snapshot_id,) + row[2:]

                    # Generate new value for row[2]
                    new_value = f"{prefix}-{year}-{str(running_no).zfill(8)}"
                    updated_row = updated_row[:2] + (new_value,) + updated_row[3:]

                    # Increment the running number
                    running_no += 1

                    # Add the updated row to the batch
                    updated_data_batch.append(updated_row)

                # Insert the batch of data into SQL Server
                cursor_sql.executemany(merge_data_query, updated_data_batch)
                sqlserver_conn.commit()
                total_inserted += len(updated_data_batch)
                context.log.info(f"Inserted {len(updated_data_batch)} rows successfully.")

            except Exception as e:
                context.log.error(f"Error executing batch insert: {e}")
                for row in data_batch:
                    error_log.append(f"Error inserting row: {row}. Error: {e}")

    # After processing all rows, update the running_no in RunningNo table
    with sqlserver_conn.cursor() as cursor_sql:
        update_running_no(cursor_sql, running_no, context)

    # Log total inserts and errors if any
    if total_inserted > 0:
        context.log.info(f"Data transfer to SQL Server completed successfully. Total rows inserted: {total_inserted}")
    else:
        context.log.warning("Data transfer completed with errors. Check the error log for details.")

    # Write error logs if necessary
    write_log_file(context, error_log, 'error_insert_household_data', base_dir)

def write_log_file(context, log_entries, log_name, base_dir):
    """Write error logs to a file."""
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
