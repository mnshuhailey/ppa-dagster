import os
from datetime import datetime
from dagster import op, In
from ppa_migration.resources import sqlserver_db_resource
import uuid

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
    """Fetch prefix, year, and running_no from RunningNo table where idno is 9."""
    cursor_sql.execute("SELECT prefix, year, running_no FROM RunningNo WHERE idno = 9")
    result = cursor_sql.fetchone()
    if result:
        context.log.info(f"Fetched running_no: {result[2]}, prefix: {result[0]}, year: {result[1]}")
        return result  # prefix, year, running_no
    else:
        raise Exception("Unable to fetch the running_no from RunningNo table.")

def update_running_no(cursor_sql, new_running_no, context):
    """Update the running_no in the RunningNo table where idno is 9."""
    cursor_sql.execute("UPDATE RunningNo SET running_no = ? WHERE idno = 9", (new_running_no,))
    context.log.info(f"Updated running_no to {new_running_no} in RunningNo table.")

@op(required_resource_keys={"sqlserver_db"}, ins={"transformed_data": In(list)})
def insert_hadkifayah_data(context, transformed_data):
    """Insert Hadkifayah data into SQL Server."""
    if not transformed_data:
        context.log.warning("No data to insert or merge.")
        return

    base_dir = os.path.dirname(os.path.realpath(__file__))
    merge_data_query_path = os.path.join(base_dir, '../sql/insert_hadkifayah_data.sql')

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
        all_asnaf_ids = [row[42] for row in transformed_data if row[42]]  # Assuming row[42] contains AsnafID
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

        # Collect all HeadofFamilyIds and fetch HouseholdIDs in batches
        all_headof_family_ids = [row[42] for row in transformed_data if row[42]]  # Assuming row[42] contains HeadofFamilyId
        household_map = {}

        if all_headof_family_ids:
            try:
                for headof_family_id_chunk in chunked_list(all_headof_family_ids, param_chunk_size):
                    cursor_sql.execute(
                        f"""
                        SELECT UPPER(HeadofFamilyId), UPPER(HouseholdID) 
                        FROM Household 
                        WHERE HeadofFamilyId IN ({",".join(["?"] * len(headof_family_id_chunk))})
                        """,
                        headof_family_id_chunk
                    )
                    household_results = cursor_sql.fetchall()
                    household_map.update({row[0]: row[1] for row in household_results})

            except Exception as e:
                context.log.error(f"Error fetching HouseholdIDs: {e}")
                return

        # Process and insert data in batches
        for data_batch in chunked_list(transformed_data, batch_size):
            try:
                updated_data_batch = []
                for row in data_batch:
                    # Convert tuple to list for mutable operations
                    row = list(row)

                    # Ensure hadkifayahID is not null (assign a new one if missing)
                    if not row[0]:  # Assuming row[0] is hadkifayahID
                        row[0] = str(uuid.uuid4())  # Assign a new UUID if hadkifayahID is missing

                    asnaf_id = str(row[42]).upper()  # Assuming row[42] contains AsnafID

                    # Get SnapshotID for the corresponding AsnafID or skip if null/empty
                    snapshot_id = asnaf_snapshot_map.get(asnaf_id, None)
                    if not snapshot_id:
                        error_log.append(f"Missing SnapshotID for AsnafID: {asnaf_id}. Skipping row.")
                        continue  # Skip row if snapshot_id is null/empty

                    # Check if row[39] (HouseholdID) is empty and assign it
                    if not row[39]:  # Assuming row[39] is HouseholdID
                        headof_family_id = str(row[42]).upper()  # Assuming row[42] contains HeadofFamilyId
                        household_id = household_map.get(headof_family_id, None)
                        if household_id:
                            row[39] = household_id  # Assign fetched household_id to row[39]
                        else:
                            error_log.append(f"Missing HouseholdID for HeadofFamilyId: {headof_family_id}. Skipping row.")
                            continue  # Skip row if HouseholdID not found

                    # Replace row[1] with SnapshotID
                    row[1] = snapshot_id

                    # Generate new value for row[2]
                    new_value = f"{prefix}-{year}-{str(running_no).zfill(8)}"
                    row[2] = new_value

                    # Increment the running number
                    running_no += 1

                    # Convert the row back to a tuple and add it to the batch
                    updated_data_batch.append(tuple(row))

                # Insert the batch of data into SQL Server
                cursor_sql.executemany(merge_data_query, updated_data_batch)
                sqlserver_conn.commit()
                total_inserted += len(updated_data_batch)
                context.log.info(f"Inserted {len(updated_data_batch)} rows successfully.")

                # Update the running number after each batch
                update_running_no(cursor_sql, running_no, context)

            except Exception as e:
                context.log.error(f"Error executing batch insert: {e}")
                for row in data_batch:
                    error_log.append(f"Error inserting row: {row}. Error: {e}")

    # Log total inserts and errors if any
    if total_inserted > 0:
        context.log.info(f"Data transfer to SQL Server completed successfully. Total rows inserted: {total_inserted}")
    else:
        context.log.warning("Data transfer completed with errors. Check the error log for details.")

    # Write error logs if necessary
    write_log_file(context, error_log, 'error_insert_hadkifayah_data', base_dir)

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
