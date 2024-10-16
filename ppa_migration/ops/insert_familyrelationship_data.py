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
    """Fetch prefix, year, and running_no from RunningNo table where idno is 3."""
    cursor_sql.execute("SELECT prefix, year, running_no FROM RunningNo WHERE idno = 3")
    result = cursor_sql.fetchone()
    if result:
        context.log.info(f"Fetched running_no: {result[2]}, prefix: {result[0]}, year: {result[1]}")
        return result  # prefix, year, running_no
    else:
        raise Exception("Unable to fetch the running_no from RunningNo table.")

def update_running_no(cursor_sql, new_running_no, context):
    """Update the running_no in the RunningNo table where idno is 3."""
    cursor_sql.execute("UPDATE RunningNo SET running_no = ? WHERE idno = 3", (new_running_no,))
    context.log.info(f"Updated running_no to {new_running_no} in RunningNo table.")

@op(required_resource_keys={"sqlserver_db"}, ins={"transformed_data": In(list)})
def insert_familyrelationship_data(context, transformed_data):
    """Insert or merge Familyrelationship data into SQL Server."""
    if not transformed_data:
        context.log.warning("No data to insert or merge.")
        return

    base_dir = os.path.dirname(os.path.realpath(__file__))
    merge_data_query_path = os.path.join(base_dir, '../sql/insert_familyrelationship_data.sql')

    # Load SQL query from file
    merge_data_query = read_sql_file(merge_data_query_path)

    sqlserver_conn = context.resources.sqlserver_db
    context.log.info("Starting Familyrelationship data insertion/merge into SQL Server.")
    
    error_log = []
    batch_size = 5000  # Process data in smaller batches
    param_chunk_size = 2000  # Limit parameter chunk size to avoid SQL Server errors
    total_inserted = 0  # Track total rows inserted
    successful_inserts = False

    try:
        # Begin transaction
        sqlserver_conn.autocommit = False

        with sqlserver_conn.cursor() as cursor_sql:
            # Fetch prefix, year, and running_no for updating row[2]
            prefix, year, running_no = fetch_running_no(cursor_sql, context)

            # Fetch SnapshotIDs in batches
            context.log.info("Fetching SnapshotID for all ParticularAsnafID.")
            all_asnaf_ids = [row[7] for row in transformed_data if row[7]]  # Assuming row[7] contains ParticularAsnafID
            asnaf_snapshot_map = {}

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

            # Fetch HouseholdIDs in batches
            context.log.info("Fetching HouseholdID for all HeadofFamilyId.")
            all_headof_family_ids = [row[6] for row in transformed_data if row[6]]  # Assuming row[6] contains HeadofFamilyId
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

            # Process data in batches
            for data_batch in chunked_list(transformed_data, batch_size):
                if not data_batch:
                    continue  # Skip empty batches

                new_data_batch = []
                try:
                    for row in data_batch:
                        particular_asnaf_id = str(row[7]).upper()  # Assuming row[7] is ParticularAsnafID
                        headof_family_id = str(row[6]).upper()  # Assuming row[6] is HeadofFamilyId

                        # Retrieve SnapshotID from the map
                        asnaf_snapshot_id = asnaf_snapshot_map.get(particular_asnaf_id, None)

                        # Skip row if SnapshotID is missing
                        if not asnaf_snapshot_id:
                            error_log.append(f"Missing SnapshotID for ParticularAsnafID: {particular_asnaf_id}. Skipping row.")
                            continue

                        # Validate HouseholdID (row[8]) and assign it if missing or empty
                        household_id = str(row[8]).upper()  # Assuming row[8] is HouseholdID
                        if household_id:
                            # Validate the household_id from the existing household_map
                            if household_id not in household_map.values():
                                error_log.append(f"Invalid HouseholdID {household_id} for ParticularAsnafID: {particular_asnaf_id}. Skipping row.")
                        else:
                            # If household_id is empty, try to assign based on HeadofFamilyId
                            household_id = household_map.get(headof_family_id, None)
                            if not household_id:
                                error_log.append(f"Missing HouseholdID for HeadofFamilyId: {headof_family_id}. Skipping row.")
                                continue

                        # Update row[2] with prefix-year-running_no format
                        new_running_no_value = f"{prefix}-{year}-{str(running_no).zfill(8)}"
                        row = row[:2] + (new_running_no_value,) + row[3:]

                        # Increment the running number
                        running_no += 1

                        # Create new row with updated SnapshotID and HouseholdID
                        new_row = row[:1] + (asnaf_snapshot_id,) + row[2:8] + (household_id,) + row[9:]

                        new_data_batch.append(tuple(new_row))

                    # Insert or merge the batch of data into Familyrelationship table
                    if new_data_batch:
                        context.log.info(f"Inserting batch of {len(new_data_batch)} rows.")
                        cursor_sql.executemany(merge_data_query, new_data_batch)
                        total_inserted += len(new_data_batch)

                except Exception as e:
                    context.log.error(f"Error executing batch insert. Error: {e}")
                    error_log.extend([f"Error inserting row: {row}. Error: {e}" for row in data_batch])

        # Commit all changes after processing batches
        sqlserver_conn.commit()
        successful_inserts = total_inserted > 0

        # Update the latest running_no in the RunningNo table
        update_running_no(cursor_sql, running_no, context)

    except Exception as e:
        context.log.error(f"Error during data insertion: {e}")
        sqlserver_conn.rollback()
        error_log.append(f"Transaction rolled back due to error: {e}")

    finally:
        # Enable auto-commit back after transaction
        sqlserver_conn.autocommit = True

    if successful_inserts:
        context.log.info(f"Data transfer to SQL Server completed successfully. Total rows inserted: {total_inserted}")
    else:
        context.log.warning("Data transfer completed with errors. Check the error log for details.")

    # Write logs if there were any errors
    write_log_file(context, error_log, 'error_insert_familyrelationship_data', base_dir)

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
