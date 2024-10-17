import os
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
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

def get_latest_running_no(cursor_sql, context):
    """Fetch the latest running_no for the snapshot."""
    cursor_sql.execute("SELECT prefix, year, running_no FROM RunningNo WHERE idno = 5")
    result = cursor_sql.fetchone()
    if result:
        prefix, year, running_no = result
        context.log.info(f"Fetched running_no: {running_no}, prefix: {prefix}, year: {year}")
        return prefix, year, running_no
    else:
        raise Exception("Unable to fetch the running_no from RunningNo table.")

def update_running_no(cursor_sql, new_running_no, context):
    """Update the running_no in the RunningNo table where idno = 5."""
    cursor_sql.execute("UPDATE RunningNo SET running_no = ? WHERE idno = 5", (new_running_no,))
    context.log.info(f"Updated running_no to {new_running_no} in RunningNo table.")

def get_latest_running_no_mig(cursor_sql, context):
    """Fetch the latest running_no for the AsnafName migration."""
    cursor_sql.execute("SELECT prefix, year, running_no FROM RunningNo WHERE idno = 10")
    result = cursor_sql.fetchone()
    if result:
        prefix, year, running_no = result
        context.log.info(f"Fetched running_no_mig: {running_no}, prefix: {prefix}, year: {year}")
        return prefix, year, running_no
    else:
        raise Exception("Unable to fetch the running_no_mig from RunningNo table.")

def update_running_no_mig(cursor_sql, new_running_no, context):
    """Update the running_no in the RunningNo table where idno = 10."""
    cursor_sql.execute("UPDATE RunningNo SET running_no = ? WHERE idno = 10", (new_running_no,))
    context.log.info(f"Updated running_no_mig to {new_running_no} in RunningNo table.")

def check_for_duplicates(cursor_sql, ids_chunk, column_name, context):
    """
    Check for duplicates in the SQL table based on provided IDs and column name.
    This function processes chunks of IDs at a time.
    """
    duplicate_dict = {}
    try:
        if ids_chunk:
            query = f"SELECT {column_name} FROM asnaf WHERE {column_name} IN ({','.join(['?'] * len(ids_chunk))})"
            cursor_sql.execute(query, ids_chunk)
            duplicates = cursor_sql.fetchall()
            duplicate_dict.update({d[0]: True for d in duplicates})

    except Exception as e:
        context.log.error(f"Error checking for duplicates: {e}")
    return duplicate_dict

@op(required_resource_keys={"sqlserver_db"}, ins={"transformed_data": In(list)})
def insert_merge_asnaf_data(context, transformed_data):
    """Insert or merge Asnaf data into SQL Server."""
    if not transformed_data:
        context.log.warning("No data to insert or merge.")
        return

    base_dir = os.path.dirname(os.path.realpath(__file__))
    merge_data_query_path = os.path.join(base_dir, '../sql/insert_merge_asnaf_data.sql')
    insert_snapshot_query_path = os.path.join(base_dir, '../sql/insert_snapshot_data.sql')
    
    # Load SQL queries once and reuse them
    merge_data_query = read_sql_file(merge_data_query_path)
    insert_snapshot_query = read_sql_file(insert_snapshot_query_path)

    sqlserver_conn = context.resources.sqlserver_db
    context.log.info("Starting data insertion or merging into SQL Server.")
    
    error_log = []
    duplicate_log = []
    successful_inserts = False
    total_inserted = 0

    batch_size = 5000

    def process_batch(batch_data, merge_data_query, sqlserver_conn):
        """Process each batch of data and commit after each batch."""
        with sqlserver_conn.cursor() as cursor_sql:
            try:
                cursor_sql.executemany(merge_data_query, batch_data)
                sqlserver_conn.commit()  # Commit after processing each batch
                context.log.info(f"Inserted {len(batch_data)} rows successfully.")
                return len(batch_data)
            except Exception as e:
                context.log.error(f"Error inserting batch: {e}")
                error_log.append(f"Error inserting batch: {e}")
                return 0

    with sqlserver_conn.cursor() as cursor_sql:
        asnaf_ids = [row[1] for row in transformed_data]
        identification_num_ics = [row[14] for row in transformed_data]

        # Retrieve latest running numbers
        prefix_snapshot, year_snapshot, running_no_snapshot = get_latest_running_no(cursor_sql, context)
        prefix_mig, year_mig, running_no_mig = get_latest_running_no_mig(cursor_sql, context)

        # Step 1: Check for duplicates for asnaf_ids (DISABLED)
        # duplicate_asnaf_ids = {}
        # for asnaf_id_chunk in chunked_list(asnaf_ids, 2000):
        #     duplicate_asnaf_ids.update(check_for_duplicates(cursor_sql, asnaf_id_chunk, "AsnafID", context))

        # Step 2: Check for duplicates for identification_num_ics (DISABLED)
        # duplicate_identification_ics = {}
        # for id_ic_chunk in chunked_list(identification_num_ics, 2000):
        #     duplicate_identification_ics.update(check_for_duplicates(cursor_sql, id_ic_chunk, "IdentificationNumIC", context))

        # Combine the duplicates from both checks (DISABLED)
        # duplicate_dict = {**duplicate_asnaf_ids, **duplicate_identification_ics}

        # Step 3: Insert or merge non-duplicate data in batches
        batch_data = []

        for row in transformed_data:
            # asnaf_id, identification_num_ic = row[1], row[14]

            # Bypass duplicate checking logic since it's disabled
            # if asnaf_id in duplicate_dict or identification_num_ic in duplicate_dict:
            #     log_entry = f"Duplicate found: {asnaf_id if asnaf_id else identification_num_ic} at {datetime.now()}"
            #     duplicate_log.append(log_entry)
            #     context.log.info(log_entry)
            #     continue

            # Generate SnapshotID and AsnafName
            snapshot_id = str(uuid.uuid4()).upper()
            snapshot_name = f"{prefix_snapshot}-{year_snapshot}-{str(running_no_snapshot).zfill(8)}"
            running_no_snapshot += 1  # Increment Snapshot running number
            
            # Generate new AsnafName using get_latest_running_no_mig and increment running_no_mig
            asnaf_name = f"{prefix_mig}-{year_mig}-{str(running_no_mig).zfill(8)}"
            running_no_mig += 1  # Increment MIG running number

            try:
                # Insert Snapshot data
                with sqlserver_conn.cursor() as snapshot_cursor:
                    snapshot_data = (
                        snapshot_id,
                        snapshot_name,
                        'ASNAFREGISTRATION',
                        datetime.now(),  # CreatedOn
                        '00000000-0000-0000-0000-000000000000',  # CreatedBy
                        'lulus'
                    )
                    snapshot_cursor.execute(insert_snapshot_query, snapshot_data)

                # Update row and prepare for batch insertion
                new_row = (snapshot_id,) + row[1:2] + (asnaf_name,) + row[3:]
                batch_data.append(tuple(new_row))

            except Exception as e:
                context.log.error(f"Error inserting snapshot: {e}")
                error_log.append(f"Error inserting snapshot: {e}")
                continue

            if len(batch_data) >= batch_size:
                total_inserted += process_batch(batch_data, merge_data_query, sqlserver_conn)
                batch_data.clear()

                # Update running numbers after each batch
                update_running_no(cursor_sql, running_no_snapshot, context)
                update_running_no_mig(cursor_sql, running_no_mig, context)

        # Process remaining rows
        if batch_data:
            total_inserted += process_batch(batch_data, merge_data_query, sqlserver_conn)

            # Update running numbers after processing the last batch
            update_running_no(cursor_sql, running_no_snapshot, context)
            update_running_no_mig(cursor_sql, running_no_mig, context)

    context.log.info(f"Data transfer to SQL Server completed successfully. Total rows inserted: {total_inserted}")

    # Step 5: Write logs
    write_log_file(context, error_log, 'error_insert_merge_asnaf_data', base_dir)
    write_log_file(context, duplicate_log, 'duplicate_asnaf_log', base_dir)

def write_log_file(context, log_entries, log_name, base_dir):
    """Write logs to a file."""
    if log_entries:
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_name = f"{log_name}_{current_datetime}.txt"
        log_file_path = os.path.join(base_dir, '../logs', log_file_name)
        
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

        with open(log_file_path, 'a') as log_file:
            log_file.write("\n".join(log_entries) + "\n")
        context.log.info(f"Log written to {log_file_path}.")
