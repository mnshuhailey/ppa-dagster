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
    context.log.info("Inserting or merging data into SQL Server.")
    
    error_log = []
    duplicate_log = []
    successful_inserts = False  # Flag to track if any rows were inserted

    with sqlserver_conn.cursor() as cursor_sql:
        # Retrieve field names before executing merge operations
        try:
            cursor_sql.execute("SELECT TOP 1 * FROM asnaf_transformed_v6")
            field_names = [desc[0] for desc in cursor_sql.description if desc[0].lower() != 'idno'] if cursor_sql.description else ["Unknown"]
            context.log.info(f"Retrieved field names: {field_names}")
        except Exception as e:
            context.log.error(f"Error retrieving field names: {e}")
            field_names = ["Unknown"]

        # Step 1: Fetch the latest running_no from the RunningNo table where idno is 10
        try:
            cursor_sql.execute("SELECT TOP 1 prefix, year, running_no FROM RunningNo WHERE idno = 10 ORDER BY running_no DESC")
            result = cursor_sql.fetchone()
            if result:
                prefix, year, latest_running_number = result
                context.log.info(f"Latest data fetched - Prefix: {prefix}, Year: {year}, Running Number: {latest_running_number}")
            else:
                context.log.error("No data found in RunningNo for idno = 10")
                return
        except Exception as e:
            context.log.error(f"Error fetching latest running number: {e}")
            return

        # SQL query for duplicate check
        check_duplicate_query = """
            SELECT COUNT(*) FROM asnaf_transformed_v6 
            WHERE AsnafID = ? OR IdentificationNumIC = ?
        """

        # Step 2: Increment the running number and assign to each row
        for index, row in enumerate(transformed_data):
            context.log.info(f"Processing row: {row}")
            
            asnaf_id = row[1]
            identification_num_ic = row[14]

            # Check for duplicates
            cursor_sql.execute(check_duplicate_query, (asnaf_id, identification_num_ic))
            duplicate_count = cursor_sql.fetchone()[0]
            
            if duplicate_count > 0:
                duplicate_type = "AsnafID" if asnaf_id else "IdentificationNumIC"
                log_entry = f"Duplicate found for {duplicate_type}: {asnaf_id if asnaf_id else identification_num_ic} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                duplicate_log.append(log_entry)
                context.log.info(log_entry)
                continue  # Skip to the next row

            # Step 3: Assign a new running number and insert data
            latest_running_number += 1  # Increment the running number
            running_number_formatted = f"{prefix}-{year}-{str(latest_running_number).zfill(8)}"  # Format as 'prefix-year-00000000'
            row = list(row)
            row[2] = running_number_formatted  # Assign the formatted running number to the third column (index 2)
            try:
                cursor_sql.execute(merge_data_query, row)
                successful_inserts = True  # Mark that at least one row was inserted successfully
            except Exception as e:
                context.log.error(f"Error executing query for row: {row}. Error: {e}")
                error_log.append(f"Error executing query for row: {row}. Error: {e}")
                continue  # Continue to the next row

        # Commit the data insertions if no errors occurred
        if not error_log and successful_inserts:
            sqlserver_conn.commit()
            context.log.info("Data transfer to SQL Server completed successfully.")
        else:
            context.log.warning("Data transfer completed with errors or duplicates. Check the log for details.")

    # Step 4: Update the RunningNo table with the new latest running number only if there were successful inserts
    if successful_inserts:
        try:
            with sqlserver_conn.cursor() as cursor_sql:
                update_running_no_query = """
                    UPDATE RunningNo SET running_no = ?
                    WHERE idno = 10
                """
                cursor_sql.execute(update_running_no_query, (latest_running_number,))
                sqlserver_conn.commit()
                context.log.info(f"Running number updated to {latest_running_number} for idno = 10.")
        except Exception as e:
            context.log.error(f"Error updating RunningNo table: {e}")
    else:
        context.log.info("No new records were inserted, so running number was not updated.")

    # Write logs
    write_log_file(context, error_log, 'error_insert_merge_asnaf_data', base_dir)
    write_log_file(context, duplicate_log, 'duplicate_asnaf_log', base_dir)

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
