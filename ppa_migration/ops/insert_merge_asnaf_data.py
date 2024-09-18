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
    duplicate_log = []
    
    # Retrieve field names before executing merge operations
    try:
        cursor_sql.execute("SELECT TOP 1 * FROM asnaf_transformed_v6")
        field_names = [desc[0] for desc in cursor_sql.description if desc[0].lower() != 'idno'] if cursor_sql.description else ["Unknown"]
        context.log.info(f"Retrieved field names: {field_names}")
    except Exception as e:
        context.log.error(f"Error retrieving field names: {e}")
        field_names = ["Unknown"]

    # SQL query for duplicate check
    check_duplicate_query = """
        SELECT COUNT(*) FROM asnaf_transformed_v6 
        WHERE AsnafID = ? OR IdentificationNumIC = ?
    """

    # Execute the merge query with duplicate check
    for row in transformed_data:
        asnaf_id = row.get('AsnafID')  # Assuming row is a dict
        identification_num_ic = row.get('IdentificationNumIC')
        
        # Check for duplicates
        cursor_sql.execute(check_duplicate_query, (asnaf_id, identification_num_ic))
        duplicate_count = cursor_sql.fetchone()[0]
        
        if duplicate_count > 0:
            duplicate_type = "AsnafID" if asnaf_id else "IdentificationNumIC"
            log_entry = f"Duplicate found for {duplicate_type}: {asnaf_id if asnaf_id else identification_num_ic} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            duplicate_log.append(log_entry)
            context.log.info(log_entry)
            continue  # Skip to the next row

        # If no duplicate, proceed with the merge
        try:
            cursor_sql.execute(merge_data_query, row)
        except Exception as e:
            context.log.error(f"Error executing query for row: {row}. Error: {e}")
            error_log.append(f"Error executing query for row: {row}. Error: {e}")
            continue  # Continue to the next row

    # Commit only if there were no errors
    if not error_log:
        sqlserver_conn.commit()
        context.log.info("Data transfer to SQL Server completed successfully.")
    else:
        context.log.warning("Data transfer completed with errors. Check the log for details.")

    cursor_sql.close()
    sqlserver_conn.close()

    # Write errors to a log file if any
    if error_log:
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_log_file_name = f"error_insert_merge_asnaf_data_{current_datetime}.txt"
        error_log_path = os.path.join(base_dir, '../logs', error_log_file_name)
        
        # Ensure the logs directory exists
        os.makedirs(os.path.dirname(error_log_path), exist_ok=True)

        # Write error log to the file
        with open(error_log_path, 'a') as error_file:
            error_file.write("\n".join(error_log) + "\n")
        context.log.info(f"Error log written to {error_log_path}.")

    # Write duplicates to a log file if any
    if duplicate_log:
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        duplicate_log_file_name = f"duplicate_asnaf_log_{current_datetime}.txt"
        duplicate_log_path = os.path.join(base_dir, '../logs', duplicate_log_file_name)
        
        # Ensure the logs directory exists
        os.makedirs(os.path.dirname(duplicate_log_path), exist_ok=True)

        # Write duplicate log to the file
        with open(duplicate_log_path, 'a') as duplicate_file:
            duplicate_file.write("\n".join(duplicate_log) + "\n")
        context.log.info(f"Duplicate log written to {duplicate_log_path}.")
