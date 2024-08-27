from dagster import job
from ppa_migration.resources import airbyte_resource, postgres_db_resource, sqlserver_db_resource
from ppa_migration.ops.sync_ppa_asnaf import sync_ppa_asnaf
from ppa_migration.ops.create_table import create_table_if_not_exists
from ppa_migration.ops.transfer_data import transfer_data_to_sqlserver

# Job definition
@job(resource_defs={"airbyte": airbyte_resource, "postgres_db": postgres_db_resource, "sqlserver_db": sqlserver_db_resource})
def ppa_migration_pipeline():
    # sync_ppa_asnaf()  # Uncomment if sync operation is needed
    create_table_if_not_exists()
    transfer_data_to_sqlserver()
