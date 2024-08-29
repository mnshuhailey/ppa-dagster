from dagster import job
from ppa_migration.resources import airbyte_resource, postgres_db_resource, sqlserver_db_resource
from ppa_migration.ops.sync_ppa_asnaf import sync_ppa_asnaf
from ppa_migration.ops.create_asnaf_scheme_table import create_asnaf_scheme_table
from ppa_migration.ops.transform_asnaf_data import transform_asnaf_data
from ppa_migration.ops.insert_merge_asnaf_data import insert_merge_asnaf_data

# Job definition
@job(resource_defs={"airbyte": airbyte_resource, "postgres_db": postgres_db_resource, "sqlserver_db": sqlserver_db_resource})
def ppa_migration_pipeline():
    # sync_ppa_asnaf()  # Uncomment if sync operation is needed
    create_asnaf_scheme_table()
    transformed_data = transform_asnaf_data()
    insert_merge_asnaf_data(transformed_data)
