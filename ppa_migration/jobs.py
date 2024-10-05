from dagster import job, op
from ppa_migration.resources import airbyte_resource, postgres_db_resource, sqlserver_db_resource
from ppa_migration.ops.sync_ppa_asnaf import sync_ppa_asnaf
from ppa_migration.ops.create_asnaf_scheme_table import create_asnaf_scheme_table
from ppa_migration.ops.create_household_scheme_table import create_household_scheme_table
from ppa_migration.ops.create_familyrelationship_scheme_table import create_familyrelationship_scheme_table
from ppa_migration.ops.create_school_scheme_table import create_school_scheme_table
from ppa_migration.ops.create_study_scheme_table import create_study_scheme_table
from ppa_migration.ops.create_hadkifayah_scheme_table import create_hadkifayah_scheme_table
from ppa_migration.ops.transform_asnaf_data import transform_asnaf_data
from ppa_migration.ops.transform_familyrelationship_data import transform_familyrelationship_data
from ppa_migration.ops.transform_household_data import transform_household_data
from ppa_migration.ops.transform_study_data import transform_study_data
from ppa_migration.ops.transform_school_data import transform_school_data
from ppa_migration.ops.transform_hadkifayah_data import transform_hadkifayah_data
from ppa_migration.ops.insert_merge_asnaf_data import insert_merge_asnaf_data
from ppa_migration.ops.insert_familyrelationship_data import insert_familyrelationship_data
from ppa_migration.ops.insert_household_data import insert_household_data
from ppa_migration.ops.insert_study_data import insert_study_data
from ppa_migration.ops.insert_school_data import insert_school_data
from ppa_migration.ops.insert_hadkifayah_data import insert_hadkifayah_data

# Job definition
@job(resource_defs={"airbyte": airbyte_resource, "postgres_db": postgres_db_resource, "sqlserver_db": sqlserver_db_resource})
def ppa_migration_pipeline():
    # Step 1: Create all required scheme tables
    create_asnaf_scheme_table()
    create_household_scheme_table()
    create_familyrelationship_scheme_table()
    create_school_scheme_table()
    create_study_scheme_table()
    create_hadkifayah_scheme_table()

    # Step 2: Transform Asnaf data and insert it
    asnaf_transformed_data = transform_asnaf_data()
    insert_merge_asnaf_data(asnaf_transformed_data)

    # Step 3: Transform Familyrelationship data and insert it after Asnaf data is inserted
    familyrelationship_transformed_data = transform_familyrelationship_data()
    insert_familyrelationship_data(familyrelationship_transformed_data)

    # Step 4: Transform Household data and insert it after Familyrelationship data is inserted
    household_transformed_data = transform_household_data()
    insert_household_data(household_transformed_data)

    # Step 5: Transform Study data and insert it after Household data is inserted
    study_transformed_data = transform_study_data()
    insert_study_data(study_transformed_data)

    # Step 5: Transform School data and insert it after Study data is inserted
    school_transformed_data = transform_school_data()
    insert_school_data(school_transformed_data)

    # Step 6: Transform Hadkifayah data and insert it after School data is inserted
    hadkifayah_transformed_data = transform_hadkifayah_data()
    insert_hadkifayah_data(hadkifayah_transformed_data)
