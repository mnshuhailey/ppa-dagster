from dagster import job, op
from ppa_migration.resources import airbyte_resource, postgres_db_resource, sqlserver_db_resource
from ppa_migration.ops.sync_ppa_asnaf import sync_ppa_asnaf
from ppa_migration.ops.create_snapshot_scheme_table import create_snapshot_scheme_table
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
from ppa_migration.ops.update_familyrelationship_snapshotid import update_familyrelationship_snapshotid
from ppa_migration.ops.update_asnaf_snapshotid import update_asnaf_snapshotid
from ppa_migration.ops.update_household_snapshotid import update_household_snapshotid

# Job definition
@job(resource_defs={"airbyte": airbyte_resource, "postgres_db": postgres_db_resource, "sqlserver_db": sqlserver_db_resource})
def ppa_migration_pipeline():
    # Step 1: Create all required scheme tables
    create_snapshot = create_snapshot_scheme_table()
    create_household = create_household_scheme_table(create_snapshot)
    create_asnaf = create_asnaf_scheme_table(create_household)
    create_school = create_school_scheme_table(create_asnaf)
    create_study = create_study_scheme_table(create_school)
    create_familyrelationship = create_familyrelationship_scheme_table(create_study)
    create_hadkifayah = create_hadkifayah_scheme_table(create_familyrelationship)

    # Step 2: Transform and insert Asnaf data
    asnaf_transformed_data = transform_asnaf_data(create_hadkifayah)
    insert_asnaf_op = insert_merge_asnaf_data(asnaf_transformed_data)

    # Step 3: Transform and insert Household data
    household_transformed_data = transform_household_data(insert_asnaf_op)
    insert_household_op = insert_household_data(household_transformed_data)

    # Step 4: Transform and insert Familyrelationship data
    familyrelationship_transformed_data = transform_familyrelationship_data(insert_household_op)
    insert_familyrelationship_op = insert_familyrelationship_data(familyrelationship_transformed_data)

    # Step 5: Update Household SnapshotID using Familyrelationship SnapshotID
    update_household_op = update_household_snapshotid(insert_familyrelationship_op)

    # Step 6: Transform and insert School data
    school_transformed_data = transform_school_data(update_household_op)
    insert_school_op = insert_school_data(school_transformed_data)

    # Step 7: Transform and insert Study data
    study_transformed_data = transform_study_data(insert_school_op)
    insert_study_op = insert_study_data(study_transformed_data)

    # Step 8: Transform and insert Hadkifayah data
    hadkifayah_transformed_data = transform_hadkifayah_data(insert_study_op)
    insert_hadkifayah_op = insert_hadkifayah_data(hadkifayah_transformed_data)

