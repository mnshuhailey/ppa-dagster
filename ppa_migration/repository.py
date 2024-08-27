from dagster import repository
from ppa_migration.jobs import ppa_migration_pipeline
from ppa_migration.schedules import daily_ppa_migration_pipeline_schedule

@repository
def ppa_repo():
    return [ppa_migration_pipeline, daily_ppa_migration_pipeline_schedule]
