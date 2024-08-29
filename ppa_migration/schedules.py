from dagster import schedule
from ppa_migration.jobs import ppa_migration_pipeline

# Define the schedule
@schedule(
    cron_schedule="0 0 * * *",  # This cron expression runs the pipeline at midnight every day
    job=ppa_migration_pipeline,
    execution_timezone="UTC",
)
def daily_ppa_migration_pipeline_schedule(_context):
    return {}
