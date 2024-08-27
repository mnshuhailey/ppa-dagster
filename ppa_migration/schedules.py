from dagster import schedule
from ppa_migration.jobs import ppa_migration_pipeline

# Define the schedule
@schedule(
    cron_schedule="0 0 * * *",  # This cron expression runs the pipeline at midnight every day
    job=ppa_migration_pipeline,
    execution_timezone="UTC",  # Set the timezone if needed
)
def daily_ppa_migration_pipeline_schedule(_context):
    return {}  # This is where you'd provide any config needed for the job run
