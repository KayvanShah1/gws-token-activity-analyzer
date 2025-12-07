from dagster import (
    AssetSelection,
    DagsterRunStatus,
    RunRequest,
    ScheduleDefinition,
    define_asset_job,
    run_status_sensor,
)

INGESTION_GROUPS = ("raw_fetch", "light_transform", "warehouse_publish")
ANALYTICS_GROUPS = ("staging", "marts")


job_ingestion = define_asset_job(
    name="gws_ingestion_job",
    selection=AssetSelection.groups(*INGESTION_GROUPS),
    description="Fetches, processes, and loads Google Workspace audit activity into DuckDB.",
)

job_analytics = define_asset_job(
    name="gws_analytics_job",
    selection=AssetSelection.groups(*ANALYTICS_GROUPS),
    description="Builds analytics models (dbt) on top of the audit warehouse.",
)

schedule_ingestion_every_2d = ScheduleDefinition(job=job_ingestion, cron_schedule="0 6 */2 * *")


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, monitored_jobs=[job_ingestion])
def trigger_analytics_after_ingestion(context):
    return RunRequest(
        run_key=f"analytics_after_{context.dagster_run.run_id}",
        job_name=job_analytics.name,
        tags={"source_run_id": context.dagster_run.run_id},
    )
