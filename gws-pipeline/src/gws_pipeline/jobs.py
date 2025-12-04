from dagster import AssetSelection, ScheduleDefinition, define_asset_job

job_all = define_asset_job(
    "gws_pipeline_job",
    # Run all processing assets (process_events + per-app processors)
    selection=AssetSelection.groups("Processing"),
)
schedule_hourly = ScheduleDefinition(job=job_all, cron_schedule="0 6 */2 * *")
