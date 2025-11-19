from dagster import AssetExecutionContext, AssetSelection, ScheduleDefinition, asset, define_asset_job
from gws_pipeline.core.fetcher import fetch_token_activity_buffered
from gws_pipeline.core.process import run as run_processing_pipeline
from gws_pipeline.core.analyze import analyze as run_analysis
from gws_pipeline.core.config import settings


@asset(group_name="Extraction")
def raw_events(context: AssetExecutionContext) -> str:
    fetch_token_activity_buffered()
    context.log.info(f"Raw under {settings.raw_data_dir}")
    return str(settings.raw_data_dir)


@asset(deps=[raw_events], group_name="Processing")
def processed_events(context: AssetExecutionContext) -> str:
    run_processing_pipeline()
    context.log.info(f"Processed under {settings.processed_data_dir}")
    return str(settings.processed_data_dir)


@asset(deps=[processed_events], group_name="Analytics")
def insights(context: AssetExecutionContext) -> str:
    run_analysis()
    return "ok"


job_all = define_asset_job("gws_pipeline_job", selection=AssetSelection.all())
schedule_hourly = ScheduleDefinition(job=job_all, cron_schedule="0 6 */2 * *")
