from dagster import AssetExecutionContext, asset
from gws_pipeline.core.process import process_recent_token_activity
from gws_pipeline.core.config import settings

# from gws_pipeline.core.analyze import analyze as run_analysis
from gws_pipeline.defs.ingestion.assets import raw_token_activity_incremental


@asset(deps=[raw_token_activity_incremental], group_name="Processing", name="processed_events")
def processed_events(context: AssetExecutionContext) -> None:
    process_recent_token_activity()
    context.log.info(f"Processed under {settings.processed_data_dir.relative_to(settings.base_dir)}")


# @asset(deps=[processed_events], group_name="Analytics")
# def insights(context: AssetExecutionContext) -> str:
#     run_analysis()
#     return "ok"
