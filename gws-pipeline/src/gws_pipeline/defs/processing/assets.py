from dagster import AssetExecutionContext, asset
from gws_pipeline.core import settings
from gws_pipeline.core.process import process_recent_token_activity_polars_lazy
from gws_pipeline.defs.ingestion.assets import raw_token_activity_incremental


@asset(deps=[raw_token_activity_incremental], group_name="Processing", name="process_events")
def process_events(context: AssetExecutionContext) -> None:
    process_recent_token_activity_polars_lazy()
    context.log.info(f"Processed under {settings.processed_data_dir.relative_to(settings.base_dir)}")
