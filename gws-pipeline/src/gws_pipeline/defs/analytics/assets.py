import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets
from gws_pipeline.defs.analytics.resources import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path, name="dbt_models")
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
