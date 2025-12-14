from dagster_dbt import DbtCliResource, DbtProject

from gws_pipeline.core import settings

dbt_project = DbtProject(project_dir=settings.DBT_PROJECT_DIR, profiles_dir=settings.DBT_PROFILES_DIR)
dbt_resource = DbtCliResource(project_dir=dbt_project)
