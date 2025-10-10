from pathlib import Path
from dagster_dbt import DbtCliResource, DbtProject

dbt_project_directory = Path(__file__).absolute().parent / "defs" / "dbt_assets"
dbt_project = DbtProject(project_dir=dbt_project_directory)
dbt_profile = Path.home() / ".dbt"
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(
    project_dir=str(dbt_project_directory), profiles_dir=str(dbt_profile)
)
