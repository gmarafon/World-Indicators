# export the tables from Bigquery to Google Drive

from google.cloud import bigquery
from dotenv import load_dotenv
import os
from worldindicators.config import gdrive_dir
from worldindicators.custom_funcs import export_bq_table_to_gdrive


load_dotenv()  # take environment variables from .env.

project_id = os.getenv(
    "GCP_PROJECT_ID"
)  # get the project ID from the environment variables

# Define the tables to export
tables_list = [
    "dbt_world_indicators.marts_dimension_series",
    "dbt_world_indicators.marts_dimension_countries",
    "dbt_world_indicators.marts_fact_world_indicators",
]


# Iterate over the list of tables and export them to Google Drive
for table in tables_list:
    print(tables_list)

    export_bq_table_to_gdrive(
        dataset_table=table,
        destination_uri_path=gdrive_dir,
        project_id=project_id,
    )
