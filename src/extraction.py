import pandas as pd
import wbgapi as wb
from dotenv import load_dotenv
import sys
import os
import pickle

from worldindicators.config import data_dir
from worldindicators.custom_funcs import (
    get_world_bank_indicators,
    upload_to_bigquery,
    get_world_bank_data,
)


load_dotenv()  # take environment variables from .env.

project_id = os.getenv(
    "GCP_PROJECT_ID"
)  # get the project ID from the environment variables

time_range = range(2014, 2025)  # range of years to fetch data for

df_indicators = get_world_bank_indicators(
    db=2
)  # get the indicators list from the World Bank API

# uploading the indicators to BigQuery
upload_to_bigquery(df_indicators, project_id, "raw_data", "world_bank_indicators")

indicators_list = df_indicators["id"].tolist()

# getting the indicators data
df_data = get_world_bank_data(indicators_list, time=time_range, store_data=True)

# adjusting the names of the columns for BQ upload
df_data.rename(columns={"series": "Series ID", "economy": "Economy"}, inplace=True)

# uploading the data to BigQuery
upload_to_bigquery(df_data, project_id, "raw_data", "world_bank_data")
