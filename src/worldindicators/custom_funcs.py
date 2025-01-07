import wbgapi as wb  # API package for extraction World Bank Data
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions
import os
from time import time
from worldindicators.config import data_dir
import pickle

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

# API documentation URL
# https://datacatalog.worldbank.org/search/dataset/0037798/Global-Economic-Monitor

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./gcp_service_key.json"

project_id = os.getenv("GCP_PROJECT_ID")


def timer_function(func):
    """
    Decorator to time a function and print the elapsed time
    """

    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        print(f"Elapsed time: {end - start} seconds")
        print("-----------------------------")
        return result

    return wrapper


@timer_function
def get_world_bank_indicators(db=None):
    """
    Get a list of all available World Bank indicators.

    Returns:
    pandas.DataFrame: A DataFrame containing all World Bank indicators
    save the DataFrame as a pickle file in the data/auxiliar directory

    db=None: str: The name of the database to use. If None, the default database is used.
    """
    print("Getting indicators...")

    # Get all indicators
    indicators = wb.series.info()

    # Convert to DataFrame
    indicators_dict = vars(indicators)
    df = pd.DataFrame.from_dict(indicators_dict.get("items"))
    print("Indicators obtained...")

    # Save to pickle file
    with open(data_dir / "auxiliar" / "indicators_df.pkl", "wb") as f:
        pickle.dump(df, f)
    print("Indicators df saved as pickle...")

    return df


@timer_function
def get_world_bank_data(
    indicator, time, labels=True, quantity_paging=10, store_data=False
) -> pd.DataFrame:
    """
    Get World Bank data for a specific indicator.
    Save the list as a pickle file in the data/auxiliar directory for iterative fetching

    Parameters:
    indicator (str): The World Bank indicator code
    time (list): A list of years to get data for
    labels (bool): Whether to include indicator labels in the DataFrame
    quantity_paging: Determines if the data should be retrieved using quantity paging. If None, the default value is used.
    store_data: bool: If True, the data is stored in a feather file at data/raw directory. If False, the data is not saved

    Returns:
    pandas.DataFrame: A DataFrame containing the World Bank data
    """

    # Get data, using quantity paging if necessary
    print("Getting data...")
    indicator_iterator = indicator
    df_concatenated = pd.DataFrame()

    while len(indicator_iterator) > 0:  # iterate over the list of indicators
        indicator = indicator_iterator[:quantity_paging]
        indicator_iterator = indicator_iterator[quantity_paging:]
        print(f"Getting data for {indicator}...")

        # Save indicator to pickle file
        with open(data_dir / "auxiliar" / "indicator.pkl", "wb") as f:
            pickle.dump(indicator, f)

        # Save indicator_iterator to pickle file
        with open(data_dir / "auxiliar" / "indicator_iterator.pkl", "wb") as f:
            pickle.dump(indicator_iterator, f)

        df = wb.data.DataFrame(indicator, time=time, labels=labels)
        df.reset_index(
            inplace=True
        )  # reset index so 'economy' and 'series' become columns
        df_concatenated = pd.concat(
            [df_concatenated, df], axis=0
        )  # concatenate the dataframes

        if store_data:
            df_concatenated.to_csv(data_dir / "raw" / "indicator_data.csv")

    print("Data extracted")

    return df_concatenated


@timer_function
def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """
    Upload a pandas DataFrame to BigQuery, creating the table if it doesn't exist
    or updating it if it does.

    Parameters:
    df (pandas.DataFrame): The DataFrame to upload
    project_id (str): Your Google Cloud project ID
    dataset_id (str): The BigQuery dataset ID
    table_id (str): The BigQuery table ID
    """

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Create full table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Configure job
    job_config = bigquery.LoadJobConfig(
        # Specify write disposition - WRITE_TRUNCATE will overwrite the table if it exists
        write_disposition="WRITE_TRUNCATE",
        # Automatically detect schema from DataFrame
        autodetect=True,
    )

    try:
        # Load data to BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

        # Wait for job to complete
        job.result()

        print(f"Successfully uploaded {len(df)} rows to {table_ref}")

    except exceptions.NotFound:
        print(f"Dataset {dataset_id} not found. Creating dataset...")

        # Create dataset if it doesn't exist
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset = client.create_dataset(dataset)

        # Try upload again
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

        job.result()
        print(
            f"Successfully created dataset and uploaded {len(df)} rows to {table_ref}"
        )

    except Exception as e:
        print(f"Error uploading to BigQuery: {str(e)}")


@timer_function
def append_to_bigquery(df, project_id, dataset_id, table_id):
    """
    Upload a pandas DataFrame to BigQuery, appending the data to the table if it exists

    Parameters:
    df (pandas.DataFrame): The DataFrame to upload
    project_id (str): Your Google Cloud project ID
    dataset_id (str): The BigQuery dataset ID
    table_id (str): The BigQuery table ID
    """

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Create full table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Configure job
    job_config = bigquery.LoadJobConfig(
        # Specify write disposition - WRITE_APPEND will append the data to the table if it exists
        write_disposition="WRITE_APPEND",
        # Automatically detect schema from DataFrame
        autodetect=True,
    )

    try:
        # Load data to BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

        # Wait for job to complete
        job.result()

        print(f"Successfully uploaded {len(df)} rows to {table_ref}")

    except exceptions.NotFound:
        print(f"Dataset {dataset_id} not found. Creating dataset...")

        # Create dataset if it doesn't exist
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset = client.create_dataset(dataset)

        # Try upload again
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

        job.result()
        print(
            f"Successfully created dataset and uploaded {len(df)} rows to {table_ref}"
        )

    except Exception as e:
        print(f"Error uploading to BigQuery: {str(e)}")


"""
df = wb.data.DataFrame("IC.REG.DURS", time=range(2010, 2020), labels=True)
upload_to_bigquery(df, "world-indicators-447017", "raw_world_bank", "IC_REG_DURS")

df2 = wb.data.DataFrame("FR.INR.RINR", time=range(2010, 2020), labels=True)
upload_to_bigquery(df, "world-indicators-447017", "raw_world_bank", "FR_INR_RINR")
"""
