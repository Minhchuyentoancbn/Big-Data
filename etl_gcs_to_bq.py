import pandas as pd

from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """
    Download the data from GCS
    """
    gcs_path = Path(f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet")
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.get_directory(
        from_path=f"{gcs_path}",
        local_path=f"data/"
    )
    return Path(f"data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """
    Clean the data
    """
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame):
    """
    Writes the dataset to BigQuery
    """
    credentials = GcpCredentials.load("gcp-creds")
    df.to_gbq(
        destination_table="trips_data_all.yellow_tripdata",
        project_id="bigdata-405714",
        credentials=credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
    return


@flow()
def etl_gcs_to_bq():
    """
    The main ETL function to load data to BigQuery
    """
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()