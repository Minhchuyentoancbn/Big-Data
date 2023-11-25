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
    # gcs_path = Path(f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet")
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.get_directory(
        # from_path=f"{gcs_path}",
        from_path=gcs_path,
        local_path=f"../data/"
    )
    # return Path(f"data/{gcs_path}")
    return f"../data/{gcs_path}"


@task()
def read(path) -> pd.DataFrame:
    """
    Read the data into pandas
    """
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame):
    """
    Writes the dataset to BigQuery
    """
    credentials = GcpCredentials.load("gcp-creds")
    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="bigdata-405714",
        credentials=credentials.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
    return len(df)


@flow()
def etl_gcs_to_bq(month: int, year: int, color: str):
    """
    The main ETL function to load data to BigQuery
    """

    path = extract_from_gcs(color, year, month)
    df = read(path)
    row_count = write_bq(df)
    return row_count


@flow(log_prints=True)
def el_parent_gcs_to_bq(
    months: list[int] = [1, 2,],
    year: int = 2021,
    color: str = "yellow"
):
    """
    Main EL Flow to load data into BigQuery
    """
    total_rows = 0
    for month in months:
        rows = etl_gcs_to_bq(month, year, color)
        total_rows += rows
    
    print(f"Total rows: {total_rows}")


if __name__ == "__main__":
    el_parent_gcs_to_bq(months=[2, 3], year=2019, color="yellow")