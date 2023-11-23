import pandas as pd

from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from random import randint


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Fetches the dataset from the web
    """
    # if randint(0, 1) > 0:  # Make it fail randomly
    #     raise Exception("Random error")
    
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix dtype issues
    """
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"colums: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str):
    """
    Writes the dataset to a local file
    """
    # path = Path(f"data/{color}/{dataset_file}.parquet")
    path = f"data/{color}/{dataset_file}.parquet"
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path):
    """
    Writes the dataset to GCS
    """
    gcs_block = GcsBucket.load("gcs-bucket")
    gcs_block.upload_from_path(
        # from_path=f"{path}",
        from_path=path,
        to_path=path
    )
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str):
    """
    The main ETL function to load data from the web to GCS
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2,],
    year: int = 2021,
    color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color="yellow"
    year=2021
    months = [1, 2, 3]
    etl_parent_flow(months, year, color)