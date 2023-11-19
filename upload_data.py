import pandas as pd

from sqlalchemy import create_engine
from time import time

if __name__ == '__main__':
    # Connect to database
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    
    # Create table
    df_iter = pd.read_csv('data/yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

    # Insert data into table
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    while True: 
        t_start = time()
        try:
            df = next(df_iter)
        except StopIteration:
            print('Done inserting data')
            break
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))
