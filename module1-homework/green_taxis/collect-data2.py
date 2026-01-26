#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


# dtype = {
#     "VendorID": "Int64",
#     "passenger_count": "Int64",
#     "trip_distance": "float64",
#     "RatecodeID": "Int64",
#     "store_and_fwd_flag": "string",
#     "PULocationID": "Int64",
#     "DOLocationID": "Int64",
#     "payment_type": "Int64",
#     "fare_amount": "float64",
#     "extra": "float64",
#     "mta_tax": "float64",
#     "tip_amount": "float64",
#     "tolls_amount": "float64",
#     "improvement_surcharge": "float64",
#     "total_amount": "float64",
#     "congestion_surcharge": "float64"
# }

# parse_dates = [
#     "tpep_pickup_datetime",
#     "tpep_dropoff_datetime"
# ]




#print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))



@click.command()
@click.option('--pg-user', default='root')
@click.option('--pg-pass', default='root')
@click.option('--pg-host', default='localhost')
@click.option('--pg-db', default='ny_taxi')
@click.option('--pg-port', type=int, default=5432)
@click.option('--year', type=int, default=2021)
@click.option('--month', type=int, default=1)
@click.option('--chunksize', type=int, default=100000)
@click.option('--target-table', default='yellow_taxi_data')
def run(pg_user, pg_pass, pg_host, pg_db, pg_port, year, month, chunksize, target_table):

#    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
#    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_csv(url)
    df.to_sql(name=target_table,
                con=engine,
                if_exists='replace')

if __name__ == '__main__':
    run()