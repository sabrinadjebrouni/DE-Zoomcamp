#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import click

@click.command()
@click.option('--user', default='postgres', help='PostgreSQL user')
@click.option('--password', default='postgres', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5433, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
def main(user, password, host, port, db, table):

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read data
    df_trips = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet')

    df_zones = pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')


    #ehail_fee is an empty column taking space so we need to clean it
    df_trips = df_trips.drop(columns=['ehail_fee'])
   


    #use int64(BIGINT) is better for IDs, format of other columns is OK
    df_trips = df_trips.astype({
        "VendorID": "Int64",
        "PULocationID": "Int64",
        "DOLocationID": "Int64"
    })


    #convert into lowercase so we don't need to use double quotes for columns names
    df_trips.columns = [c.lower() for c in df_trips.columns]

    df_zones.columns = [c.lower() for c in df_zones.columns]


    print(pd.io.sql.get_schema(df_trips, name='yellow_taxi_data', con=engine))


    print(pd.io.sql.get_schema(df_zones, name='zones', con=engine))


    #I didn't break into chunks because we have small amount of data, only 46912 rows

    df_trips.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


    df_zones.to_sql(name='zones', con=engine, if_exists='append')

if __name__ == '__main__':
    main()



