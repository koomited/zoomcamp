import pandas as pd 
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    csv_name = "output.csv"
    
    parquet_name = "output.parquet"
    
    os.system(f"wget {url} -O {parquet_name}")
    
    
    df = pd.read_parquet(parquet_name)
    
    df.to_csv(csv_name, index=False)
    
    
    
    
    
    #download the csv
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    engine.connect()
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    

    df  = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")



    
    while True:
        df  = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists="append")
        
        print("Inserting another chunck")


parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")


# user , password, host, port database name, table name
# url of the csv
if __name__=="__main__":
    
    parser.add_argument('user', help="username for postgres")
    parser.add_argument('password', help="password for postgres")
    parser.add_argument('host', help="host for postgres")
    parser.add_argument('port', help="port for postgres")
    parser.add_argument('db', help="database name for postgres")
    parser.add_argument('table_name', help="table to send tha results")
    parser.add_argument('url', help="url of the csv file")

    args = parser.parse_args()
    
    main(args)




