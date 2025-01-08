import pandas as pd 
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
    
    
    os.system(f"wget -O temp.gz {url} && gunzip -c temp.gz > {csv_name} && rm temp.gz")
    

    
    #download the csv
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    

    df  = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")


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




