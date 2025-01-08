import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):
    # Extract parameters
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Temporary file names
    gz_file = "temp.gz"
    csv_file = "output.csv"

    # Download and decompress the file
    os.system(f"wget -O {gz_file} {url} && gunzip -c {gz_file} > {csv_file} && rm {gz_file}")

    # Create database connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read CSV in chunks and process
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    # Load the first chunk and create the table schema
    df = next(df_iter)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Write schema to database
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    print("Inserted first chunk")

    # Process remaining chunks
    while True:
        try:
            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df.to_sql(name=table_name, con=engine, if_exists="append")
            print("Inserted another chunk")
        except StopIteration:
            print("All chunks have been inserted.")
            break
        except Exception as e:
            print(f"Error while processing chunk: {e}")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    # Add command-line arguments
    parser.add_argument('user', help="Username for PostgreSQL")
    parser.add_argument('password', help="Password for PostgreSQL")
    parser.add_argument('host', help="Host for PostgreSQL")
    parser.add_argument('port', help="Port for PostgreSQL")
    parser.add_argument('db', help="Database name for PostgreSQL")
    parser.add_argument('table_name', help="Target table name")
    parser.add_argument('url', help="URL of the CSV file")

    # Parse arguments and run the main function
    args = parser.parse_args()
    main(args)
