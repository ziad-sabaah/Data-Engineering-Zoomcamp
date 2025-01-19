#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
import argparse, os, sys
import gzip
import shutil
import requests

def download_file(url, file_name):
    print(f'Downloading {file_name} ...')
    try:
        response = requests.get(url.strip(), stream=True)
        if response.status_code == 200:
            with open(file_name, 'wb') as f_out:
                shutil.copyfileobj(response.raw, f_out)
            print(f'Download complete. File size: {os.path.getsize(file_name)} bytes\n')
        else:
            print(f"Error: Could not download file. HTTP status code {response.status_code}")
            sys.exit()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit()


def decompress_gz(file_name):
    if file_name.endswith('.gz'):
        print(f'Decompressing {file_name} ...')
        with gzip.open(file_name, 'rb') as f_in:
            with open(file_name[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f'Decompressed to {file_name[:-3]}. File size: {os.path.getsize(file_name[:-3])} bytes')
        return file_name[:-3]
    return file_name

def main(parms):
    user = parms.user
    password = parms.password
    host = parms.host
    port = parms.port
    db = parms.db
    table_name = parms.table_name
    url = parms.url

    # Get the name of the file and then decompress it
    file_name = url.rsplit('/', 1)[-1].strip()
    download_file(url, file_name)
    file_name = decompress_gz(file_name)

    # Create an open SQL database connection object or a SQLAlchemy connectable
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    if '.csv' in file_name:
        df = pd.read_csv(file_name, nrows=10)
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    elif '.parquet' in file_name:
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    else: 
        print('Error. Only .csv or .parquet files allowed.')
        sys.exit()

        # Create the table
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    # Insert values
    t_start = time()
    count = 0
    for batch in df_iter:
        count+=1

        if '.parquet' in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch

        print(f'inserting batch {count}...')
        df
        b_start = time()
        batch_df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
        b_end = time()

        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')
        
    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')   


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

	# user
	# password
	# host
	# port
	# database name
	# table name
	# url of the csv

	parser.add_argument('--user', help="user name for postgres")
	parser.add_argument('--password', help="password for postgres")
	parser.add_argument('--host', help="host for postgres")
	parser.add_argument('--port', help="port for postgres")
	parser.add_argument('--db', help="database name for postgres")
	parser.add_argument('--table_name', help="name of the table where we will write the results to")
	parser.add_argument('--url', help="url of the file")

	args = parser.parse_args()
	main(args)