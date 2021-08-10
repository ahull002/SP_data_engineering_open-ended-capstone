import main_etl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Pull .csv files into a parquet file
yellow = pd.read_csv('..\data\sample_yellow_tripdata.csv')
green = pd.read_csv('..\data\sample_green_tripdata.csv')
fhvhv = pd.read_csv('..\data\sample_fhvhv_tripdata.csv')
fhv = pd.read_csv('..\data\sample_fhv_tripdata.csv')

# Convert DataFrame to Apache Arrow Table
yellow_table = pa.Table.from_pandas(yellow)
green_table = pa.Table.from_pandas(green)
fhvhv_table = pa.Table.from_pandas(fhvhv)
fhv_table = pa.Table.from_pandas(fhv)

# Parquet with Brotli compression
pq.write_table(table, '../data/yellow.parquet', compression='GZIP')
pq.write_table(table, '../data/green.parquet', compression='GZIP')
pq.write_table(table, '../data/fhvhv.parquet', compression='GZIP')
pq.write_table(table, '../data/fhv.parquet', compression='GZIP')