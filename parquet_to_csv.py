import pyarrow.parquet as pq
import pandas as pd

# Read the Parquet file
parquet_file = 'yellow_tripdata_2021-01.parquet'
table = pq.read_table(parquet_file)

# Convert to DataFrame and select the first 100 rows
df = table.to_pandas()
# df.head(100).to_csv('first_100_rows.csv', index=False)
df.to_csv('yellow_tripdata_2021-01.csv', index=False)
