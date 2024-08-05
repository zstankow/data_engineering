
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os
import pyarrow.parquet as pq

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'output.parquet'
    csv_name = 'output.csv'
    os.system(f"wget {url} -O {parquet_name}")

    table = pq.read_table(parquet_name)
    df_table = table.to_pandas()
    df_table.to_csv(csv_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 

        try:
            t_start = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)


# # ### Connecing with Postgres via Jupyter and Pandas

# # In[ ]:


# # !pip install sqlalchemy psycopg2-binary


# # In[63]:


# import pandas as pd
# from sqlalchemy import create_engine
# import psycopg2

# df = pd.read_csv('first_100_rows.csv')
# df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
# df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
# df.passenger_count = df.passenger_count.astype(int)
# df.RatecodeID = df.RatecodeID.astype(int)

# engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
# engine.connect()


# # ### Using pandas

# # In[64]:


# query = """
# SELECT 1 as number;
# """

# pd.read_sql(query, con=engine)


# # In[65]:


# query = """
# SELECT *
# FROM pg_catalog.pg_tables
# WHERE schemaname != 'pg_catalog' AND
#     schemaname != 'information_schema';
# """

# pd.read_sql(query, con=engine)


# # In[66]:


# query = """
# SELECT *
# FROM yellow_taxi_data
# LIMIT 10;
# """

# pd.read_sql(query, con=engine)


# # # Connecting pgAdmin and Postgres

# # In[ ]:




