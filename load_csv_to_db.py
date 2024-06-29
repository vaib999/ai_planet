import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv('AB_NYC_2019.csv')
print(df.columns)

hostname = 'localhost'
database = 'AI_Planet'
username = 'postgres'
pwd = 'xyz123'
port_id = '5432'

connection_string = f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}'
engine = create_engine(connection_string)

try:
    table_name = 'airbnb_listings'  # Replace with your actual table name
    df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000, method='multi')

except Exception as error:
    print(error)
finally:
    print('done')