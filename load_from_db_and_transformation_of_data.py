import pandas as pd
from sqlalchemy import create_engine

def establish_connection():
    hostname = 'host.docker.internal'
    database = 'AI_Planet'
    username = 'postgres'
    pwd = 'xyz123'
    port_id = '5432'

    connection_string = f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}'
    engine = create_engine(connection_string)

    return engine

def load_data_from_db():
    query = "SELECT * FROM airbnb_listings"  # Adjust your_table_name and query as needed

    df = pd.read_sql(query, establish_connection())

    return df

def write_transformed_data_to_db(df):
    table_name = 'airbnb_listings_transformed'  # Replace with your actual table name
    df.to_sql(table_name, establish_connection(), if_exists='replace', index=False, chunksize=1000, method='multi')

def write_initial_load_data():
    df = pd.read_csv('AB_NYC_2019.csv')

    table_name = 'airbnb_listings'  # Replace with your actual table name
    df.to_sql(table_name, establish_connection(), if_exists='replace', index=False, chunksize=1000, method='multi')

def transform_data(df):
    df.fillna({'reviews_per_month':0}, inplace=True)
    print(df.reviews_per_month.isnull().sum())

    df.dropna(how='any',inplace=True)
    df.info()

    print(df['last_review'])

    nghb = df.neighbourhood.value_counts()
    print(nghb.head(50))

    df['last_review'] = pd.to_datetime(df['last_review'])
    df['review_month'] = df['last_review'].dt.month

    print(df.columns)

    return df