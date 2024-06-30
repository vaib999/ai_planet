import pandas as pd
from sqlalchemy import create_engine, text

initial_load_schema = '''
        CREATE TABLE IF NOT EXISTS airbnb_listings (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            host_id INT,
            host_name VARCHAR(20),
            neighbourhood_group VARCHAR(20),
            neighbourhood VARCHAR(30),
            latitude FLOAT,
            longitude FLOAT,
            room_type VARCHAR(30),
            price INT,
            minimum_nights INT,
            number_of_reviews INT,
            last_review DATE,
            reviews_per_month FLOAT,
            calculated_host_listings_count INT,
            availability_365 INT
        )
'''

transformed_schema = '''
        CREATE TABLE IF NOT EXISTS airbnb_listings_transformed (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            host_id INT,
            host_name VARCHAR(20),
            neighbourhood_group VARCHAR(20),
            neighbourhood VARCHAR(30),
            latitude FLOAT,
            longitude FLOAT,
            room_type VARCHAR(30),
            price INT,
            minimum_nights INT,
            number_of_reviews INT,
            last_review DATE,
            reviews_per_month FLOAT,
            calculated_host_listings_count INT,
            availability_365 INT,
            review_month INT
        )
'''

def establish_connection():
    hostname = 'host.docker.internal'
    # hostname = 'localhost'
    database = 'AI_Planet'
    username = 'postgres'
    pwd = 'xyz123'
    port_id = '5432'

    connection_string = f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}'
    engine = create_engine(connection_string)

    return engine

def write_initial_load_data():
    df = pd.read_csv('AB_NYC_2019.csv')

    engine = establish_connection()

    with engine.connect() as connection:
        connection.execute(text(initial_load_schema))

    table_name = 'airbnb_listings'  # Replace with your actual table name
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000, method='multi')

def load_data_from_db():
    query = "SELECT * FROM airbnb_listings"  # Adjust your_table_name and query as needed

    df = pd.read_sql(query, establish_connection())

    return df

def write_transformed_data_to_db(df):
    engine = establish_connection()

    with engine.connect() as connection:
        connection.execute(text(transformed_schema))

    table_name = 'airbnb_listings_transformed'  # Replace with your actual table name
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000, method='multi')

def transform_data(df):
    df.fillna({'reviews_per_month':0}, inplace=True)
    # print(df.reviews_per_month.isnull().sum())

    df.dropna(how='any',inplace=True)
    df.info()

    # print(df['last_review'])

    nghb = df.neighbourhood.value_counts()
    # print(nghb.head(50))

    df['last_review'] = pd.to_datetime(df['last_review'])
    df['review_month'] = df['last_review'].dt.month

    # print(df.columns)

    return df