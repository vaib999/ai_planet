import pandas as pd
from sqlalchemy import create_engine, text

# Schema for initial data load
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

# Schema for cleaned and transformed data
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

# Connection with
def establish_connection():
    """
    Returns connection to postgres database
    """

    hostname = 'host.docker.internal' # For establishing connection from Docker container to postgres
    # hostname = 'localhost'
    database = 'AI_Planet'
    username = 'postgres'
    pwd = 'xyz123'
    port_id = '5432'

    connection_string = f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}'
    engine = create_engine(connection_string)

    return engine

def write_initial_load_data():
    """
    Creates table in database if not there and write csv data in the table
    """
    
    df = pd.read_csv('AB_NYC_2019.csv')

    engine = establish_connection()

    with engine.connect() as connection:
        connection.execute(text(initial_load_schema))

    # pandas to_sql method uses parameters like chunksize and method so that 
    # chunks of rows are added in one attempt making write operation more efficient
    table_name = 'airbnb_listings'
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000, method='multi')

def load_data_from_db():
    """
    Read data from database table
    """
    query = "SELECT * FROM airbnb_listings" 

    df = pd.read_sql(query, establish_connection())

    return df

def write_transformed_data_to_db(df):
    """
    Creates table in database if not there and write csv data in the table
    """
    
    engine = establish_connection()

    with engine.connect() as connection:
        connection.execute(text(transformed_schema))

    table_name = 'airbnb_listings_transformed'  # Replace with your actual table name
    df.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000, method='multi')

def transform_data(df):
    """
    Transform data like cleaning, manipulation, creation of new columns and much more 
    """

    # Fill all NA in column in place
    df.fillna({'reviews_per_month':0}, inplace=True)

    # Drop all NA rows
    df.dropna(how='any',inplace=True)
    # df.info()

    # nghb = df.neighbourhood.value_counts()
    # print(nghb.head(50))

    # Convert last_review to datetime column type
    df['last_review'] = pd.to_datetime(df['last_review'])
    # Extract Month and create new column for month
    df['review_month'] = df['last_review'].dt.month

    return df