import pandas as pd
from sqlalchemy import create_engine

# Replace with your actual database details
hostname = 'localhost'
database = 'AI_Planet'
username = 'postgres'
pwd = 'xyz123'
port_id = '5432'

# Create a SQLAlchemy engine
connection_string = f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}'
engine = create_engine(connection_string)

# Query to retrieve data
query = "SELECT * FROM airbnb_listings"  # Adjust your_table_name and query as needed

# Execute query and read into DataFrame
df = pd.read_sql(query, engine)

#Transformation
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

hostname = 'localhost'
database = 'AI_Planet'
username = 'postgres'
pwd = 'xyz123'
port_id = '5432'

connection_string = f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}'
engine = create_engine(connection_string)

try:
    table_name = 'airbnb_listings_transformed'  # Replace with your actual table name
    df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000, method='multi')

except Exception as error:
    print(error)
finally:
    print('done')