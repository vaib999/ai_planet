import psycopg2

hostname = 'localhost'
database = 'AI_Planet'
username = 'postgres'
pwd = 'xyz123'
port_id = '5432'

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )

    cur = conn.cursor()

    create_table_script = '''
        CREATE TABLE IF NOT EXISTS airbnb_listings_transformed (
            id INT,
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

    cur.execute(create_table_script)

    conn.commit()
except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()