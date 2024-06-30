from load_from_db_and_transformation_of_data import *

df = load_data_from_db()
print(df.head())
transformed_df = transform_data(df)
print(transformed_df.head())
write_transformed_data_to_db(transformed_df)