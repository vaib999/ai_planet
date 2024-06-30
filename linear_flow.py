from metaflow import FlowSpec, step
from load_from_db_and_transformation_of_data import *

class LinearFlow(FlowSpec):
      
    """
    A flow to verify you can run a basic metaflow flow.
    """
    
    # Global initializations here
    
    @step
    def start(self):
        self.next(self.get_data_from_db)

    @step
    def get_data_from_db(self):
        self.df = load_data_from_db()
        print("Data Fetched")
        self.next(self.data_transformation)

    @step
    def data_transformation(self):
        self.transformed_df = transform_data(self.df)
        print("Data Transformed")
        self.next(self.push_data_to_db)

    @step
    def push_data_to_db(self):
        write_transformed_data_to_db(self.transformed_df)
        print('Data pushed to DB')
        self.next(self.end)

    @step
    def end(self):
        print("Flow is done!")

if __name__ == '__main__':
    LinearFlow()