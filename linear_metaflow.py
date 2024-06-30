from metaflow import FlowSpec, step, retry, catch, schedule
from load_from_db_and_transformation_of_data import *

@schedule(hourly=True)
class LinearFlow(FlowSpec):
      
    """
    A flow to verify you can run a basic metaflow flow.
    """
    
    @step
    def start(self):
        self.next(self.get_data_from_db)

    @catch(var='load_error')
    @retry(times=2)
    @step
    def get_data_from_db(self):
        self.df = load_data_from_db()
        print("Data Fetched")
        self.next(self.data_transformation)

    @catch(var='transformation_error')
    @retry(times=2)
    @step
    def data_transformation(self):
        if self.load_error:
            print("An error occurred while loading the data from database")
        else:
            self.transformed_df = transform_data(self.df)
            print("Data Transformed")
        self.next(self.push_data_to_db)

    @catch(var='write_error')
    @retry(times=2)
    @step
    def push_data_to_db(self):
        if self.load_error or self.transformation_error:
            print("An error occurred while transforming the data")
        else:
            write_transformed_data_to_db(self.transformed_df)
            print('Data pushed to DB')
        self.next(self.end)

    @step
    def end(self):
        print("Pipeline Flow ended !")

if __name__ == '__main__':
    LinearFlow()