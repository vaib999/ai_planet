from metaflow import FlowSpec, step

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
        print("Data Fetched")
        self.input_data = 'Data'
        self.next(self.data_transformation)

    @step
    def data_transformation(self):
        print('the message is: %s' % self.input_data)
        self.data_transformed = 'Data_transformed'
        self.next(self.push_data_to_db)

    @step
    def push_data_to_db(self):
        print('the message is still: %s' % self.data_transformed)
        print('Data pushed to DB')
        self.next(self.end)

    @step
    def end(self):
        print("Flow is done!")

if __name__ == '__main__':
    LinearFlow()