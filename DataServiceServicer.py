import grpc
import pandas as pd
import numpy as np
from concurrent import futures
import time

# Import the generated classes
import dataservice_pb2
import dataservice_pb2_grpc

class DataServiceServicer(dataservice_pb2_grpc.DataServiceServicer):
    def ProcessData(self, request, context):
        """
        Process incoming data from the C++ client
        """
        # Prepare response
        response = dataservice_pb2.StringResponse()
        
        print(request)
        df = self._protobuf_to_dataframe(request)
        
        # Print the received data
        print("Received data:")
        print(df)
        
        # Perform processing on the dataframe
        # For this example, we'll just report some basic statistics
        response.status = f"Successfully processed {len(df)} rows with {len(df.columns)} columns"
        
        return response
    
    def _protobuf_to_dataframe(self, table):
        """
        Convert a protobuf Table to a pandas DataFrame
        """
        data = {}
        
        # Process each column
        for column in table.columns:
            column_name = column.name
            column_values = []
            
            print(column)
            # Process values in the column
            # for value in column.values:
            if column.HasField('double_array'):
                column_values = list(column.double_array.v)
                # column_values.append(value.double_value)
            elif column.HasField('long_array'):
                # column_values.append(value.long_value)
                column_values = list(column.long_array.v)
            elif column.HasField('string_array'):
                # column_values.append(value.string_value)
                column_values = list(column.string_array.v)
            else:
                column_values = []
            
            # Add the column to our data dictionary
            data[column_name] = column_values
        
        # Create and return DataFrame
        return pd.DataFrame(data)

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add the service to the server
    dataservice_pb2_grpc.add_DataServiceServicer_to_server(
        DataServiceServicer(), server)
    
    # Listen on port 50051
    server.add_insecure_port('[::]:50051')
    server.start()
    
    print("Server started, listening on port 50051")
    
    try:
        # Keep server running until keyboard interrupt
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()