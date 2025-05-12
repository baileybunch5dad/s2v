import multiprocessing
import threading
import io
import os
import sys
import grpc
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd

# Import the generated gRPC code
import handshake_pb2
import handshake_pb2_grpc

class HandShakeClient:
    def __init__(self, server_address, useSecureChannel):
        print(f"Initializing a 'thread' for dedicated communication to {server_address}: TLS {useSecureChannel}")        
        if useSecureChannel:
            with open("server.crt", "rb") as f:
                trusted_certs = f.read()

            credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)
            self.channel = grpc.secure_channel(server_address, credentials)
        else:
            self.channel = grpc.insecure_channel(server_address)
        # try:
        #     print("Establishing communication with channel")
        #     grpc.channel_ready_future(self.channel).result(timeout=10)
        #     print("Channel is ready!")
        # except grpc.FutureTimeoutError:
        #     print("Failed to connect to the server within the timeout period.")
        #     exit()

        self.stub = handshake_pb2_grpc.HandShakeStub(self.channel)
        
        
    
    def aggregate_local(self, ports):
        request = handshake_pb2.AggregateLocalRequest(ports=ports)
        try:
            response = self.stub.AggregateLocal(request)
            print(f"Server response: {response.return_code}")
            return True
        except grpc.RpcError as e:
            print(f"Error aggregating locally: {e.code()}: {e.details()}")
            return False

    def aggregate_global(self, servers):
        request = handshake_pb2.AggregateGlobalRequest(servers=servers)
        try:
            response = self.stub.AggregateGlobal(request)
            print(f"Server response: {response.return_code}")
            return True
        except grpc.RpcError as e:
            print(f"Error aggregating locally: {e.code()}: {e.details()}")
            return False

    def send_shutdown(self):
        request = handshake_pb2.ShutdownRequest()
        try:
            response = self.stub.Shutdown(request)
        except grpc.RpcError as e:
            print(f"ShutDown Error: {e.code()}: {e.details()}")
            return False
            


    def say_hello(self, name):
        """Send a hello message to the server."""
        request = handshake_pb2.HelloRequest(name=name)
        try:
            response = self.stub.Hello(request)
            print(f"Server response: {response.reply}")
            return True
        except grpc.RpcError as e:
            print(f"Error saying hello: {e.code()}: {e.details()}")
            print(f"Exiting PID {os.getpid()}")
            exit(1)
    
    def send_table_from_parquet(self, parquet_file):
        """Load a Parquet file into a PyArrow Table and send it to the server."""
        try:
            # Read the Parquet file into a PyArrow Table
            print(f"PID {os.getpid()} Loading Parquet file: {parquet_file}")
            table = pq.read_table(parquet_file)
            return self.send_table(table)
        except Exception as e:
            print(f"Error loading Parquet file: {e}")
            return False
    
    def send_table(self, table):
        """Send a PyArrow Table to the server."""
        try:
            # Serialize the table to IPC format
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, table.schema)
            writer.write_table(table)
            writer.close()
            buf = sink.getvalue()
            
            # Create the request
            request = handshake_pb2.ArrowTableRequest(
                serialized_table=buf.to_pybytes() # ,         table_name=table_name
            )
            
            # Send the request
            print(f"PID {os.getpid()} sending Arrow Table with {table.num_rows} rows and {table.num_columns} columns")
            response = self.stub.ProcessArrowStream(request)
            
            if response.success:
                print(f"Server successfully processed the table: {response.message}")
                # print(f"Server reports {response.row_count} rows and {response.column_count} columns")
                return True
            else:
                print(f"Server reported an error: {response.message}")
                return False
                
        except grpc.RpcError as e:
            print(f"RPC error: {e.code()}: {e.details()}")
            return False
        except Exception as e:
            print(f"Error sending table: {e}")
            return False

def create_sample_parquet_file(filename="sample_data.parquet", num_rows=1000):
    """Create a sample Parquet file for testing if none is provided."""
    # Create a sample DataFrame
    df = pd.DataFrame({
        'id': np.arange(num_rows),
        'name': [f'item-{i}' for i in range(num_rows)],
        'value': np.random.randn(num_rows),
        'category': np.random.choice(['A', 'B', 'C', 'D'], size=num_rows),
        'date': pd.date_range('2023-01-01', periods=num_rows)
    })
    
    # Write to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)
    print(f"Created sample Parquet file: {filename} with {num_rows} rows")
    return filename

def mainTest():
    # Default settings
    server_address = 'localhost:50051'
    
    # Create a client
    client = HandShakeClient(server_address, false)
    
    # First, say hello
    print("Saying hello to the server...")
    client.say_hello("KRM_CAPP_EMULATOR")
    
    # Then send a table
    if len(sys.argv) > 1:
        # Use the provided Parquet file
        parquet_file = sys.argv[1]
    else:
        # Create a sample Parquet file
        parquet_file = create_sample_parquet_file()
    
    # Send the table
    client.send_table_from_parquet(parquet_file)

def capp_thread(server_address, flist, useSecureChannel):
     # Create a client
     
    print(f"capp thread {server_address=} {useSecureChannel=}")
    
    # if useSecureChannel:
    #     with open("server.crt", "rb") as f:
    #         trusted_certs = f.read()

    #     credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)
    #     channel = grpc.secure_channel(server_address, credentials)

    # stub = handshake_pb2_grpc.HandShakeStub(channel)
    # request = handshake_pb2.HelloRequest(name=f"gRPC Test secure={useSecureChannel}")
    # response = stub.Hello(request)

    # print("Server Response:", response.reply)

    client = HandShakeClient(server_address, useSecureChannel)
    
    
    
    # First, say hello
    print("Saying hello to the server...")
    client.say_hello(f"KRM_CAPP_EMULATOR TLS={useSecureChannel}")
    
    for parquet_file in flist:
        # print(f"Processs {os.getpid()} sending arrow stream from {parquet_file}")
        client.send_table_from_parquet(parquet_file)
        

if __name__ == "__main__":
    # mainTest()
    FOLDER_PATH='./parquet'
    parquetflist = [os.path.join(root, file) for root, dirs, files in os.walk(FOLDER_PATH) for file in files if file.endswith(".parquet") and file.startswith("MV_OUT")]
    with open("ports.txt", "r") as file:
        portdata = file.read()

# Convert space-separated integers into a list
    ports = list(map(int, portdata.split()))

    useSecureChannel = False
    for argument in sys.argv[1:]:
        if argument.startswith("--secure") or argument.startswith("--tls"):
            useSecureChannel = True
        if argument.startswith("--nosecure") or argument.startswith("--notls"):
            useSecureChannel = False
    # ports = [50051,50052,50053,50054]
    server_addresses = ['localhost:' + str(i) for i in ports]
    sublists = [[],[],[],[],[]]
    which = 0
    for f in parquetflist:
        sublists[which].append(f)
        which += 1
        if which == len(ports):
            which = 0
    processes = []
    for subprocessaddress,subprocessfiles in zip(server_addresses, sublists):
        print(subprocessaddress)
        print(len(subprocessfiles))
        print(subprocessfiles[0], subprocessfiles[-1])
        process = multiprocessing.Process(target=capp_thread, args=(subprocessaddress, subprocessfiles, useSecureChannel), name="Dedicated therad for "+subprocessaddress)
        process.start()
        processes.append(process)

    for process in processes:
        process.join()        

    server_address = server_addresses[0]
    print(f"Main driver aggregating {server_address=} {useSecureChannel=}")
        
    # Create a client
    client = HandShakeClient(server_address, useSecureChannel)
    
    # First, say hello
    print("Saying hello to the server...")
    client.say_hello(f"KRM_CAPP_EMULATOR TLS={useSecureChannel}")
    
    # First, aggregate locally
    client.aggregate_local(ports)

    # then globally
    client.aggregate_global(server_addresses)
    
    # now shut down each server
    for server_address in server_addresses:
        client = HandShakeClient(server_address, useSecureChannel)
        client.send_shutdown()

    print("Finished")


