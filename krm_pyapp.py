# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC helloworld.Greeter server."""

from concurrent import futures
# import logging
import os
import grpc
import handshake_pb2
import handshake_pb2_grpc
import pandas as pd
import numpy as np
import multiprocessing
import threading
import time
import sys
from DynamicDist import DynamicDist
from datetime import datetime

ports = [50051,50052,50053,50054]
if len(sys.argv) > 1:
    for i, arg in enumerate(sys.argv):
        print(f"Argument {i}: {arg}")
        if arg.startswith("--ports"):
            portstr = arg[8:]
            ports = list(map(int, portstr.split(',')))
else:
    print("No arguments provided.")

print(f"Using ports {ports}")

class HandShakeServer(handshake_pb2_grpc.HandShakeServicer):

    # def __init__(self, server):
    #     self.server = server
        # self.logger = logging.getLogger('HandShakeService')

    def setPortIdServer(self, port, id, server):
        self.port = port
        self.id = id
        self.server = server
        self.hists = {}
        self.printed = False

    def ProcessData(self, request, context):
        """
        Process incoming data from the C++ client
        """
        # Prepare response
        response = handshake_pb2.ProcessDataResponse()
        
        # Convert protobuf to pandas DataFrame
        df = self._protobuf_to_dataframe(request.data)
        
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
            
            # Process values in the column
            for value in column.values:
                if value.HasField('double_value'):
                    column_values.append(value.double_value)
                elif value.HasField('string_value'):
                    column_values.append(value.string_value)
                elif value.HasField('long_value'):
                    l = value.long_value
                    dt = datetime.fromtimestamp(l).strftime("%Y-%m-%d")
                    column_values.append(dt)
                else:
                    column_values.append(None)  # Handle empty values
            
            # Add the column to our data dictionary
            data[column_name] = column_values
        
        # Create and return DataFrame
        pdf = pd.DataFrame(data)
        return pdf

    def SendTable(self, request, context):
        dname: str = request.doublename
        dvals: np.array = np.array(request.doublevalues, dtype=np.float64)
        sname: str = request.stringname
        svals: np.array = np.array(request.stringvalues, dtype=str)
        dnametoks  = dname.split(' ')
        nds = len(dnametoks)
        nrows = len(dvals) / nds
        dc = {}
        for i in range(nds):
            colname = dnametoks[i]
            startidx = int(i*nrows)
            endidx = int((i+1)*nrows)
            coldata = dvals[startidx:endidx]
            dc[colname] = coldata
        snametoks  = sname.split(' ')
        nss = len(snametoks)
        nrows = len(svals) / nss
        for i in range(nds):
            colname = snametoks[i]
            startidx = int(i*nrows)
            endidx = int((i+1)*nrows)
            coldata = svals[startidx:endidx]
            dc[colname] = coldata
        
        df = pd.DataFrame(dc)
        if not self.printed:
            print(f"First frame recieved on {os.getpid()=} received\n{df}")
            self.printd = True
        grps = df.groupby(by=['scenario'])
        for grpid, grp in grps:
            # print(f"On {os.getpid()=} adding {grpid=}")
            # print(grp)
            key = grpid[0]
            if key not in self.hists.keys():
                dd = DynamicDist()
                self.hists[key] = dd
            else:
                dd = self.hists[key]
            histdata = grp['interest'].to_numpy()
            print(f"On {os.getpid()=} adding {histdata[:3]}... to hist for {key=} ")
            dd.add_many(histdata)
        return handshake_pb2.ArrayResponse(message="Array received successfully!")

    def AggregateLocal(self, request, context):
        print(f"On {os.getpid()=} AggregateLocal")
        ports: np.array = np.array(request.ports, dtype=int)
        if len(ports) > 1:
            for p in ports[1:]:
                channelName = "localhost:" + str(p)
                print(f'Thread 0 aggregating its results with {channelName}')
                channel = grpc.insecure_channel(channelName)
                stub = handshake_pb2_grpc.HandShakeStub(channel)
                # print(f'Thread 0 aggregating its results with {channelName}')
                otherThreadResults = stub.GetResults(handshake_pb2.GetResultsRequest(which_results=0))
                print(f'Python thread 0 received this response from {channelName}')
                dname: str = otherThreadResults.doublename
                dvals: np.array = np.array(otherThreadResults.doublevalues, dtype=np.float64)
                sname: str = otherThreadResults.stringname
                svals: np.array = np.array(otherThreadResults.stringvalues, dtype=str)
                df = pd.DataFrame({dname: dvals, sname: svals})
                print(df.round(2))
        else:
            print('Thread 0 aggregating as sole thread')
        return handshake_pb2.AggregateLocalResponse(return_code=0)

    def AggregateGlobal(self, request, context):
        print(f"On {os.getpid()=} AggregateGlobal")
        if len(request.servers) > 0:
            ports: np.array = np.array(request.ports, dtype=int)
            hosts: np.array = np.array(request.servers, dtype=str)
            for server in hosts:
                channelName = server + ":" + str(ports[0])
                print(f'Server 0 aggregating globally its results with {channelName}')
                channel = grpc.insecure_channel(channelName)
                stub = handshake_pb2_grpc.HandShakeStub(channel)
                # print(f'Server 0 aggregating its results with {channelName}')
                otherThreadResults = stub.GetResults(handshake_pb2.GetResultsRequest(which_results=1))
                print(f'Server thread 0 received this response from {channelName}')
                dname: str = otherThreadResults.doublename
                dvals: np.array = np.array(otherThreadResults.doublevalues, dtype=np.float64)
                sname: str = otherThreadResults.stringname
                svals: np.array = np.array(otherThreadResults.stringvalues, dtype=str)
                df = pd.DataFrame({dname: dvals, sname: svals})
                print(df.round(2))
        else:
            print('Thread 0 aggregating globally as sole host')
        return handshake_pb2.AggregateGlobalResponse(return_code=0)
    
    def SendDataFrame(self, request, context):
        print(request.columns)
        dct = {}
        for col in request.columns:
            # print(f'{col.name=}')
            if col.HasField("string_values"):
                # print("string columns")
                strvals: np.array = np.array(col.string_values.values, dtype=str)
                # print(strvals)
                dct[col.name] = strvals
            elif col.HasField('double_values'):
                # print("numeric columns")
                doublevals: np.array = np.array(col.double_values.values, dtype=np.float64)
                # print(doublevals)
                dct[col.name] = doublevals
            elif col.HasField('long_values'):
                # print("numeric columns")
                dtvals: np.array = np.array(col.datetime_values.values, dtype=np.int64)
                dtvals = dtvals.astype('datetime64[s]')
                # print(doublevals)
                dct[col.name] = dtvals
            else:
                print('Unknown column type')
        print(pd.DataFrame(dct))
        return handshake_pb2.StringResponse(str="It worked")

    def Hello(self, request, context):
        instr = str(request.name)
        print(f"Hello from TestMethod, received {instr}  {os.getpid()=} ")
        outstr = instr + " and back "
        return handshake_pb2.HelloResponse(reply=outstr)

    def GetResults(self, request, context):
        np.random.seed(0)
        dvals = np.random.uniform(low=100,high=200, size=3)
        response = handshake_pb2.StackedTable(doublename="Rocco"+str(self.id),
            doublevalues=dvals,
            stringname="Sameer"+str(self.id), 
            stringvalues=["IRBB","VAR","Net"])
        return response
    
    def Shutdown(self, request, context):
        print(f"Shutdown RPC called. Post message to terminate server.  {self.port=}")
        shutdown_thread = threading.Thread(target=self._shutdown, args=())
        shutdown_thread.daemon = True
        shutdown_thread.start()
        print(f"Daemon thread started, returning.  {self.port=}")
        return handshake_pb2.ShutdownResponse(status=f"Server shutting down {self.port=}")

    def _shutdown(self):
        time.sleep(1)  # Brief delay to allow response to be sent
        self.server.stop(grace=3)
        # sys.exit(0)

def serve(port:int=50051, id: int=0):
    port = str(port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    hs = HandShakeServer()
    hs.setPortIdServer(port, id, server)
    handshake_pb2_grpc.add_HandShakeServicer_to_server(hs, server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print(f"Server started, listening on {port} from {os.getpid()=}")
    server.wait_for_termination()
    # print("Waiting on stop event")
    # server_servicer = server._state.generic_handlers[0].service.servicer
    # server_servicer.server_stop_event.wait()
    # print("Stop event recieved, stopping")
    # server.stop(0)


if __name__ == "__main__":
    # logging.basicConfig( level=logging.INFO)
    processes = []
    for idx, port in enumerate(ports):
        process = multiprocessing.Process(target=serve, args=(port, idx), name="Server"+str(idx))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()
