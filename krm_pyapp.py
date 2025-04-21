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
import logging
import os
import grpc
import handshake_pb2
import handshake_pb2_grpc
import pandas as pd
import numpy as np
import multiprocessing

ports = [50051,50052,50053,50054]

class HandShakeServer(handshake_pb2_grpc.HandShake):
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
        
        # df = pd.DataFrame({dname: dvals, sname: svals})
        df = pd.DataFrame(dc)
        print(f"On {os.getpid()} received\n{df}")
        # print(f"Received floating data array {request.doublename} with {len(request.doublevalues)} values on OS process {os.getpid()}.")
        # print(f"Received character data array {request.stringname} with {len(request.stringvalues)} values on OS process {os.getpid()}.")
        # Here, you could further process the array as needed
        return handshake_pb2.ArrayResponse(message="Array received successfully!")

    def Aggregate(self, request, context):
        ports: np.array = np.array(request.ports, dtype=int)
        for p in ports[1:]:
            channelName = "localhost:" + str(p)
            print(f'Thread 0 aggregating its results with {channelName}')
            channel = grpc.insecure_channel(channelName)
            stub = handshake_pb2_grpc.HandShakeStub(channel)
            print(f'Thread 0 aggregating its results with {channelName}')
            otherThreadResults = stub.GetResults(handshake_pb2.GetResultsRequest(which_results=7))
            print(f'Python thread 0 received this response from {channelName}')
            dname: str = otherThreadResults.doublename
            dvals: np.array = np.array(otherThreadResults.doublevalues, dtype=np.float64)
            sname: str = otherThreadResults.stringname
            svals: np.array = np.array(otherThreadResults.stringvalues, dtype=str)
            df = pd.DataFrame({dname: dvals, sname: svals})
            print(df)
        return handshake_pb2.AggregateResponse(return_code=0)
    
    def GetResults(self, request, context):
        response = handshake_pb2.StackedTable(doublename="Rocco",
            doublevalues=[1.1,2.2,3.3],
            stringname="Sameer", 
            stringvalues=["IRBB","VAR","Net"])
        return response


def serve(port:int=50051):
    port = str(port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    handshake_pb2_grpc.add_HandShakeServicer_to_server(HandShakeServer(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    processes = []
    for port in ports:
        process = multiprocessing.Process(target=serve, args=(port,), name="Server1")
        process.start()
        processes.append(process)
    for process in processes:
        process.join()
