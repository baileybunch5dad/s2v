import grpc
import asyncio
import multiprocessing
import os
import grpc
import asyncio
from concurrent import futures
import handshake_pb2
import handshake_pb2_grpc
import numpy as np
import time

class MyServiceServicer(handshake_pb2_grpc.HandShakeServicer):
    # Synchronous method called both synchronously and asynchronously
    def Hello(self, request, context):
        print("grpc recursion")
        stubs = getStubs()
        print("serial")
        responses = [stub.GetState(handshake_pb2.EmptyRequest()) for stub in stubs]
        print("parallel")
        gsfutures = [stub.GetState.future(handshake_pb2.EmptyRequest()) for stub in stubs]
        responses = [gsf.result() for gsf in gsfutures]
        print("ending grp recursion")
        return handshake_pb2.StateMessage(workerstatus=handshake_pb2.BUILDING_GLOBAL_AGGREGATIONS)
    
    # Synchronous method called both synchronously and asynchronously
    def GetState(self, request, context):
        print(f"starting getstate (burning cpu on operating system process {os.getpid()})")
        size = 2 * 10**4
        loopcount = 10**3
        for i in range(loopcount):
            arr = np.random.rand(size)
            for a in arr:
                b = np.sqrt(a)
        print("ending   getstate")
        return handshake_pb2.StateMessage(workerstatus=handshake_pb2.BUILDING_GLOBAL_AGGREGATIONS)

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    ms = MyServiceServicer()
    handshake_pb2_grpc.add_HandShakeServicer_to_server(ms, server)
    addr = f"[::]:{port}"
    server.add_insecure_port(addr)
    server.start()
    print(f"Server running {addr}")
    server.wait_for_termination()

def tstart():
    return time.perf_counter()

def tend(t1):
    elapsed = time.perf_counter()-t1
    print(f"Elapsed time {elapsed:.2f}")

def getPorts():
    return [50051, 50052, 50053, 50054]

def getStubs():
    return [handshake_pb2_grpc.HandShakeStub(grpc.insecure_channel(f'localhost:{port}')) for port in getPorts()]
        

if __name__ == "__main__":
    processes = [multiprocessing.Process(target=serve, args=(port,)) for port in getPorts()]
    for process in processes:
        process.start()

    time.sleep(2)
    stubs = getStubs()
    
    print("Serially burn cpus")
    t = tstart()
    responses = [stub.GetState(handshake_pb2.EmptyRequest()) for stub in stubs] 
    tend(t)
    
    
    print(f"Parallel burn cpus")
    t = tstart()
    gsfutures = [stub.GetState.future(handshake_pb2.EmptyRequest()) for stub in stubs]
    responses = [gsf.result() for gsf in gsfutures]
    tend(t)
    
    print(f"Now from within a method call recursively")
    stubs[0].Hello(handshake_pb2.HelloRequest(name='test'))
    print("Test completed")
    exit(1)
 