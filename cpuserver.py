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
    # Synchronous method
    def Hello(self, request, context):
        print("starting hello")
        print("ending   hello")
        return handshake_pb2.HelloResponse(reply='fred')

    # Synchronous method called asynchronously
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

if __name__ == "__main__":
    ports = [50051, 50052, 50053, 50054]
    processes = []
    for port in ports:
        process = multiprocessing.Process(target=serve, args=(port,))
        process.start()
        processes.append(process)
    stubs = []

    time.sleep(2)
    for port in ports:
        channel = grpc.insecure_channel(f'localhost:{port}')
        stub = handshake_pb2_grpc.HandShakeStub(channel)
        stubs.append(stub)
    
    print("Hello")
    t = tstart()
    for stub in stubs:
        resp = stub.Hello(handshake_pb2.HelloRequest(name='fred'))
    tend(t)
    
    print("Serially burn cpus")
    t = tstart()
    for stub in stubs:
        resp = stub.GetState(handshake_pb2.EmptyRequest())
    tend(t)
    
    
    print(f"Parallel burn cpus")
    gsfutures = []
    for stub in stubs:
        t = tstart()
        getstate_future = stub.GetState.future(handshake_pb2.EmptyRequest())
        gsfutures.append(getstate_future)
    responses = [gsf.result() for gsf in gsfutures]
    tend(t)
    print("Test completed")
    exit(1)
 