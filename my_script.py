from concurrent import futures
import multiprocessing
import os
import grpc
import handshake_pb2
import handshake_pb2_grpc
import sys

class HandShakeServer(handshake_pb2_grpc.HandShakeServicer):
    def Hello(self, request, context):
        instr = str(request.name)
        print(f"Hello from TestMethod, received {instr}  {os.getpid()=} ")
        outstr = instr + " and back "
        return handshake_pb2.HelloResponse(reply=outstr)

def serve(q):
    for i in range(10):
        print(f"subprocess {os.getpid()} i like banaas")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    for i in range(10):
        print(f"subprocess {os.getpid()} i like apples")
    hs = HandShakeServer()
    for i in range(10):
        print(f"subprocess {os.getpid()} i like peaches")
    handshake_pb2_grpc.add_HandShakeServicer_to_server(hs, server)
    for i in range(10):
        print(f"subprocess {os.getpid()} i like grapes")
    newport = server.add_insecure_port('[::]:0')
    for i in range(10):
        print(f"subprocess {os.getpid()} i like blueberries")
    server.start()
    for i in range(10):
        print(f"subprocess {os.getpid()} i like walnuts")
    q.put(newport)
    for i in range(10):
        print(f"subprocess {os.getpid()} i like pineapples")
    # sys.stdout.flush()
    server.wait_for_termination()        
    

if __name__ == "__main__":
    print("main process: prior to mp")
    queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=serve, args=(queue,), name="Server")
    p.start()
    print("main process: after child launched")
    # sys.stdout.flush()
    p.join()
    print("main process: child has completed")
    sys.stdout.flush()
