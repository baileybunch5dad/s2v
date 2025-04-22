import grpc
from concurrent import futures
import handshake_pb2
import handshake_pb2_grpc

class HandShake(handshake_pb2_grpc.HandShakeServicer):
    def Hello(self, request, context):
        return handshake_pb2.HelloResponse(message=f"Hello, {request.name}!")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    handshake_pb2_grpc.add_HandShakeServicer_to_server(HandShake(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Server is running on port 50051")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()