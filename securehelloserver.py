import grpc
from concurrent import futures
import handshake_pb2
import handshake_pb2_grpc

class SecureHelloService(handshake_pb2_grpc.HandShakeServicer):
    def Hello(self, request, context):
        print(f"Received: {request.name}")
        return handshake_pb2.HelloResponse(reply=f"Hello, {request.name}")

def serve():
    # Load SSL credentials
    with open("server.key", "rb") as f:
        private_key = f.read()
    with open("server.crt", "rb") as f:
        certificate = f.read()

    credentials = grpc.ssl_server_credentials([(private_key, certificate)])
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    handshake_pb2_grpc.add_HandShakeServicer_to_server(SecureHelloService(), server)

    server.add_secure_port("[::]:50051", credentials)
    server.start()
    print("Secure gRPC Server running on port 50051...")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()