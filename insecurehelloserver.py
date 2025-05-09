import grpc
from concurrent import futures
import handshake_pb2
import handshake_pb2_grpc

class InSecureHelloService(handshake_pb2_grpc.HandShakeServicer):
    def Hello(self, request, context):
        print(f"Received: {request.name}")
        return handshake_pb2.HelloResponse(reply=f"Hello, {request.name}")

def serve():
    # Load SSL credentials
    big_msg_options = [
        ("grpc.max_send_message_length", 10 * 1024 * 1024),  # 10MB
        ("grpc.max_receive_message_length", 1024 * 1024 * 1024)  # 1GB
    ]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5), options=big_msg_options)
    handshake_pb2_grpc.add_HandShakeServicer_to_server(InSecureHelloService(), server)

    # server.add_secure_port("[::]:50051", credentials)
    newport = server.add_insecure_port("[::]:50051")
    server.start()
    print(f"Secure gRPC Server running on port {newport}...")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()