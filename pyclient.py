import grpc
import handshake_pb2
import handshake_pb2_grpc


def run():
    channel = grpc.insecure_channel("localhost:50051")
    stub = handshake_pb2_grpc.HandShakeStub(channel)

    # Send request
    request = handshake_pb2.HelloRequest(name="Chris")
    response = stub.Hello(request)

    # Print the response
    print("Server replied:", response.message)

if __name__ == "__main__":
    run()    