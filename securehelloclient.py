import grpc
import handshake_pb2
import handshake_pb2_grpc

def send_secure_hello():
    with open("server.crt", "rb") as f:
        trusted_certs = f.read()

    credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)
    channel = grpc.secure_channel("localhost:50051", credentials)

    stub = handshake_pb2_grpc.HandShakeStub(channel)
    request = handshake_pb2.HelloRequest(name="Secure gRPC Test")
    response = stub.Hello(request)

    print("Server Response:", response.reply)

if __name__ == "__main__":
    send_secure_hello()