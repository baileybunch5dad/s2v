import grpc
from concurrent import futures
import hister_pb2
import hister_pb2_grpc

class DataServiceServicer(hister_pb2_grpc.DataServiceServicer):
    def SendData(self, request, context):
        print("Received DataContainer:")
        for struct in request.structures:
            print(f"Name: {struct.name}")
            print(f"Int Array: {list(struct.int_array)}")
            print(f"Double Array: {list(struct.double_array)}")
            print("-" * 30)

        return hister_pb2.Response(status="Data received successfully!")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    hister_pb2_grpc.add_DataServiceServicer_to_server(DataServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Server listening on port 50051...")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()