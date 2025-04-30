import grpc
import hister_pb2
import hister_pb2_grpc
import numpy

def send_data():
    channel = grpc.insecure_channel("localhost:50051")
    stub = hister_pb2_grpc.DataServiceStub(channel)

    # Create request message with multiple DataStruct entries
    v = []
    for i in range(4):
        int_array_np = numpy.array(range(i+1,i+4))
        double_array_np = numpy.linspace(i+1.1, i+4.1, num=3)
        name = "Histogram#" + str(i)
        d = hister_pb2.DataStruct(name=name, int_array=int_array_np, double_array=double_array_np)
        v.append(d)

    request = hister_pb2.DataContainer(structures=v)
    # request = hister_pb2.DataContainer(structures=[
    #     # hister_pb2.DataStruct(name="Set1", int_array=[1, 2, 3], double_array=[1.1, 2.2, 3.3]),
    #     hister_pb2.DataStruct(name="Set1", int_array=int_array_np, double_array=[1.1, 2.2, 3.3]),
    #     hister_pb2.DataStruct(name="Set2", int_array=[4, 5, 6], double_array=[4.4, 5.5, 6.6]),
    #     hister_pb2.DataStruct(name="Set3", int_array=[7, 8, 9], double_array=[7.7, 8.8, 9.9])
    # ])

    response = stub.SendData(request)
    print("Server response:", response.status)

if __name__ == "__main__":
    send_data()