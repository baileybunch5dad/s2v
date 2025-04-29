cmake_minimum_required(VERSION 3.10)
project(GrpcColumnTransfer)

set(CMAKE_CXX_STANDARD 17)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

add_executable(
    data_client
    DataServiceClient.cpp
    dataservice.pb.cc
    dataservice.grpc.pb.cc
)

target_link_libraries(
    data_client
    ${Protobuf_LIBRARIES}
    gRPC::grpc++
    gRPC::grpc++_reflection
)