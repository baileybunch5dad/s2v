#ifndef HANDSHAKECLIENT_H
#define HANDSHAKECLIENT_H
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "handshake.grpc.pb.h"
#include "columnvariant.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using handshake::AggregateGlobalRequest;
using handshake::AggregateGlobalResponse;
using handshake::AggregateLocalRequest;
using handshake::AggregateLocalResponse;
using handshake::ArrayResponse;
using handshake::Column;
using handshake::HandShake;
using handshake::HelloRequest;
using handshake::HelloResponse;
using handshake::ShutdownRequest;
using handshake::ShutdownResponse;
using handshake::StackedTable;
using handshake::StringResponse;
// using handshake::DataFrame;
using handshake::Column;
using handshake::Table;
using handshake::StringArray;
using handshake::DoubleArray;
using handshake::LongArray;
// using handshake::Table;
// using handshake::Column;
// using handshake::Value;



class HandShakeClient
{
public:
    HandShakeClient(std::shared_ptr<Channel> channel);
    std::string Shutdown();
    // std::string SendDataFrame(std::vector<std::string> &columnNames,
    //                           std::vector<char> &columnTypes,
    //                           std::vector<std::variant<std::vector<std::string> *, std::vector<double> *, std::vector<int64_t> *>> &columnData);
    std::string Hello(std::string instr);
    std::string ProcessData(const NamedColumns &columnsData);
    std::string SendArray(const std::string &dnames, const std::vector<double> &dvals,
                          const std::string &snames, const std::vector<std::string> &svals);
    int AggregateLocal(const std::vector<int> &ports);
    int AggregateGlobal(const std::vector<int> &ports, const std::vector<std::string> &servers);

private:
    std::unique_ptr<HandShake::Stub> stub_;
};

#endif