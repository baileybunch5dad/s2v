#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "handshake.grpc.pb.h"

ABSL_FLAG(std::string, target, "localhost:50051", "Server address");

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using handshake::StackedTable;
using handshake::ArrayResponse;
using handshake::AggregateRequest;
using handshake::AggregateResponse;
using handshake::HandShake;

class HandShakeClient
{
public:
    HandShakeClient(std::shared_ptr<Channel> channel)
        : stub_(HandShake::NewStub(channel)) {}

    std::string SendArray(const std::string &dnames, const std::vector<double> &dvals,
                          const std::string &snames, const std::vector<std::string> &svals)
    {
        StackedTable request;
        request.set_doublename(dnames);
        for (auto dval : dvals)
        {
            request.add_doublevalues(static_cast<double>(dval)); // Example: i / 10.0
        }
        request.set_stringname(snames);
        for (auto sval : svals)
        {
            request.add_stringvalues(sval); // Example: i / 10.0
        }

        // Call the SendArray RPC
        ArrayResponse response;
        ClientContext context;
        Status status = stub_->SendTable(&context, request, &response);

        // Handle the response
        if (status.ok())
        {
            return response.message();
        }
        else
        {
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return "RPC failed";
            
        }
    }

    int Aggregate(const std::vector<int> &ports)
    {
        AggregateRequest request;
        for (auto p : ports)
        {
            request.add_ports(p);
        }

        //Call the Aggregate RPC
        AggregateResponse response;
        ClientContext context;
        Status status = stub_->Aggregate(&context, request, &response);

        // Handle the response
        if (status.ok())
        {
            return response.return_code();
        }
        else
        {
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return (-1);
        }
    }

private:
    std::unique_ptr<HandShake::Stub> stub_;
};

// Function to run a client in a separate thread
void AsynchStreamDataToPython(HandShakeClient *handshakeClient)
{
    for (int i = 0; i < 10; i++)
    {
        // Generate 5000 random doubles
        std::vector<double> doubles;
        doubles.reserve(5000);

        // Use random number generator for doubles between 0.0 and 100.0
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(0.0, 100.0);

        std::cout << "Generating 5000 random doubles..." << std::endl;
        for (int i = 0; i < 5000; i++)
        {
            double dd = dis(gen);
            // if(i < 2000)
            //     dd = std::floor(dd);
            doubles.push_back(dd);
        }

        std::string dname = "mortage loan payment interest gain";
        std::string sname = "txn_id scenario product_id load_id run_id";
        std::cout << "Generating 5000 strings..." << std::endl;
        std::vector<std::string> strings;
        strings.reserve(5000);
        for (int i = 0; i < 5000; i++)
        {
            strings.push_back(std::string("Number ") + std::to_string(i));
        }

        std::string reply = handshakeClient->SendArray(dname, doubles, sname, strings);

        std::cout << "Client returned: " << reply << std::endl;
    }
}

// void RunClient(const std::string& server_address, const std::string& client_name) {
void RunClient(HandShakeClient *client, const std::string &client_name)
{
    // Create a gRPC client and connect to the given server address
    // HandShakeClient client(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    AsynchStreamDataToPython(client);
}

int main()
{
    std::cout << "Starting multi-threaded C++ krm_capp talking to multiprocess krm_pyapp" << std::endl;
    // List of server addresses
    std::vector<int> ports = { 50051, 50052, 50053, 50054};
    std::vector<std::string> server_addresses;
    for(auto p : ports)
        server_addresses.emplace_back("localhost:" + std::to_string(p));
    std::vector<HandShakeClient *> clients;
    // Vector to hold threads
    std::vector<std::thread> threads;

    // Launch threads for each server
    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        HandShakeClient *client = new HandShakeClient(grpc::CreateChannel(server_addresses[i], grpc::InsecureChannelCredentials()));
        clients.emplace_back(client);
        threads.emplace_back(RunClient, client, "Client" + std::to_string(i + 1));
    }

    // Join all threads
    for (auto &thread : threads)
    {
        thread.join();
    }
    std::cout << "Everything worked multi-threaded, now aggregate with threads" << std::endl;
    clients[0]->Aggregate(ports);
    std::cout << "Thread aggregation finished" << std::endl;

    return 0;
}

