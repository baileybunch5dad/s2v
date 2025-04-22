#include <Python.h>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "handshake.grpc.pb.h"

// const std::string server="cldlgn01.unx.sas.com";

// ABSL_FLAG(std::string, target, server + ":" + "50051", "Server address");

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using handshake::AggregateRequest;
using handshake::AggregateResponse;
using handshake::ArrayResponse;
using handshake::HandShake;
using handshake::ShutdownRequest;
using handshake::ShutdownResponse;
using handshake::StackedTable;
using handshake::HelloRequest;
using handshake::HelloResponse;

class HandShakeClient
{
public:
    HandShakeClient(std::shared_ptr<Channel> channel)
        : stub_(HandShake::NewStub(channel)) {}

    std::string Shutdown()
    {
        ShutdownRequest request;
        ShutdownResponse response;
        grpc::ClientContext context;

        grpc::Status status = stub_->Shutdown(&context, request, &response);

        if (status.ok())
        {
            return response.status();
        }
        else
        {
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return "RPC failed";
        }
    }

    std::string Hello(std::string instr)
    {
        HelloRequest request;
        HelloResponse response;
        grpc::ClientContext context;

        // sleep(2); // for first message give server a chance to launch threads and processes
        // context.set_wait_for_ready(true);

        // // Optionally, set a deadline to avoid waiting indefinitely
        // std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(5); 
        // context.set_deadline(deadline);
        // request.set_message(instr);
        std::cout << "invoke Hello(" << instr << ")" << std::endl;
        request.set_name(instr.c_str());
        grpc::Status status = stub_->Hello(&context, request, &response);
        std::cout << "Status ok " << status.ok() << std::endl;
        if (status.ok())
        {
            return response.reply();
        }
        else
        {
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return "RPC failed";
        }
    }

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

        // Call the Aggregate RPC
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
    for (int i = 0; i < 1; i++)
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

void TestClient(HandShakeClient* client, const std::string& client_name) {
    std::string tmstr = client->Hello(client_name);
    if(std::strncmp(tmstr.c_str(), "RPC failed", 10) == 0) {
        std::cerr << "Aborting, cannot invoke remote procedure calls" << std::endl;
        exit(1);
    }
}

// void RunClient(const std::string& server_address, const std::string& client_name) {
void RunClient(HandShakeClient *client, const std::string &client_name)
{

    // Create a gRPC client and connect to the given server address
    // HandShakeClient client(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    AsynchStreamDataToPython(client);
}

int startPythonProcess(std::string pyfname)
{
    Py_Initialize();
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.append('.')");
    FILE *py_file = fopen(pyfname.c_str(), "r");
    if (!py_file)
    {
        std::cerr << "Error: Could not open Python script " << pyfname << std::endl;
        return 1;
    }
    std::cout << "C++ Launching embedded Python" << std::endl;
    PyRun_SimpleFile(py_file, pyfname.c_str());
    Py_Finalize();
    return 0;
}

void WaitForChannelReady(const std::shared_ptr<Channel>& channel, int max_attempts = 10, int delay_ms = 500) {
    for (int i = 0; i < max_attempts; ++i) {
        // Get the current channel state
        auto state = channel->GetState(true);

        // Check if the channel is ready
        if (state == GRPC_CHANNEL_READY) {
            std::cout << "Channel is ready!" << std::endl;
            return;
        } else {
            std::cout << "Waiting for channel to be ready. Current state: " << state << std::endl;
        }

        // Sleep for the specified delay
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }

    std::cerr << "Channel did not become ready within the timeout period." << std::endl;
}


int main(int argc, char **argv)
{
    std::string server = "localhost";
    bool inprocessPython = false;

    for (int i = 1; i < argc; i++)
    {
        std::string s = argv[i];
        if (std::strncmp(s.c_str(), "--server", 8) == 0)
        {
            server = s.substr(9);
        }
        if (std::strncmp(s.c_str(), "--embedded", 10) == 0)
        {
            inprocessPython = true;
        }
    }
    std::cout << "Server " << server << std::endl;
    std::thread pythrd = {};
    if (inprocessPython)
    {
        std::cout << "Launching inprocess Python";
        pythrd = std::thread(startPythonProcess, "krm_pyapp.py");
        sleep(1);
    }
    else
    {
        std::cout << "Attaching to remote out of process aggregation server " << std::endl;
    }

    std::cout << "Starting multi-threaded C++ krm_capp talking to multiprocess krm_pyapp" << std::endl;
    // List of server addresses
    std::vector<int> ports = {50051, 50052, 50053, 50054};
    std::vector<std::string> server_addresses;
    for (auto p : ports)
        server_addresses.emplace_back(server + ":" + std::to_string(p));
    std::vector<HandShakeClient *> clients;
    // Vector to hold threads
    std::vector<std::thread> threads;

    // Launch threads for each server
    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        std::string addr = server_addresses[i];
        std::cout << "Creating channel for " << addr << std::endl;
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        WaitForChannelReady(channel);
        HandShakeClient *client = new HandShakeClient(channel);
        clients.emplace_back(client);
    }

    // std::cout << "Wait for socket binding" << std::endl; 

    // std::this_thread::sleep_for(std::chrono::milliseconds(2000)); // ms
    
    for (size_t i = 0; i < server_addresses.size(); ++i) {
        auto client = clients[i];
        std::string client_name = "Client" + std::to_string(i + 1);
        TestClient(client, client_name);
    }


    for (size_t i = 0; i < server_addresses.size(); ++i) {
        auto client = clients[i];
        std::string client_name = "Client" + std::to_string(i + 1);
        threads.emplace_back(RunClient, client, client_name);
    }



    std::cout << "Everything worked multi-threaded, now aggregate with threads" << std::endl;
    clients[0]->Aggregate(ports);
    std::cout << "Thread aggregation finished" << std::endl;

    // for(auto client : clients) {
    //     client->Shutdown();
    // }

    // Join all threads
    for (auto &thread : threads)
    {
        if(thread.joinable())
            thread.join();
    }

    if(pythrd.joinable()) {
        pythrd.join();
    }

    exit(0);

    return 0;
}
