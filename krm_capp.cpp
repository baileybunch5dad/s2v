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
using handshake::AggregateGlobalRequest;
using handshake::AggregateGlobalResponse;
using handshake::AggregateLocalRequest;
using handshake::AggregateLocalResponse;
using handshake::ArrayResponse;
using handshake::HandShake;
using handshake::HelloRequest;
using handshake::HelloResponse;
using handshake::ShutdownRequest;
using handshake::ShutdownResponse;
using handshake::StackedTable;
using handshake::DataFrame;
using handshake::StringResponse;
using handshake::Column;

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
            std::cout << "Shutdown" << std::endl;
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return "RPC failed";
        }
    }

    std::string SendDataFrame() 
    {
        handshake::DataFrame request;
        StringResponse response;
        grpc::ClientContext context;

        auto *col1 = request.add_columns();
        col1->set_name("SomeStrings");
        auto svalptr = col1->mutable_string_values();
        svalptr->add_values("hello");
        svalptr->add_values("there");
        auto *col2 = request.add_columns();
        col2->set_name("NiceNumbers");
        auto dvalptr = col2->mutable_double_values();
        dvalptr->add_values(3.1415);
        dvalptr->add_values(2.7182818);
        auto *col3 = request.add_columns();
        col3->set_name("AndDatetimeValues");
        auto dtvalptr = col3->mutable_datetime_values();
        auto now = std::chrono::system_clock::now();
        auto seconds_since_epoch = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        dtvalptr->add_values(seconds_since_epoch);
        dtvalptr->add_values(seconds_since_epoch+100);


        grpc::Status status = stub_->SendDataFrame(&context, request, &response);

        if (status.ok())
        {
            return response.str();
        }
        else
        {
            std::cout << "SendDataFrame" << std::endl;
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
            std::cout << "Hello failure" << std::endl;
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
            std::cout << "SendArray failure" << std::endl;
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return "RPC failed";
        }
    }

    int AggregateLocal(const std::vector<int> &ports)
    {
        AggregateLocalRequest request;
        for (auto p : ports)
        {
            request.add_ports(p);
        }

        // Call the Aggregate RPC
        AggregateLocalResponse response;
        ClientContext context;
        Status status = stub_->AggregateLocal(&context, request, &response);

        // Handle the response
        if (status.ok())
        {
            return response.return_code();
        }
        else
        {
            std::cout << "AggregateLocal failure" << std::endl;
            std::cout << status.error_code() << ": " << status.error_message()
                      << std::endl;
            return (-1);
        }
    }

    int AggregateGlobal(const std::vector<int> &ports, const std::vector<std::string> &servers)
    {
        AggregateGlobalRequest request;
        for (int p : ports)
        {
            request.add_ports(p);
        }
        for (std::string s : servers)
        {
            request.add_servers(s);
        }

        // Call the Aggregate RPC
        AggregateGlobalResponse response;
        ClientContext context;
        Status status = stub_->AggregateGlobal(&context, request, &response);

        // Handle the response
        if (status.ok())
        {
            return response.return_code();
        }
        else
        {
            std::cout << "AggregateGlobal failure" << std::endl;
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
        std::string choices[] = {"Broccoli", "Tomato", "Kiwi", "Kale", "Tomatillo"};
        strings.reserve(5000);
        for (int i = 0; i < 5000; i++)
        {
            strings.push_back(choices[std::rand() % 5]);
        }

        std::string reply = handshakeClient->SendArray(dname, doubles, sname, strings);

        std::cout << "Client returned: " << reply << std::endl;
    }
}

void TestClient(HandShakeClient *client, const std::string &client_name)
{
    std::string tmstr = client->Hello(client_name);
    if (std::strncmp(tmstr.c_str(), "RPC failed", 10) == 0)
    {
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

void WaitForChannelReady(const std::shared_ptr<Channel> &channel, int max_attempts = 10, int delay_ms = 500)
{
    for (int i = 0; i < max_attempts; ++i)
    {
        // Get the current channel state
        auto state = channel->GetState(true);

        // Check if the channel is ready
        if (state == GRPC_CHANNEL_READY)
        {
            std::cout << "Channel is ready!" << std::endl;
            return;
        }
        else
        {
            std::cout << "Waiting for channel to be ready. Current state: " << state << std::endl;
        }

        // Sleep for the specified delay
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }

    std::cerr << "Channel did not become ready within the timeout period." << std::endl;
}

std::vector<int> stringToArrayOfInts(const std::string &inputString)
{
    std::vector<int> intArray;
    std::stringstream ss(inputString);
    std::string token;

    while (std::getline(ss, token, ','))
    {
        token.erase(std::remove_if(token.begin(), token.end(), ::isspace), token.end());
        try
        {
            intArray.push_back(std::stoi(token));
        }
        catch (const std::invalid_argument &ia)
        {
            std::cerr << "Invalid argument: " << ia.what() << " for token: " << token << std::endl;
        }
        catch (const std::out_of_range &oor)
        {
            std::cerr << "Out of range error: " << oor.what() << " for token: " << token << std::endl;
        }
    }
    return intArray;
}

std::string to_comma_separated_string(const std::vector<int> &arr)
{
    if (arr.empty())
    {
        return "";
    }

    std::stringstream ss;
    std::for_each(arr.begin(), arr.end() - 1, [&](int x)
                  { ss << x << ","; });
    ss << arr.back();
    return ss.str();
}

std::vector<std::string> stringToVector(const std::string &str)
{
    std::vector<std::string> result;
    std::stringstream ss(str);
    std::string token;

    while (std::getline(ss, token, ','))
    {
        result.push_back(token);
    }
    return result;
}

int startPythonProcessPipe(std::string pyfname, int argc, char **argv, std::vector<int> ports)
{
    // pass the arguments
    std::ostringstream command;
    command << "python3 " << pyfname << " ";
    command << argv[1];  // Program to run
    for (int i = 1; i < argc; ++i) {
        command << " " << argv[i];  // Append arguments
    }

    std::string cmdstr = command.str();
    FILE* pipe = popen(cmdstr.c_str(), "r");
    if (!pipe) {
        std::cerr << "Failed to start process\n";
        exit(1);
    }
    std::cout << cmdstr << std::endl;

    char buffer[128];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        std::cout << buffer;  // Echo output as it runs
    }

    pclose(pipe);
}
int startPythonProcessEmbedded(std::string pyfname, int argc, char **argv, std::vector<int> ports)
{
    Py_Initialize();

    // Prepare arguments for Python
    std::vector<wchar_t *> wargv(argc);
    for (int i = 0; i < argc; ++i)
    {
        size_t size = mbstowcs(nullptr, argv[i], 0) + 1;
        wargv[i] = new wchar_t[size];
        mbstowcs(wargv[i], argv[i], size);
    }

    // Set sys.argv
    PySys_SetArgv(argc, wargv.data());
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.append('.')");

    FILE *py_file = fopen(pyfname.c_str(), "r");
    if (!py_file)
    {
        std::cerr << "Error: Could not open Python script " << pyfname << std::endl;
        exit(1);
    }
    std::cout << "C++ Launching embedded Python" << std::endl;
    PyRun_SimpleFile(py_file, pyfname.c_str());
    Py_Finalize();
    return 0;
}

bool startswith(const std::string &s, const std::string &prfix)
{
    return (std::strncmp(s.c_str(), prfix.c_str(), prfix.size()) == 0);
}
std::string parmval(const std::string &s, const std::string &prfix)
{
    return s.substr(prfix.size() + 1);
}
int main(int argc, char **argv)
{
    std::string server = "localhost";
    bool inprocessPython = false;
    std::vector<int> ports = {50051, 50052, 50053, 50054};
    std::vector<std::string> externalServers = {};
    bool shutdown = false;

    for (int i = 1; i < argc; i++)
    {
        std::string s = argv[i];
        std::string o = "--server";
        if (startswith(s,"--server") )
        {
            server = parmval(s,"--server");
        }
        if (startswith(s,"--embedded"))
        {
            inprocessPython = true;
        }
        if (startswith(s,"--ports")) 
        {
            ports = stringToArrayOfInts(parmval(s, "--ports"));
        }
        if (startswith(s, "--externalServers"))
        {
            externalServers = stringToVector(parmval(s, "--externalServers"));
        }
        if (startswith(s,"--shutdown"))
        {
            shutdown = true;
        }
    }
    std::cout << "Ports ";
    for (auto p : ports)
    {
        std::cout << p << " ";
    }
    std::cout << std::endl;
    std::cout << "Server " << server << std::endl;
    std::thread pythrd = {};
    if (inprocessPython)
    {
        std::cout << "Launching inprocess Python";
        pythrd = std::thread(startPythonProcessPipe, "krm_pyapp.py", argc, argv, ports);
        sleep(1);
    }
    else
    {
        std::cout << "Attaching to remote out of process aggregation server " << std::endl;
    }

    std::cout << "Starting multi-threaded C++ krm_capp talking to multiprocess krm_pyapp" << std::endl;
    // List of server addresses
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

    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        auto client = clients[i];
        std::string client_name = "Client" + std::to_string(i + 1);
        TestClient(client, client_name);
    }

    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        auto client = clients[i];
        std::string client_name = "Client" + std::to_string(i + 1);
        threads.emplace_back(RunClient, client, client_name);
    }

    // Join all threads, letting the asynch threads flood the python listeners
    for (auto &thread : threads)
    {
        if (thread.joinable())
            thread.join();
    }
    std::cout << "Local aggregation within threads commencing" << std::endl;
    clients[0]->AggregateLocal(ports);
    std::cout << "Global aggregation across servers commencing" << std::endl;
    clients[0]->AggregateGlobal(ports, externalServers);
    std::cout << "Aggregation finished" << std::endl;

    clients[0]->SendDataFrame();

    if (shutdown)
    {
        for (auto client : clients)
        {
            client->Shutdown();
        }
    }

    if (pythrd.joinable())
    {
        pythrd.join();
    }

    exit(0);

    return 0;
}
