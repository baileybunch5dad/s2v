#include <Python.h>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "handshake.grpc.pb.h"
#include "HandShakeClient.h"
#include "EmbeddedPythonController.h"
#include "ChunkedDataFrame.h"
#include <filesystem>

#if __cplusplus >= 201703L // C++17 and later
#include <string_view>

static bool ends_with(std::string_view str, std::string_view suffix)
{
    return str.size() >= suffix.size() && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

static bool starts_with(std::string_view str, std::string_view prefix)
{
    return str.size() >= prefix.size() && str.compare(0, prefix.size(), prefix) == 0;
}
#endif // C++17

namespace fs = std::filesystem;

// const std::string server="cldlgn01.unx.sas.com";

// ABSL_FLAG(std::string, target, server + ":" + "50051", "Server address");

void RunClient(EmbeddedPythonController *epc, HandShakeClient *client, const std::string &client_name, int clientIdx, int numClients)
{

    // std::string csv_file = "/mnt/e/shared/mv_out.csv";
    // ChunkedDataFrame cdf(csv_file);

    std::string path = "/mnt/e/shared/parquet"; // Specify the directory path
    std::vector<std::string> dirList;
    std::vector<std::string> parquetFiles;

    try
    {
        for (const auto &entry : fs::directory_iterator(path))
        {
            std::string fname = entry.path().string();
            if (ends_with(fname, ".parquet"))
                dirList.push_back(fname);
        }
    }
    catch (const fs::filesystem_error &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        exit(1);
    }

    std::sort(dirList.begin(), dirList.end());
    int curDex = 0;
    for (auto f : dirList)
    {
        if (curDex == clientIdx)
        {
            std::cout << f << std::endl;
            parquetFiles.push_back(f); // everyone gets 1/nth of the data
        }
        curDex++;
        if (curDex == numClients)
        {
            curDex = 0;
        }
    }

    ChunkedDataFrame cdf(parquetFiles);

    epc->AsynchStreamDataToPython(client, clientIdx, numClients, &cdf);
}

void startPipe(EmbeddedPythonController *epc, std::string pyfname, int argc, char **argv, std::vector<int> ports)
{
    epc->startPythonProcessPipe(pyfname, argc, argv, ports);
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

// bool startswith(const std::string &s, const std::string &prfix)
// {
//     return (std::strncmp(s.c_str(), prfix.c_str(), prfix.size()) == 0);
// }
std::string parmval(const std::string &s, const std::string &prfix)
{
    return s.substr(prfix.size() + 1);
}
int main(int argc, char **argv)
{
    EmbeddedPythonController *epc = new EmbeddedPythonController();
    std::string server = "localhost";
    bool inprocessPython = false;
    std::vector<int> ports = {50051, 50052, 50053, 50054};
    std::vector<std::string> externalServers = {};
    bool shutdown = false;

    for (int i = 1; i < argc; i++)
    {
        std::string s = argv[i];
        std::string o = "--server";
        if (starts_with(s, "--server"))
        {
            server = parmval(s, "--server");
        }
        if (starts_with(s, "--embedded"))
        {
            inprocessPython = true;
        }
        if (starts_with(s, "--ports"))
        {
            ports = stringToArrayOfInts(parmval(s, "--ports"));
        }
        if (starts_with(s, "--externalServers"))
        {
            externalServers = stringToVector(parmval(s, "--externalServers"));
        }
        if (starts_with(s, "--shutdown"))
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
        pythrd = std::thread(startPipe, epc, "krm_pyapp.py", argc, argv, ports);
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
        grpc::ChannelArguments channel_args;
        channel_args.SetMaxSendMessageSize(1024 * 1024 * 1024);    // 1GB
        channel_args.SetMaxReceiveMessageSize(1024 * 1024 * 1024); // 1GB
        std::string addr = server_addresses[i];
        std::cout << "Creating channel for " << addr << std::endl;
        auto channel = grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), channel_args);
        epc->WaitForChannelReady(channel);
        HandShakeClient *client = new HandShakeClient(channel);
        clients.emplace_back(client);
    }

    // std::cout << "Wait for socket binding" << std::endl;

    // std::this_thread::sleep_for(std::chrono::milliseconds(2000)); // ms

    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        auto client = clients[i];
        std::string client_name = "Client" + std::to_string(i + 1);
        epc->TestClient(client, client_name);
    }

    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        auto client = clients[i];
        std::string client_name = "Client" + std::to_string(i + 1);
        threads.emplace_back(RunClient, epc, client, client_name, i, int(server_addresses.size()));
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

    // clients[0]->SendDataFrame();

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
