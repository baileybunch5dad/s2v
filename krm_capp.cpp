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

// const std::string server="cldlgn01.unx.sas.com";

// ABSL_FLAG(std::string, target, server + ":" + "50051", "Server address");

void RunClient(EmbeddedPythonController *epc, HandShakeClient *client, const std::string &client_name, int clientIdx, int numClients)
{
    std::string csv_file = "/mnt/e/shared/mv_out.csv";
    ChunkedDataFrame cdf(csv_file);

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
        if (startswith(s, "--server"))
        {
            server = parmval(s, "--server");
        }
        if (startswith(s, "--embedded"))
        {
            inprocessPython = true;
        }
        if (startswith(s, "--ports"))
        {
            ports = stringToArrayOfInts(parmval(s, "--ports"));
        }
        if (startswith(s, "--externalServers"))
        {
            externalServers = stringToVector(parmval(s, "--externalServers"));
        }
        if (startswith(s, "--shutdown"))
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
        std::string addr = server_addresses[i];
        std::cout << "Creating channel for " << addr << std::endl;
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
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
