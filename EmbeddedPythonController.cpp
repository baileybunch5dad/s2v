#include <Python.h>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <sstream>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "handshake.grpc.pb.h"
#include "HandShakeClient.h"
#include "EmbeddedPythonController.h"

// const std::string server="cldlgn01.unx.sas.com";

// ABSL_FLAG(std::string, target, server + ":" + "50051", "Server address");

// Function to run a client in a separate thread
void EmbeddedPythonController::AsynchStreamDataToPython(HandShakeClient *handshakeClient, int clientIdx, 
int numClients, ChunkedDataFrame *cdf)
{
    std::thread::id threadId = std::this_thread::get_id();
    std::stringstream ss;
    ss << threadId;
    std::string threadIdStr = ss.str();
    auto start_time = std::chrono::high_resolution_clock::now();
    int clientNum = 0;
    int chunkSize = 5000;
    long totRows = 28229622;
    long totRowsRead = 0;
    for (int rowsRead = cdf->readChunk(chunkSize); rowsRead > 0; rowsRead = cdf->readChunk(chunkSize))
    {
        if (clientNum == clientIdx)
        {
            totRowsRead += rowsRead;

            handshakeClient->ProcessData(cdf->columnData);

            // std::cout << "Sending chunk " << clientNum << std::endl;
            auto cur_time = std::chrono::high_resolution_clock::now();
            double percentComplete = 100.*(totRowsRead)/((totRows+0.0)/numClients);
            std::chrono::duration<double> elapsed_seconds = cur_time - start_time;
            std::cout << "Thread " << threadIdStr << " processed " << totRowsRead << " rows, percent complete " << percentComplete << "% Elapsed " << elapsed_seconds.count() << " seconds." << std::endl;

            break;
        }
        else
        {
            // std::cout << "Skipping chunk " << clientNum << std::endl;
        }
        clientNum++;
        if (clientNum == numClients)
        {
            clientNum = 0;
        }
        // auto cur_time = std::chrono::high_resolution_clock::now();
        // totRowsRead += rowsRead;
        // double percentComplete = 100.*(totRowsRead)/totRows;
        // std::chrono::duration<double> elapsed_seconds = cur_time - start_time;
        // std::cout << "Thread " << threadIdStr << " Percent complete " << percentComplete << "% Elapsed " << elapsed_seconds.count() << " seconds." << std::endl;
    }

    // for (int i = 0; i < 1; i++)
    // {
    //     // Generate 5000 random doubles
    //     std::vector<double> doubles;
    //     doubles.reserve(5000);

    //     // Use random number generator for doubles between 0.0 and 100.0
    //     std::random_device rd;
    //     std::mt19937 gen(rd());
    //     std::uniform_real_distribution<> dis(0.0, 100.0);

    //     std::cout << "Generating 5000 random doubles..." << std::endl;
    //     for (int i = 0; i < 5000; i++)
    //     {
    //         double dd = dis(gen);
    //         // if(i < 2000)
    //         //     dd = std::floor(dd);
    //         doubles.push_back(dd);
    //     }

    //     std::string dname = "mortage loan payment interest gain";
    //     std::string sname = "txn_id scenario product_id load_id run_id";
    //     std::cout << "Generating 5000 strings..." << std::endl;
    //     std::vector<std::string> strings;
    //     std::string choices[] = {"Broccoli", "Tomato", "Kiwi", "Kale", "Tomatillo"};
    //     strings.reserve(5000);
    //     for (int i = 0; i < 5000; i++)
    //     {
    //         strings.push_back(choices[std::rand() % 5]);
    //     }

    //     std::string reply = handshakeClient->SendArray(dname, doubles, sname, strings);

    //     std::cout << "Client returned: " << reply << std::endl;
    // }
}

void EmbeddedPythonController::TestClient(HandShakeClient *client, const std::string &client_name)
{
    std::string tmstr = client->Hello(client_name);
    if (std::strncmp(tmstr.c_str(), "RPC failed", 10) == 0)
    {
        std::cerr << "Aborting, cannot invoke remote procedure calls" << std::endl;
        exit(1);
    }
}

void EmbeddedPythonController::WaitForChannelReady(const std::shared_ptr<Channel> &channel, int max_attempts, int delay_ms)
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

int EmbeddedPythonController::startPythonProcessPipe(std::string pyfname, int argc, char **argv, std::vector<int> ports)
{
    // pass the arguments
    std::ostringstream command;
    command << "python3 " << pyfname << " ";
    command << argv[1]; // Program to run
    for (int i = 1; i < argc; ++i)
    {
        command << " " << argv[i]; // Append arguments
    }

    std::string cmdstr = command.str();
    FILE *pipe = popen(cmdstr.c_str(), "r");
    if (!pipe)
    {
        std::cerr << "Failed to start process\n";
        exit(1);
    }
    std::cout << cmdstr << std::endl;

    char buffer[128];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr)
    {
        std::cout << buffer; // Echo output as it runs
    }

    pclose(pipe);
    return 0;
}
int EmbeddedPythonController::startPythonProcessEmbedded(std::string pyfname, int argc, char **argv, std::vector<int> ports)
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
