#ifndef EPC_H
#define EPC_H
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
#include "ChunkedDataFrame.h"

class EmbeddedPythonController
{
public:
    void AsynchStreamDataToPython(HandShakeClient *handshakeClient, int clientIdx, int numClients, ChunkedDataFrame *cdf);
    void TestClient(HandShakeClient *client, const std::string &client_name);
    void WaitForChannelReady(const std::shared_ptr<Channel> &channel, int max_attempts = 10, int delay_ms = 500);
    int startPythonProcessPipe(std::string pyfname, int argc, char **argv, std::vector<int> ports);
    int startPythonProcessEmbedded(std::string pyfname, int argc, char **argv, std::vector<int> ports);
};

#endif