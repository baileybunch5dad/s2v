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
using handshake::HandShake;
using handshake::ArrayRequest;
using handshake::ArrayResponse;


class HandShakeClient {
 public:
  HandShakeClient(std::shared_ptr<Channel> channel)
      : stub_(HandShake::NewStub(channel)) {}

  
  std::string SendArray(const std::string& dnames, const std::vector<double>& dvals,
  const std::string& snames, const std::vector<std::string>& svals) {
    ArrayRequest request;
    request.set_doublename(dnames);
    for (auto dval : dvals) {
        request.add_doublevalues(static_cast<double>(dval)); // Example: i / 10.0
    }
    request.set_stringname(snames);
    for (auto sval : svals) {
        request.add_stringvalues(sval); // Example: i / 10.0
    }

    // Call the SendArray RPC
    ArrayResponse response;
    ClientContext context;
    Status status = stub_->SendArray(&context, request, &response);

    // Handle the response
    if (status.ok()) {
      return response.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }    
  }

 private:
  std::unique_ptr<HandShake::Stub> stub_;
};


// Function to run a client in a separate thread
void AsynchStreamDataToPython(HandShakeClient& handshakeClient) {
    for(int i=0;i<10;i++) {
            // Generate 5000 random doubles
        std::vector<double> doubles;
        doubles.reserve(5000);
        
        // Use random number generator for doubles between 0.0 and 100.0
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(0.0, 100.0);
        
        std::cout << "Generating 5000 random doubles..." << std::endl;
        for (int i = 0; i < 5000; i++) {
            doubles.push_back(dis(gen));
        }

    std::string dname = "DoubleHeader";
    std::string sname = "StringHeader";
        std::cout << "Generating 5000 strings..." << std::endl;
        std::vector<std::string> strings;
        strings.reserve(5000);
        for(int i=0;i<5000;i++) {
            strings.push_back(std::string("Number ") + std::to_string(i));
        }


    std::string reply = handshakeClient.SendArray(dname,doubles,sname,strings);

    std::cout << "Client returned: " << reply << std::endl;
    }
}

void RunClient(const std::string& server_address, const std::string& client_name) {
    // Create a gRPC client and connect to the given server address
    HandShakeClient client(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    AsynchStreamDataToPython(client);
}

int main() {
    std::cout << "Starting multi-threaded C++ krm_capp talking to multiprocess krm_pyapp" << std::endl;
    // List of server addresses
    std::vector<std::string> server_addresses = {"localhost:50051", "localhost:50052"};
    
    // Vector to hold threads
    std::vector<std::thread> threads;

    // Launch threads for each server
    for (size_t i = 0; i < server_addresses.size(); ++i) {
        threads.emplace_back(RunClient, server_addresses[i], "Client" + std::to_string(i + 1));
    }

    // Join all threads
    for (auto& thread : threads) {
        thread.join();
    }
    std::cout << "Everything worked" << std::endl;
}


// int main() {
//     HandShakeClient py1 = HandShakeClient(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
//     std::thread t1(AsynchStreamDataToPython,std::ref(py1));
//     HandShakeClient py2 = HandShakeClient(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
//     std::thread t2(AsynchStreamDataToPython,std::ref(py2));
//     t1.join();
//     t2.join();
//     return 0;
// }


// int main(int argc, char** argv) {
//   absl::ParseCommandLine(argc, argv);
//   // Instantiate the client. It requires a channel, out of which the actual RPCs
//   // are created. This channel models a connection to an endpoint specified by
//   // the argument "--target=" which is the only expected argument.
//   std::string target_str = absl::GetFlag(FLAGS_target);
//   // We indicate that the channel isn't authenticated (use of
//   // InsecureChannelCredentials()).
//   HandShakeClient handshakeClient(
//       grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

//       // Generate 5000 random doubles
//     std::vector<double> doubles;
//     doubles.reserve(5000);
    
//     // Use random number generator for doubles between 0.0 and 100.0
//     std::random_device rd;
//     std::mt19937 gen(rd());
//     std::uniform_real_distribution<> dis(0.0, 100.0);
    
//     std::cout << "Generating 5000 random doubles..." << std::endl;
//     for (int i = 0; i < 5000; i++) {
//         doubles.push_back(dis(gen));
//     }


//   std::string reply = handshakeClient.SendArray(doubles);
//   std::cout << "Greeter received: " << reply << std::endl;
//   return 0;
// }
