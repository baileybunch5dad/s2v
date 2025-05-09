
#include <iostream>
#include <fstream>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "handshake.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using handshake::HandShake;
using handshake::HelloRequest;
using handshake::HelloResponse;

class HelloClient {
public:
    HelloClient(std::shared_ptr<grpc::Channel> channel, const std::string& name)
        : stub_(HandShake::NewStub(channel)), server_name(name) {}

    void SayHello(const std::string& message) {
        HelloRequest request;
        request.set_name(message);

        HelloResponse response;
        ClientContext context;

        Status status = stub_->Hello(&context, request, &response);

        if (status.ok()) {
            std::cout << server_name << " Response: " << response.reply() << std::endl;
        } else {
            std::cerr << server_name << " RPC failed." << std::endl;
        }
    }

private:
    std::unique_ptr<HandShake::Stub> stub_;
    std::string server_name;
};



void WaitForChannelReady(const std::shared_ptr<grpc::Channel> &channel, int max_attempts = 10, int delay_ms = 500)
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
    exit(1);
}

std::shared_ptr<HelloClient> createSecureClient(std::string addr) {
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxSendMessageSize(1024 * 1024 * 1024);    // 1GB
    channel_args.SetMaxReceiveMessageSize(1024 * 1024 * 1024); // 1GB
       // Load the server certificate for the secure connection
    std::string server_cert;
    std::ifstream cert_file("server.crt");
    server_cert.assign(std::istreambuf_iterator<char>(cert_file), std::istreambuf_iterator<char>());
    grpc::SslCredentialsOptions ssl_opts;
    ssl_opts.pem_root_certs = server_cert;

    std::shared_ptr<grpc::ChannelCredentials> credentials = grpc::SslCredentials(ssl_opts);
    std::shared_ptr<grpc::Channel> secure_channel = grpc::CreateCustomChannel(addr, credentials, channel_args);
    WaitForChannelReady(secure_channel);
    std::shared_ptr<HelloClient> hcp = std::make_shared<HelloClient>(secure_channel, "Secure Server");
    return hcp;
}

std::shared_ptr<HelloClient> createInSecureClient(std::string addr) {
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxSendMessageSize(1024 * 1024 * 1024);    // 1GB
    channel_args.SetMaxReceiveMessageSize(1024 * 1024 * 1024); // 1GB
    std::shared_ptr<grpc::ChannelCredentials> credentials = grpc::InsecureChannelCredentials()  ;
    std::shared_ptr<grpc::Channel> insecure_channel = grpc::CreateCustomChannel(addr, credentials, channel_args);
    WaitForChannelReady(insecure_channel);
    std::shared_ptr<HelloClient> hcp = std::make_shared<HelloClient>(insecure_channel, "Insecure Server");
    return hcp;
}

int main() {
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxSendMessageSize(1024 * 1024 * 1024);    // 1GB
    channel_args.SetMaxReceiveMessageSize(1024 * 1024 * 1024); // 1GB

    // auto insecure_channel = grpc::CreateCustomChannel("localhost:34797", grpc::InsecureChannelCredentials(), channel_args);
    // HelloClient insecure_client(insecure_channel, "Insecure Server");
    std::shared_ptr<HelloClient> insecure_client = createInSecureClient("localhost:34797");

    // Load the server certificate for the secure connection
    // std::string server_cert;
    // std::ifstream cert_file("server.crt");
    // server_cert.assign(std::istreambuf_iterator<char>(cert_file), std::istreambuf_iterator<char>());

    // grpc::SslCredentialsOptions ssl_opts;
    // ssl_opts.pem_root_certs = server_cert;
    
    // auto secure_channel = grpc::CreateChannel("localhost:50052", grpc::SslCredentials(ssl_opts));
    // auto secure_channel = grpc::CreateCustomChannel("localhost:37645", grpc::SslCredentials(ssl_opts), channel_args);
    // HelloClient secure_client(secure_channel, "Secure Server");
    // std::shared_ptr<HelloClient> secure_client = std::make_shared<HelloClient>(secure_channel, "SecureServer");
    std::shared_ptr<HelloClient> secure_client = createSecureClient("localhost:37645");


    // Send hello messages
    insecure_client->SayHello("Hello from C++ to Insecure Server!");
    secure_client->SayHello("Hello from C++ to Secure Server!");

    return 0;
}
