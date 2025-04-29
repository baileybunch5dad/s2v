#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "handshake.grpc.pb.h"
#include "HandShakeClient.h"

HandShakeClient::HandShakeClient(std::shared_ptr<Channel> channel)
    : stub_(HandShake::NewStub(channel)) {}

std::string HandShakeClient::ProcessTable(const std::vector<std::pair<std::string, std::vector<std::variant<double, int64_t,  std::string>>>> &columnsData)
{
    // Create request
    ProcessDataRequest request;
    Table *table = request.mutable_data();

    // Fill the table with columns
    for (const auto &columnData : columnsData)
    {
        Column *column = table->add_columns();
        column->set_name(columnData.first);

        // Add values to the column
        for (const auto &val : columnData.second)
        {
            Value *value = column->add_values();

            if (std::holds_alternative<double>(val))
            {
                value->set_double_value(std::get<double>(val));
            }
            else if (std::holds_alternative<int64_t>(val))
            {
                value->set_int64_value(std::get<int64_t>(val));
            }
            else
            {
                value->set_string_value(std::get<std::string>(val));
            }
        }
    }

    // Prepare response
    ProcessDataResponse response;

    // Set up context
    ClientContext context;

    // Call RPC
    Status status = stub_->ProcessData(&context, request, &response);

    // Check status
    if (status.ok())
    {
        return response.status();
    }
    else
    {
        std::cout << "RPC failed: " << status.error_message() << std::endl;
        return "RPC failed: " + status.error_message();
    }
}
std::string HandShakeClient::Shutdown()
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

// std::string HandShakeClient::SendDataFrame(std::vector<std::string> &columnNames,
//                                            std::vector<char> &columnTypes,
//                                            std::vector<std::variant<std::vector<std::string> *, std::vector<double> *, std::vector<int64_t> *>> &columnData)
// {
//     handshake::DataFrame request;
//     StringResponse response;
//     grpc::ClientContext context;

//     for (size_t i = 0; i < columnNames.size(); i++)
//     {
//         auto *col = request.add_columns();
//         col->set_name(columnNames[i]);
//         if (columnTypes[i] == 'd')
//         {
//             auto dateVecIn = std::get<std::vector<int64_t> *>(columnData[i]);
//             auto dateVecOut = col->mutable_int64_values();
//             for (auto &cell : *dateVecIn)
//                 dateVecOut->add_values(cell);
//             std::cout << columnNames[i] << " dateVecOut->size=" << dateVecOut->values_size() << std::endl;
//         }
//         else if (columnTypes[i] == 'f')
//         {
//             auto doubleVecIn = std::get<std::vector<double> *>(columnData[i]);
//             auto doubleVecOut = col->mutable_double_values();
//             for (auto &cell : *doubleVecIn)
//                 doubleVecOut->add_values(cell);
//             std::cout << columnNames[i] << " doubleVecOut->size=" << doubleVecOut->values_size() << std::endl;
//         }
//         else
//         {
//             auto stringVecIn = std::get<std::vector<std::string> *>(columnData[i]);
//             std::cout << "Adding " << stringVecIn->size() << " strings for " << columnNames[i] << std::endl;
//             auto stringVecOut = col->mutable_string_values();
//             for (auto &cell : *stringVecIn)
//                 stringVecOut->add_values(cell);
//             std::cout << columnNames[i] << " stringVecOut->size=" << stringVecOut->values_size() << std::endl;
//         }
//     }
//     // auto *col1 = request.add_columns();
//     // col1->set_name("SomeStrings");
//     // auto svalptr = col1->mutable_string_values();
//     // svalptr->add_values("hello");
//     // svalptr->add_values("there");
//     // auto *col2 = request.add_columns();
//     // col2->set_name("NiceNumbers");
//     // auto dvalptr = col2->mutable_double_values();
//     // dvalptr->add_values(3.1415);
//     // dvalptr->add_values(2.7182818);
//     // auto *col3 = request.add_columns();
//     // col3->set_name("AndDatetimeValues");
//     // auto dtvalptr = col3->mutable_datetime_values();
//     // auto now = std::chrono::system_clock::now();
//     // auto seconds_since_epoch = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
//     // dtvalptr->add_values(seconds_since_epoch);
//     // dtvalptr->add_values(seconds_since_epoch + 100);

//     grpc::Status status = stub_->SendDataFrame(&context, request, &response);

//     if (status.ok())
//     {
//         return response.str();
//     }
//     else
//     {
//         std::cout << "SendDataFrame" << std::endl;
//         std::cout << status.error_code() << ": " << status.error_message()
//                   << std::endl;
//         return "RPC failed";
//     }
// }
std::string HandShakeClient::Hello(std::string instr)
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

std::string HandShakeClient::SendArray(const std::string &dnames, const std::vector<double> &dvals,
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

int HandShakeClient::AggregateLocal(const std::vector<int> &ports)
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

int HandShakeClient::AggregateGlobal(const std::vector<int> &ports, const std::vector<std::string> &servers)
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
