#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "dataservice.grpc.pb.h"

using dataservice::Column;
using dataservice::DataService;
// using dataservice::ProcessDataRequest;
// using dataservice::ProcessDataResponse;
using dataservice::Table;
using dataservice::Column;
using dataservice::StringArray;
using dataservice::DoubleArray;
using dataservice::LongArray;
using dataservice::StringArray;
using dataservice::StringResponse;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using SingleColumn = std::variant<std::vector<std::string>*, std::vector<double>*, std::vector<int64_t>*>;
using NamedColumns = std::vector<std::pair<std::string, SingleColumn>>;

class DataServiceClient
{
public:
    DataServiceClient(std::shared_ptr<Channel> channel)
        : stub_(DataService::NewStub(channel)) {}

    // Send data to server
    std::string ProcessData(const NamedColumns &columnsData)
    {
        // Create request
        // ProcessDataRequest request;
        Table request = Table{};
        // Table *table = request.mutable_data();

        // Fill the table with columns
        for (const auto &columnData : columnsData)
        {
            Column *column = request.add_columns();
            std::string name = columnData.first;
            column->set_name(name);

            // Add values to the column

            SingleColumn vec = columnData.second;

            if (std::holds_alternative<std::vector<double>*>(vec))
            {
                std::vector<double> *dv = std::get<std::vector<double>*>(vec);
                DoubleArray *double_array = column->mutable_double_array();
                for (auto d : *dv)
                {
                    // Value *value = column->add_values();
                    std::cout << d << std::endl;
                    // value->set_double_value(d);
                    double_array->add_v(d);
                }
            }
            else if (std::holds_alternative<std::vector<int64_t>*>(vec))
            {
                std::vector<int64_t> *iv = std::get<std::vector<int64_t>*>(vec);
                LongArray *long_array = column->mutable_long_array();
                for (auto i : *iv)
                {
                    // Value *value = column->add_values();
                    std::cout << i << std::endl;
                    // value->set_long_value(i);
                    long_array->add_v(i);
                }
            }
            else if (std::holds_alternative<std::vector<std::string>*>(vec))
            {
                std::vector<std::string> *sv = std::get<std::vector<std::string>*>(vec);
                StringArray *string_array = column->mutable_string_array();
                for (auto s : *sv)
                {
                    // Value *value = column->add_values();
                    std::cout << s << std::endl;
                    // value->set_string_value(s);
                    string_array->add_v(s);
                }
            }
        }

        // for (const auto &val : columnData.second)
        // {
        //     Value *value = column->add_values();

        //     if (std::holds_alternative<double>(val))
        //         value->set_double_value(std::get<double>(val));
        //     else if (std::holds_alternative<int64_t>(val))
        //         value->set_long_value(std::get<double>(val));
        //     else
        //         value->set_string_value(std::get<std::string>(val));
        // }
    // }

    // Prepare response
    StringResponse response;

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

private : std::unique_ptr<DataService::Stub>
              stub_;
}
;

int main()
{
    // Connect to the server
    std::string server_address("localhost:50051");

    // Create channel
    auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
    DataServiceClient client(channel);

    // Create example data: two columns, one numeric and one string
    NamedColumns data;
    // std::vector<std::pair<std::string, std::vector<std::variant<double, int64_t, std::string>>>> data;

    // Create a numeric column named "prices"
    SingleColumn prices = new std::vector<double>{10.5, 20.3, 15.7, 30.2};
    // std::vector<std::variant<double, int64_t, std::string>> prices = {10.5, 20.3, 15.7, 30.2};
    data.push_back({"prices", prices});

    // Create a string column named "categories"
    // std::vector<std::variant<double, int64_t, std::string>> categories = {"food", "electronics", "books", "clothes"};
    SingleColumn categories = new std::vector<std::string>{"food", "electronics", "books", "clothes"};
    data.push_back({"categories", categories});

    // std::vector<std::variant<double, int64_t, std::string>> numbers = {12345L, 6789L, 271819L, 311415L};
    SingleColumn dates = new std::vector<int64_t>{12345L, 6789L, 271819L, 311415L};
    // for(auto nOuter: numbers) {
    //     auto nInner = std::get<int64_t>(nOuter);
    //     std::cout << nInner << std::endl;
    // }
    data.push_back({"dates", dates});

    // Send the data
    std::string response = client.ProcessData(data);
    std::cout << "Server response: " << response << std::endl;

    return 0;
}