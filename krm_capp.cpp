#include <Python.h>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include <ctime>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <fstream>
#include <unistd.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/signal.h>
#include "handshake.grpc.pb.h"
#include <filesystem>

// Arrow and Parquet headers
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
// #include <arrow/array/array_view.h>
#include <arrow/type_fwd.h>

#include <grpcpp/grpcpp.h>
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

namespace fs = std::filesystem;

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

class HandShakeClient
{
private:
    std::unique_ptr<handshake::HandShake::Stub> stub_;

    // Simple function to serialize an Arrow Table to a Buffer
    arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeTableToBuffer(
        const std::shared_ptr<arrow::Table> &table)
    {

        // Step 1: Create a BufferOutputStream to write to memory
        ARROW_ASSIGN_OR_RAISE(auto buffer_stream, arrow::io::BufferOutputStream::Create());

        // Step 2: Create IPC stream writer
        ARROW_ASSIGN_OR_RAISE(auto writer,
                              arrow::ipc::MakeStreamWriter(buffer_stream, table->schema()));

        // Step 3: Write the table to the stream
        ARROW_RETURN_NOT_OK(writer->WriteTable(*table));

        // Step 4: Close the writer to complete serialization
        ARROW_RETURN_NOT_OK(writer->Close());

        // Step 5: Finish and return the buffer
        return buffer_stream->Finish();
    }

    // arrow::Result<std::shared_ptr<arrow::Table>> DeserializeBufferToTable(
    //     const std::shared_ptr<arrow::Buffer> &buffer)
    // {
    //     auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    //     ARROW_ASSIGN_OR_RAISE(auto reader,
    //                           arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));
    //     std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    //     ARROW_ASSIGN_OR_RAISE(batches, reader->ToRecordBatches());
    //     return arrow::Table::FromRecordBatches(batches);
    // }

    // Helper method to serialize an Arrow table to a buffer

    // arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeTable(const std::shared_ptr<arrow::Table> &table)
    // {L

    //     // Define the initial capacity of the buffer.
    //     int64_t initial_capacity = 1024 * 1024 * 1024;

    //     // Create a resizable buffer.
    //     arrow::Result<std::shared_ptr<arrow::ResizableBuffer>> buffer_result = arrow::AllocateResizableBuffer(initial_capacity);
    //     if (!buffer_result.ok())
    //     {
    //         std::cerr << "Error allocating resizable buffer: " << buffer_result.status() << std::endl;
    //         exit(1);
    //     }
    //     std::shared_ptr<arrow::ResizableBuffer> buffer = *buffer_result;

    //     // Create an output stream that writes to the resizable buffer.
    //     std::shared_ptr<arrow::io::OutputStream> raw_output = std::make_shared<arrow::io::BufferOutputStream>(buffer);

    //     // Create a buffered output stream with a default buffer size
    //     arrow::Result<std::shared_ptr<arrow::io::BufferedOutputStream>> buffered_output_result = arrow::io::BufferedOutputStream::Create(initial_capacity, arrow::default_memory_pool(), raw_output);
    //     if (!buffered_output_result.ok())
    //     {
    //         std::cerr << "Error creating buffered output stream: " << buffered_output_result.status() << std::endl;
    //         exit(1);
    //     }
    //     std::shared_ptr<arrow::io::BufferedOutputStream> buffered_output = *buffered_output_result;

    //     // Create IPC writer options

    //     arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();

    //     ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(buffered_output, table->schema(), options));

    //     // Write the table

    //     ARROW_RETURN_NOT_OK(writer->WriteTable(*table));

    //     // Close the writer

    //     // ARROW_RETURN_NOT_OK(writer->Close());
    //     // ARROW_RETURN_NOT_OK(writer->Flush());

    //     // Finish the stream and get buffer
    //     std::shared_ptr<arrow::io::OutputStream> raw_stream;

    //     // If the underlying stream is a BufferOutputStream, we can directly get its buffer
    //     auto buffer_stream = std::dynamic_pointer_cast<arrow::io::BufferOutputStream>(
    //         buffered_output->raw());

    //     // If it's a BufferOutputStream, we can directly get its buffer
    //     auto buf = buffer_stream->Finish();
    //     return buf;
    // }

public:
    HandShakeClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(handshake::HandShake::NewStub(channel)) {}

    // Send an Arrow table to the server

    bool ProcessArrowStream(const std::shared_ptr<arrow::Table> &table)
    {

        // Serialize the Arrow table to a buffer

        arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_serialized = SerializeTableToBuffer(table);

        if (!maybe_serialized.ok())
        {

            std::cerr << "Failed to serialize table: " << maybe_serialized.status().ToString() << std::endl;

            return false;
        }

        std::shared_ptr<arrow::Buffer> serialized = *maybe_serialized;

        // Create the request and set the serialized data

        handshake::ArrowTableRequest request;

        request.set_serialized_table(serialized->data(), serialized->size());

        // Set up the response and context

        handshake::ArrowTableResponse response;

        grpc::ClientContext context;

        // Send the RPC

        grpc::Status status = stub_->ProcessArrowStream(&context, request, &response);

        if (status.ok())
        {

            std::cout << "Table sent successfully: " << response.message() << std::endl;

            return response.success();
        }
        else
        {

            std::cerr << "Error sending table: " << status.error_message() << std::endl;

            return false;
        }
    }

    std::string ProcessData(std::shared_ptr<arrow::Table> &table)
    {
        handshake::Table request{};

        int num_columns = table->num_columns();
        // int num_rows = table->num_rows();

        // std::cout << "Table: " << table->name() << std::endl;
        // std::cout << "Number of columns: " << num_columns << std::endl;
        // std::cout << "Number of rows: " << num_rows << std::endl;
        // std::cout << std::endl;

        for (int i = 0; i < num_columns; ++i)
        {
            std::shared_ptr<arrow::ChunkedArray> arrow_column = table->column(i);
            std::string arrow_column_name = table->field(i)->name();
            std::string arrow_column_type_name = arrow_column->type()->ToString();
            // std::cout << "Column " << i << " name='" << arrow_column_name << "' type='" << arrow_column_type_name << "'" << std::endl;
            // SingleColumn mv = columnData[i].second;
            // std::string grpcName = columnData[i].first;
            // std::cout << "grpc name " << grpcName << " grpcType " << columnTypes[i] << std::endl;

            handshake::Column *handshake_column = request.add_columns();
            handshake_column->set_name(arrow_column_name);
            handshake_column->set_type(arrow_column_type_name);

            for (int j = 0; j < arrow_column->num_chunks(); ++j)
            {
                std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(j);

                auto arrowChunkType = chunk->type_id();
                auto arrowChunkLen = chunk->length();
                // if(arrowChunkLen > 3) arrowChunkLen = 3; // temp for debugging

                if (arrowChunkType == arrow::Type::STRING)
                {
                    auto arrow_string_array = std::static_pointer_cast<arrow::StringArray>(chunk);
                    handshake::StringArray *handshake_string_array = handshake_column->mutable_string_array();
                    for (int k = 0; k < arrowChunkLen; k++)
                    {
                        std::string arrow_val = arrow_string_array->GetString(k);
                        handshake_string_array->add_v(arrow_val);
                    }
                }
                else if (arrowChunkType == arrow::Type::DOUBLE)
                {
                    auto arrow_double_array = std::static_pointer_cast<arrow::DoubleArray>(chunk);
                    handshake::DoubleArray *handshake_double_array = handshake_column->mutable_double_array();
                    for (int k = 0; k < arrowChunkLen; k++)
                    {
                        double arrow_val = arrow_double_array->Value(k);
                        handshake_double_array->add_v(arrow_val);
                    }
                }
                else if (arrowChunkType == arrow::Type::DATE32)
                {
                    auto arrow_date_array = std::static_pointer_cast<arrow::Date32Array>(chunk);
                    handshake::Int32Array *handshake_int32_array = handshake_column->mutable_int32_array();
                    for (int k = 0; k < arrowChunkLen; k++)
                    {
                        int32_t arrow_val = arrow_date_array->Value(k);
                        handshake_int32_array->add_v(arrow_val);
                    }
                }
                else if (arrowChunkType == arrow::Type::INT32)
                {
                    auto arrow_int32_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
                    handshake::Int32Array *handshake_int32_array = handshake_column->mutable_int32_array();
                    for (int k = 0; k < arrowChunkLen; k++)
                    {
                        int32_t arrow_val = arrow_int32_array->Value(k);
                        handshake_int32_array->add_v(arrow_val);
                    }
                }
                else if (arrowChunkType == arrow::Type::INT64)
                {
                    auto arrow_int64_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
                    handshake::Int64Array *handshake_int64_array = handshake_column->mutable_int64_array();
                    for (int k = 0; k < arrowChunkLen; k++)
                    {
                        int32_t arrow_val = arrow_int64_array->Value(k);
                        handshake_int64_array->add_v(arrow_val);
                    }
                }
                else
                {
                    std::cerr << "Unhandle type " << arrow_column_type_name << std::endl;
                    exit(1);
                }
            }
        }

        // Prepare response
        handshake::StringResponse response;

        // Set up context
        grpc::ClientContext context;

        // Call RPC
        grpc::Status status = stub_->ProcessData(&context, request, &response);

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
    std::string Shutdown()
    {
        handshake::ShutdownRequest request;
        handshake::ShutdownResponse response;
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

    std::string Hello(std::string instr)
    {
        handshake::HelloRequest request;
        handshake::HelloResponse response;
        grpc::ClientContext context;

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

    int AggregateLocal(const std::vector<int> &ports)
    {
        handshake::AggregateLocalRequest request;
        for (auto p : ports)
        {
            request.add_ports(p);
        }

        // Call the Aggregate RPC
        handshake::AggregateLocalResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub_->AggregateLocal(&context, request, &response);

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
        handshake::AggregateGlobalRequest request;
        for (int p : ports)
        {
            request.add_ports(p);
        }
        for (std::string s : servers)
        {
            request.add_servers(s);
        }

        // Call the Aggregate RPC
        handshake::AggregateGlobalResponse response;
        grpc::ClientContext context;
        grpc::Status status = stub_->AggregateGlobal(&context, request, &response);

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

std::string parmval(const std::string &s, const std::string &prfix)
{
    return s.substr(prfix.size() + 1);
}

std::vector<std::string> getListofParquetFiles()
{
    // std::string path = "/mnt/e/shared/parquet"; // Specify the directory path
    std::string path = "./parquet";
    std::vector<std::string> dirList;

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
    return dirList;
}

std::vector<std::vector<std::string>> chopList(const std::vector<std::string> &dirList, int numPieces)
{
    std::vector<std::vector<std::string>> cList;

    for (int i = 0; i < numPieces; i++)
    {
        cList.push_back(std::vector<std::string>{});
    }

    int which = 0;
    for (auto s : dirList)
    {
        cList[which].push_back(s);
        which++;
        if (which == numPieces)
        {
            which = 0;
        }
    }
    return cList;
}

arrow::Status doit(const std::string &path_to_file, std::shared_ptr<HandShakeClient> client)
{

    arrow::MemoryPool *pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(path_to_file));

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_ASSIGN_OR_RAISE(arrow_reader, parquet::arrow::OpenFile(input, pool));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

    // std::string s = client->ProcessData(table);
    // std::cout << s << std::endl;

    bool worked = client->ProcessArrowStream(table);
    if (!worked)
    {
        std::cerr << "Process ArrowStream failed " << std::endl;
        exit(1);
    }
    // std::cout << " worked " << worked << std::endl;

    return arrow::Status::OK();
}
void RunClient(std::shared_ptr<HandShakeClient> client, const std::vector<std::string> &fList, const std::string &client_name, int clientIdx, int numClients)
{
    std::thread::id threadId = std::this_thread::get_id();
    std::stringstream ss;
    ss << threadId;
    std::string threadIdString = ss.str();
    for (std::string fileName : fList)
    {
        // std::cout << "Thread " << threadId << " grpcing arrow in " << fileName << std::endl;
        arrow::Status status = doit(fileName, client);
        if (!status.ok())
        {
            std::cout << "status " << status.ToString() << std::endl;
            exit(1);
        }
    }
}

// int ensureChildDeath(FILE *pipe)
// {
//     // Get the file descriptor of the pipe
//     int fd = fileno(pipe);
//     if (fd == -1)
//     {
//         std::cerr << "fileno() failed!" << std::endl;
//         pclose(pipe);
//         return 1;
//     }

//     // Get the process ID of the child process
//     pid_t child_pid = fcntl(fd, F_GETOWN);
//     if (child_pid == -1)
//     {
//         std::cerr << "fcntl() failed!" << std::endl;
//         pclose(pipe);
//         return 1;
//     }

//     // Set the PR_SET_PDEATHSIG option to send SIGKILL if the parent dies
//     if (prctl(PR_SET_PDEATHSIG, SIGKILL) == -1)
//     {
//         std::cerr << "prctl() failed!" << std::endl;
//         pclose(pipe);
//         return 1;
//     }
//     return 0;
// }
std::vector<int> ports;
std::mutex vector_mutex;

void startPython(int argc, char **argv)
{
    std::unique_lock<std::mutex> lock(vector_mutex);
    int pipefd[2];
    if (pipe(pipefd) == -1)
    {
        perror("pipe");
        exit(1);
    }

    pid_t pid = fork();
    if (pid == -1)
    {
        perror("fork");
        exit(1);
    }

    if (pid == 0)
    {                     // Child Process (Python)
        close(pipefd[0]); // Close unused read end

        // Set PR_SET_PDEATHSIG to terminate child if parent dies
        prctl(PR_SET_PDEATHSIG, SIGTERM);

        // Redirect stdout to pipe
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);

        // Prepare execv arguments using std::vector
        std::vector<std::string> args;
        args.push_back(".venv/bin/python3.11");
        args.push_back("krm_pyapp.py");

        for (int i = 1; i < argc; ++i)
        {
            args.push_back(argv[i]); // Collect parent arguments
        }

        // Convert std::vector<std::string> to char* array for execv
        std::vector<char *> execArgs;
        for (auto &arg : args)
        {
            // std::cout << arg << ' ' ;
            execArgs.push_back(const_cast<char *>(arg.c_str()));
        }
        // std::cout << std::endl;
        execArgs.push_back(nullptr); // Null-terminate the list

        // Execute Python script with arguments

        execv(args[0].c_str(), execArgs.data());

        perror("execv"); // If execv fails
        exit(1);
    }
    else
    {                     // Parent Process (C++)
        close(pipefd[1]); // Close unused write end

        bool first = true;
        // std::string initialString = "";
        std::string multiline_string = "";
        char buffer[128];
        while (true)
        {
            ssize_t bytesRead = read(pipefd[0], buffer, sizeof(buffer) - 1);
            if (bytesRead > 0)
            {
                buffer[bytesRead] = '\0';
                // std::cout << "Python Output: " << buffer << std::endl;
                std::cout << buffer;
                if (first)
                {
                    // std::string multiline_string = buffer;
                    //  buffering until first complete line read for ports
                    // initialString = initialString + multiline_string;
                    multiline_string += buffer;
                    // if (initialString.find('\n') != std::string::npos)
                    if (multiline_string.find('\n') != std::string::npos)
                    {
                        // std::cout << "Reading ports" << std::endl;
                        // std::cout << "ml=" << multiline_string << std::endl;

                        std::string first_line;

                        std::istringstream stream(multiline_string);

                        // Read first line into a buffer
                        std::getline(stream, first_line);
                        // std::cout << "fl=" << first_line << std::endl;


                        // Parse space-separated integers
                        std::vector<int> numbers;
                        std::istringstream line_stream(first_line);
                        int num;

                        while (line_stream >> num)
                        {
                            std::cout << "Port " << num << std::endl;
                            ports.push_back(num);
                        }
                        first = false;
                        lock.unlock();
                    }
                }
            }
            else
            {
                break; // Exit when pipe closes
            }
        }

        close(pipefd[0]);
        std::cout << "Parent exiting, Python process should terminate..." << std::endl;
    }

    return;
}

// void oldStartPython(int argc, char **argv)
// {
//     std::unique_lock<std::mutex> lock(vector_mutex);
//     std::cout << "Launching user exit" << std::endl;
//     std::string cmd = "python3.11 krm_pyapp.py";
//     for (int i = 1; i < argc; i++)
//     {
//         cmd += " ";
//         cmd += argv[i];
//     }
//     std::cout << cmd << std::endl;
//     // std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
//     FILE *pipe = popen(cmd.c_str(), "r");

//     if (!pipe)
//     {
//         std::cerr << "Unable to launch python" << std::endl;
//         exit(1);
//     }
//     ensureChildDeath(pipe);
//     std::cout << "process launched, capturing output" << std::endl;
//     // lock.unlock();

//     char buffer[64];
//     bool first = true;
//     std::string initialString = "";
//     // while (fgets(buffer, sizeof(buffer), pipe.get()) != nullptr)
//     while (fgets(buffer, sizeof(buffer), pipe) != nullptr)
//     {
//         std::cout << buffer;
//         if (first)
//         {
//             std::string multiline_string = buffer;
//             //  buffering until first complete line read for ports
//             initialString = initialString + multiline_string;
//             if (initialString.find('\n') != std::string::npos)
//             {
//                 std::string first_line;

//                 std::istringstream stream(multiline_string);
//                 // Read first line into a buffer
//                 std::getline(stream, first_line);

//                 // Parse space-separated integers
//                 std::vector<int> numbers;
//                 std::istringstream line_stream(first_line);
//                 int num;

//                 while (line_stream >> num)
//                 {
//                     std::cout << "Port " << num << std::endl;
//                     ports.push_back(num);
//                 }
//                 first = false;
//                 lock.unlock();
//             }
//         }
//     }
//     pclose(pipe);
// }

std::shared_ptr<HandShakeClient> createSecureClient(std::string addr)
{
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
    std::shared_ptr<HandShakeClient> hcp = std::make_shared<HandShakeClient>(secure_channel);
    return hcp;
}

std::shared_ptr<HandShakeClient> createInSecureClient(std::string addr)
{
    grpc::ChannelArguments channel_args;
    channel_args.SetMaxSendMessageSize(1024 * 1024 * 1024);    // 1GB
    channel_args.SetMaxReceiveMessageSize(1024 * 1024 * 1024); // 1GB
    std::shared_ptr<grpc::ChannelCredentials> credentials = grpc::InsecureChannelCredentials();
    std::shared_ptr<grpc::Channel> insecure_channel = grpc::CreateCustomChannel(addr, credentials, channel_args);
    WaitForChannelReady(insecure_channel);
    std::shared_ptr<HandShakeClient> hcp = std::make_shared<HandShakeClient>(insecure_channel);
    return hcp;
}

bool tls = false;

int main(int argc, char **argv)
{
    // EmbeddedPythonController *epc = new EmbeddedPythonController();
    std::string server = "localhost";
    bool inprocessPython = true;
    // std::vector<int> ports = {50051, 50052, 50053, 50054};
    std::vector<std::string> externalServers = {};
    bool shutdown = false;
    std::thread pythrd;
    // std::vector<int> ports;

    for (int i = 1; i < argc; i++)
    {
        std::string s = argv[i];
        std::string o = "--server";
        if (starts_with(s, "--server"))
        {
            server = parmval(s, "--server");
        }
        if (starts_with(s, "--external"))
        {
            inprocessPython = false;
        }
        if (starts_with(s, "--secure") || (starts_with(s, "--tls")))
        {
            tls = true;
        }
        if (starts_with(s, "--nosecure") || (starts_with(s, "--notls")))
        {
            tls = false;
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

    if (inprocessPython)
    {
        std::cout << "inProcess" << std::endl;
        pythrd = std::thread(startPython, argc, argv);                 // , std::cref(ports));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // let other thread acquire the lock
        std::cout << "Waiting on lock" << std::endl;
        std::unique_lock<std::mutex> lock(vector_mutex);
        std::cout << "Processing output in primordial thread" << std::endl;
    }
    else
    {
        std::ifstream portfile("ports.txt"); // Open the file
        if (portfile)
        {
            std::istream_iterator<int> start(portfile), end;
            ports.assign(start, end); // Read integers into the vector
        }
        else
        {
            std::cerr << "Error opening file!\n";
            return 1;
        }
    }
    for (auto p : ports)
    {
        std::cout << p << ' ';
    }
    std::cout << "There are " << ports.size() << " ports opened in separate threads with dedicated grpc servers" << std::endl;

    std::cout << "Ports ";
    for (auto p : ports)
    {
        std::cout << p << " ";
    }
    std::cout << std::endl;
    std::cout << "Server " << server << std::endl;

    std::cout << "Starting multi-threaded C++ krm_capp talking to multiprocess krm_pyapp" << std::endl;
    // List of server addresses
    std::vector<std::string> server_addresses;
    for (auto p : ports)
        server_addresses.emplace_back(server + ":" + std::to_string(p));
    std::vector<std::shared_ptr<HandShakeClient>> clients;
    // Vector to hold threads
    std::vector<std::thread> threads;

    // Launch threads for each server
    std::cout << "TLS " << tls << std::endl;
    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        std::string addr = server_addresses[i];
        std::shared_ptr<HandShakeClient> client;
        if (tls)
            client = createSecureClient(addr);
        else
            client = createInSecureClient(addr);
        clients.push_back(client);

        // grpc::ChannelArguments channel_args;
        // channel_args.SetMaxSendMessageSize(1024 * 1024 * 1024);    // 1GB
        // channel_args.SetMaxReceiveMessageSize(1024 * 1024 * 1024); // 1GB
        // std::cout << "Creating channel for " << addr << std::endl;
        // std::cout << "Using TLS security" << tls << std::endl;

        // std::shared_ptr<grpc::ChannelCredentials> credentials;
        // if (tls)
        // {
        //     std::string server_cert;
        //     // Load the server certificate
        //     std::ifstream cert_file("server.crt");
        //     server_cert.assign(std::istreambuf_iterator<char>(cert_file), std::istreambuf_iterator<char>());
        //     grpc::SslCredentialsOptions ssl_opts;
        //     ssl_opts.pem_root_certs = server_cert;
        //     credentials = grpc::SslCredentials(ssl_opts);
        // }
        // else
        // {
        //     credentials = grpc::InsecureChannelCredentials();
        // }

        // std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(addr, credentials, channel_args);
        // WaitForChannelReady(channel);
        // HandShakeClient *client = new HandShakeClient(channel);
        // clients.emplace_back(client);
    }

    // std::cout << "Wait for socket binding" << std::endl;

    // std::this_thread::sleep_for(std::chrono::milliseconds(2000)); // ms

    auto dirList = getListofParquetFiles();
    auto clientDirList = chopList(dirList, server_addresses.size());

    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        auto client = clients[i];
        std::string client_name = "Client" + std::to_string(i + 1);
        std::string tmstr = client->Hello(client_name);
        if (std::strncmp(tmstr.c_str(), "RPC failed", 10) == 0)
        {
            std::cerr << "Aborting, cannot invoke remote procedure calls" << std::endl;
            exit(1);
        }
    }

    for (size_t i = 0; i < server_addresses.size(); ++i)
    {
        auto client = clients[i];
        std::string client_name = "Client" + std::to_string(i + 1);
        threads.emplace_back(RunClient, client, clientDirList[i], client_name, i, int(server_addresses.size()));
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

    // if (pythrd.joinable())
    // {
    //     pythrd.join();
    // }

    exit(0);

    return 0;
}
