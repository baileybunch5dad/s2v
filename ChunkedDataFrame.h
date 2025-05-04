 #ifndef CHUNKEDDATAFRAME_H
 #define CHUNKEDDATAFRAME_H

#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <vector>
#include <string>
#include <type_traits>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <cmath>
#include <variant>
#include <stdexcept>
#include <chrono>
#include <memory>

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

#include <iostream>
#include <memory>
#include <string>
#include <ctime>

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
#include "columnvariant.h"

class ChunkedDataFrame
{
public:
    std::vector<std::string> columnNames;
    std::vector<char> columnTypes;
    NamedColumns columnData;


    // explicit ChunkedDataFrame(std::string &file_path);
    explicit ChunkedDataFrame(const std::vector<std::string> &v);
    // std::vector<int64_t> &dateCol(int colIndex);
    // std::vector<double> &doubleCol(int colIndex);    
    // std::vector<std::string> &stringCol(int colIndex);
    int readChunk(int maxRows);

private:    
    std::ifstream file;
    std::vector<std::vector<std::string>> columns;
    std::vector<std::string> parquetFiles;

    arrow::Status doit(std::string & path_to_file, int &rowsPtr);
    void loadColumnTypes();
    void allocateStorage();
    void clearData();
    void loadHeaders(std::ifstream &file);
    int64_t parseDate(std::string &time_string);
    double parseDouble(std::string double_string);
    bool is_number(const std::string &s);
    int readCsvIntoColumns(int maxRows);
    size_t parquetCount;
};
    
#endif