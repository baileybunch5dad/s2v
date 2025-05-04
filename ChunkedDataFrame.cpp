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
#include "ChunkedDataFrame.h"

ChunkedDataFrame::ChunkedDataFrame(const std::vector<std::string> &v)
{
    // this->parquetFiles = v;
    for (auto pf : v)
    {
        std::cout << pf << std::endl;
        this->parquetFiles.push_back(pf);
        this->parquetCount = 0;
    }
    std::vector<std::string> new_strings = {
        "run_id",
        "cur_dt",
        "val_dt",
        "data_dt",
        "scenario",
        "txn_id",
        "sec_id",
        "prod_id",
        "val_method",
        "par_bal",
        "mkt_val",
        "mkt_val_pay",
        "mkt_val_opt1",
        "mkt_val_opt2",
        "clean_price",
        "avg_life",
        "mod_duration",
        "duration",
        "convexity",
        "stv",
        "sem_mkt_val",
        "dval_dr",
        "bs_delta",
        "bs_gamma",
        "bs_vega",
        "bs_theta",
        "bs_rho",
        "par_cpn",
        "ref_name",
        "rw_model",
        "ptnl_exp",
        "cr_exp",
        "pd",
        "lgd",
        "rw",
        "r_corr",
        "m_mat",
        "b_mat_adj",
        "ccf",
        "ead",
        "rwa",
        "capital",
        "el",
        "own_funds",
        "jtd_pre_adj",
        "jtd_post_adj",
    };
    for (auto colNm : new_strings)
    {
        columnNames.push_back(colNm);
    }

    // loadHeaders(file);
    loadColumnTypes();
    allocateStorage();
}

// ChunkedDataFrame::ChunkedDataFrame(std::string &file_path)
// {
//     file = std::ifstream(file_path);
//     if (!file.is_open())
//     {
//         std::cerr << "Failed to open the file: " << file_path << std::endl;
//         return;
//     }

//     loadHeaders(file);
//     loadColumnTypes();
//     allocateStorage();
// }

// temporarily pre-assign rather than get dynamically
void ChunkedDataFrame::loadColumnTypes()
{
    columnTypes = std::vector<char>{
        'c', //  run_id       | character varying(8)  |           | not null |
        'd', //  cur_dt       | date                  |           | not null |
        'd', //  val_dt       | date                  |           | not null |
        'd', //  data_dt      | date                  |           | not null |
        'f', //  scenario     | numeric(9,0)          |           | not null |
        'c', //  txn_id       | character varying(38) |           | not null |
        'c', //  sec_id       | character varying(18) |           |          |
        'f', //  prod_id      | numeric(14,0)         |           | not null |
        'f', //  val_method   | numeric(2,0)          |           |          |
        'f', //  par_bal      | numeric(18,2)         |           |          |
        'f', //  mkt_val      | numeric(18,2)         |           |          |
        'f', //  mkt_val_pay  | numeric(18,2)         |           |          |
        'f', //  mkt_val_opt1 | numeric(18,2)         |           |          |
        'f', //  mkt_val_opt2 | numeric(18,2)         |           |          |
        'f', //  clean_price  | numeric(18,6)         |           |          |
        'f', //  avg_life     | numeric(18,6)         |           |          |
        'f', //  mod_duration | numeric(18,6)         |           |          |
        'f', //  duration     | numeric(18,6)         |           |          |
        'f', //  convexity    | numeric(18,6)         |           |          |
        'f', //  stv          | numeric(18,2)         |           |          |
        'f', //  sem_mkt_val  | numeric(18,2)         |           |          |
        'f', //  dval_dr      | numeric(22,2)         |           |          |
        'f', //  bs_delta     | numeric(18,2)         |           |          |
        'f', //  bs_gamma     | numeric(18,2)         |           |          |
        'f', //  bs_vega      | numeric(18,2)         |           |          |
        'f', //  bs_theta     | numeric(18,2)         |           |          |
        'f', //  bs_rho       | numeric(18,2)         |           |          |
        'f', //  par_cpn      | numeric(11,6)         |           |          |
        'c', //  ref_name     | character varying(25) |           |          |
        'c', //  rw_model     | character varying(3)  |           |          |
        'f', //  ptnl_exp     | numeric(18,2)         |           |          |
        'f', //  cr_exp       | numeric(18,2)         |           |          |
        'f', //  pd           | numeric(16,12)        |           |          |
        'f', //  lgd          | numeric(11,6)         |           |          |
        'f', //  rw           | numeric(11,6)         |           |          |
        'f', //  r_corr       | numeric(11,6)         |           |          |
        'f', //  m_mat        | numeric(11,6)         |           |          |
        'f', //  b_mat_adj    | numeric(11,6)         |           |          |
        'f', //  ccf          | numeric(11,6)         |           |          |
        'f', //  ead          | numeric(18,2)         |           |          |
        'f', //  rwa          | numeric(18,2)         |           |          |
        'f', //  capital      | numeric(18,2)         |           |          |
        'f', //  el           | numeric(18,2)         |           |          |
        'f', //  own_funds    | numeric(18,2)         |           |          |
        'f', //  jtd_pre_adj  | numeric(18,2)         |           |          |
        'f', //  jtd_post_adj | numeric(18,2)         |           |          |
    };
}

void ChunkedDataFrame::allocateStorage()
{

    for (size_t col = 0; col < columnTypes.size(); col++)
    {
        columns.emplace_back();
        std::string name = columnNames[col];
        DynamicVector dynVec;
        std::string type;
        if (columnTypes[col] == 'f')
        {
            dynVec = std::vector<double>{};
            type = "double";
        }
        else if (columnTypes[col] == 'c')
        {
            dynVec = std::vector<std::string>{};
            type = "string";
        }
        else if (columnTypes[col] == 'd')
        {
            dynVec = std::vector<int64_t>{};
            type = "date32";
        }
        columnData.push_back({SingleColumn(name, type, dynVec)});
    }
}

void ChunkedDataFrame::clearData()
{
    for (size_t col = 0; col < columnTypes.size(); col++)
    {
        DynamicVector v = columnData[col].values;

        if (std::holds_alternative<std::vector<double>>(v))
        {
            std::vector<double> &dvp = std::get<std::vector<double>>(v);
            dvp.clear();
        }
        else if (std::holds_alternative<std::vector<int32_t>>(v))
        {
            std::vector<int32_t> &ivp = std::get<std::vector<int32_t>>(v);
            ivp.clear();
        }
        else if (std::holds_alternative<std::vector<int64_t>>(v))
        {
            std::vector<int64_t> &ivp = std::get<std::vector<int64_t>>(v);
            ivp.clear();
        }
        else if (std::holds_alternative<std::vector<std::string>>(v))
        {
            std::vector<std::string> &svp = std::get<std::vector<std::string>>(v);
            svp.clear();
        }
        columns[col].clear();
    }
}

void ChunkedDataFrame::loadHeaders(std::ifstream &file)
{
    std::string line;

    // Read the header line
    if (std::getline(file, line))
    {
        std::istringstream header_stream(line);
        std::string header;
        while (std::getline(header_stream, header, ','))
        {
            columnNames.push_back(header);
        }
    }
}

int64_t ChunkedDataFrame::parseDate(std::string &time_string)
{
    std::tm t{};
    std::istringstream ss(time_string);
    // std::cout << time_string << std::endl;
    ss >> std::get_time(&t, "%Y-%m-%d");
    if (ss.fail())
    {
        std::cout << "Failed to parse time string " << time_string << std::endl;
        return 0;
    }
    std::time_t time_since_epoch = mktime(&t);
    // Time back to string
    // std::tm t2 = *std::localtime(&time_since_epoch);
    std::tm t2 = *std::gmtime(&time_since_epoch);
    std::ostringstream oss;
    oss << std::put_time(&t2, "%Y-%m-%d %H:%M:%S");
    std::string time_string_again = oss.str();
    // std::cout << "Orig:" << time_string << " Seconds:" << time_since_epoch << " Revert:" << time_string_again << std::endl;
    // std::cout << "Time as string again: " << time_string_again << "\n";
    int64_t i64 = static_cast<int64_t>(time_since_epoch);
    return i64;
}

double ChunkedDataFrame::parseDouble(std::string double_string)
{

    double d{};
    if (double_string.size() > 0)
    {
        try
        {
            d = std::stof(double_string);
            // std::cout << "Double value: " << d << std::endl;
        }
        catch (const std::invalid_argument &e)
        {
            std::cerr << "Invalid argument: " << e.what() << std::endl;
        }
        catch (const std::out_of_range &e)
        {
            std::cerr << "Out of range: " << e.what() << std::endl;
        }
    }
    else
    {
        d = std::nan("");
        // std::cout << "Double value as Nan: " << d << std::endl;
    }
    return d;
}

// Helper function to check if a string is numeric
bool ChunkedDataFrame::is_number(const std::string &s)
{
    std::istringstream iss(s);
    double number;
    return (iss >> number) && iss.eof();
}

// std::vector<int64_t> *ChunkedDataFrame::dateCol(int colIndex)
// {
//     return std::get<std::vector<int64_t> *>(columnData[colIndex]);
// }
// std::vector<double> *ChunkedDataFrame::doubleCol(int colIndex)
// {
//     return std::get<std::vector<double> *>(columnData[colIndex]);
// }
// std::vector<std::string> *ChunkedDataFrame::stringCol(int colIndex)
// {
//     return std::get<std::vector<std::string> *>(columnData[colIndex]);
// }

int ChunkedDataFrame::readCsvIntoColumns(int maxRows)
{
    // size_t numCols = columnTypes.size();
    std::string line;
    int row = 0;
    while (std::getline(file, line) && row < maxRows)
    {
        std::istringstream line_stream(line);
        std::string cell;

        size_t col_index = 0;
        while (std::getline(line_stream, cell, ','))
        {
            columns[col_index].push_back(cell); // Add value to corresponding column
            ++col_index;
        }
        if ((size_t)col_index < columnNames.size())
            columns[col_index++].push_back("");
        // std::cout << "Populated data for " << columnNames[0] << ".." <<  columnNames[col_index-1] << std::endl;
        row++;
    }
    return row;
}

arrow::Status ChunkedDataFrame::doit(std::string &path_to_file, int &rowsPtr)
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

    int num_columns = table->num_columns();
    int num_rows = table->num_rows();
    rowsPtr = num_rows;

    // std::cout << "Table: " << table->name() << std::endl;
    std::cout << "Number of columns: " << num_columns << std::endl;
    std::cout << "Number of rows: " << num_rows << std::endl;
    std::cout << std::endl;

    columnData.clear();
    for (int i = 0; i < num_columns; ++i)
    {
        int rowNum = 0;
        std::shared_ptr<arrow::ChunkedArray> column = table->column(i);
        std::string arrow_column_name = table->field(i)->name();
        std::string arrow_column_type_name = column->type()->ToString();
        std::cout << "Column " << i << ": " << arrow_column_name << " (" << arrow_column_type_name << ")" << std::endl;
        // SingleColumn mv = columnData[i].second;
        // std::string grpcName = columnData[i].first;
        // std::cout << "grpc name " << grpcName << " grpcType " << columnTypes[i] << std::endl;

        if (arrow_column_type_name == "string")
        {
            auto newVec = std::vector<std::string>{};
            newVec.reserve(num_rows);
            DynamicVector dv = newVec;
            SingleColumn single_column(arrow_column_name, arrow_column_type_name, dv);
            columnData.push_back(single_column);
        }
        else if (arrow_column_type_name == "date32" || arrow_column_type_name == "int32")
        {
            auto newVec = std::vector<int32_t>{};
            newVec.reserve(num_rows);
            DynamicVector dv = newVec;
            SingleColumn single_column(arrow_column_name, arrow_column_type_name, dv);
            columnData.push_back(single_column);
        }
        else if (arrow_column_type_name == "date64" || arrow_column_type_name == "int64")
        {
            auto newVec = std::vector<int64_t>{};
            newVec.reserve(num_rows);
            DynamicVector dv = newVec;
            SingleColumn single_column(arrow_column_name, arrow_column_type_name, dv);
            columnData.push_back(single_column);
        }
        else if (arrow_column_type_name == "double" || arrow_column_type_name == "float64")
        {
            auto newVec = std::vector<double>{};
            newVec.reserve(num_rows);
            DynamicVector dv = newVec;
            SingleColumn single_column(arrow_column_name, arrow_column_type_name, dv);
            columnData.push_back(single_column);
        }

        for (int j = 0; j < column->num_chunks(); ++j)
        {
            std::shared_ptr<arrow::Array> chunk = column->chunk(j);

            auto arrowChunkType = chunk->type_id();
            auto arrowChunkLen = chunk->length();

            DynamicVector dvp = columnData[i].values;
            if (arrowChunkType == arrow::Type::STRING)
            {
                auto arrow_string_array = std::static_pointer_cast<arrow::StringArray>(chunk);
                std::vector<std::string> &stringVec = std::get<std::vector<std::string>>(dvp);
                for (int k = 0; k < arrowChunkLen; k++)
                {
                    std::string arrow_val = arrow_string_array->GetString(k);
                    stringVec[rowNum++] = arrow_val;
                }
            }
            else if (arrowChunkType == arrow::Type::DATE32)
            {
                auto arrow_date_array = std::static_pointer_cast<arrow::Date32Array>(chunk);
                std::vector<int32_t> &int32Vec = std::get<std::vector<int32_t>>(dvp);
                for (int k = 0; k < arrowChunkLen; k++)
                {
                    int32_t arrow_val = arrow_date_array->Value(k);
                    int32Vec[rowNum++] = arrow_val;
                }
            }
            else if (arrowChunkType == arrow::Type::DOUBLE)
            {
                auto arrow_double_array = std::static_pointer_cast<arrow::DoubleArray>(chunk);
                std::vector<double> &doubleVec = std::get<std::vector<double>>(dvp);
                for (int k = 0; k < arrowChunkLen; k++)
                {
                    double d = arrow_double_array->Value(k);
                    doubleVec[rowNum++] = d;
                }
            }
            else if (arrowChunkType == arrow::Type::INT32)
            {
                auto arrow_int32_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
                std::vector<int32_t> &int32Vec = std::get<std::vector<int32_t>>(dvp);
                for (int k = 0; k < arrowChunkLen; k++)
                {
                    int32_t arrow_val = arrow_int32_array->Value(k);
                    int32Vec[rowNum++] = arrow_val;
                }
            }
            else if (arrowChunkType == arrow::Type::INT64)
            {
                auto arrow_int64_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
                std::vector<int64_t> &int64Vec = std::get<std::vector<int64_t>>(dvp);
                for (int k = 0; k < arrowChunkLen; k++)
                {
                    int64_t arrow_val = arrow_int64_array->Value(k);
                    int64Vec[rowNum++] = arrow_val;
                }
            }
            else
            {
                std::cerr << "Uknonw column type " << std::endl;
            }
        }
    }
    //                 std::cout << arrow_string_array->GetString(k) << " ";

    //         }
    //         for (int k = 0; k < chunk->length() && k < 3; ++k)
    //         {
    //             if (chunk->type_id() == arrow::Type::STRING)
    //             {
    //                 auto string_array = std::static_pointer_cast<arrow::StringArray>(chunk);
    //                 std::cout << string_array->GetString(k) << " ";
    //             }
    //             else if (chunk->type_id() == arrow::Type::INT64)
    //             {
    //                 auto int64_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
    //                 std::cout << int64_array->Value(k) << " ";
    //             }
    //             else if (chunk->type_id() == arrow::Type::INT32)
    //             {
    //                 auto int32_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
    //                 std::cout << int32_array->Value(k) << " ";
    //             }
    //             else if (chunk->type_id() == arrow::Type::DOUBLE)
    //             {
    //                 auto double_array = std::static_pointer_cast<arrow::DoubleArray>(chunk);
    //                 std::cout << double_array->Value(k) << " ";
    //             }
    //             else if (chunk->type_id() == arrow::Type::DATE32)
    //             {
    //                 auto date32_casted_array = std::static_pointer_cast<arrow::Date32Array>(chunk);
    //                 std::cout << date32_casted_array->Value(k) << " ";
    //             }
    //         }
    //     }
    // }
    return arrow::Status::OK();
}

int ChunkedDataFrame::readChunk(int maxRows)
{
    clearData();

    if (parquetFiles.size() > 0)
    {
        if (parquetCount >= parquetFiles.size())
            return 0;

        // Define the path to the Parquet file
        std::string path_to_file = parquetFiles[parquetCount++];
        int rows = 0;
        arrow::Status status = doit(path_to_file, rows);
        if (!status.ok())
        {
            return 0;
        }
        return rows;
    }
    else
    {

        std::string line;
        // Read the data lines
        int row = readCsvIntoColumns(maxRows);

        for (size_t colIndex = 0; colIndex < columnTypes.size(); colIndex++)
        {
            std::string name = columnData[colIndex].name;
            DynamicVector dvp = columnData[colIndex].values;
            if (columnTypes[colIndex] == 'd')
            {
                std::vector<int64_t> & dateVecPtr = std::get<std::vector<int64_t>>(dvp);
                for (auto &cell : columns[colIndex])
                    dateVecPtr.emplace_back(parseDate(cell));
            }
            else if (columnTypes[colIndex] == 'f')
            {
                std::vector<double> &doubleVecPtr = std::get<std::vector<double>>(dvp);
                for (auto &cell : columns[colIndex])
                    doubleVecPtr.emplace_back(parseDouble(cell));
            }
            else
            {
                std::vector<std::string> &stringVecPtr = std::get<std::vector<std::string>>(dvp);
                for (auto &cell : columns[colIndex])
                    stringVecPtr.emplace_back(cell);
            }
        }
        if (row < maxRows)
            file.close();
        return row;
    }
    return (0);
}q
