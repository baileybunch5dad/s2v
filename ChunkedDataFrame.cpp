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
#include "ChunkedDataFrame.h"

ChunkedDataFrame::ChunkedDataFrame(std::string &file_path)
{
    file = std::ifstream(file_path);
    if (!file.is_open())
    {
        std::cerr << "Failed to open the file: " << file_path << std::endl;
        return;
    }

    loadHeaders(file);
    loadColumnTypes();
    allocateStorage();
}

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
        if (columnTypes[col] == 'f')
            columnData.emplace_back(new std::vector<double>);
        else if (columnTypes[col] == 'c')
            columnData.emplace_back(new std::vector<std::string>);
        else if (columnTypes[col] == 'd')
            columnData.emplace_back(new std::vector<int64_t>);
        columns.emplace_back(); // Create a column for each header
    }
}

void ChunkedDataFrame::clearData()
{
    for (size_t col = 0; col < columnTypes.size(); col++)
    {
        if (columnTypes[col] == 'f')
            (std::get<std::vector<double> *>(columnData[col]))->clear();
        else if (columnTypes[col] == 'c')
            (std::get<std::vector<std::string> *>(columnData[col]))->clear();
        else if (columnTypes[col] == 'd')
            (std::get<std::vector<int64_t> *>(columnData[col]))->clear();
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


std::vector<int64_t> *ChunkedDataFrame::dateCol(int colIndex)
{
    return std::get<std::vector<int64_t> *>(columnData[colIndex]);
}
std::vector<double> *ChunkedDataFrame::doubleCol(int colIndex)
{
    return std::get<std::vector<double> *>(columnData[colIndex]);
}
std::vector<std::string> *ChunkedDataFrame::stringCol(int colIndex)
{
    return std::get<std::vector<std::string> *>(columnData[colIndex]);
}

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
        if((size_t) col_index < columnNames.size())
            columns[col_index++].push_back("");
        // std::cout << "Populated data for " << columnNames[0] << ".." <<  columnNames[col_index-1] << std::endl;
        row++;
    }
    return row;
}

int ChunkedDataFrame::readChunk(int maxRows)
{
    clearData();
    std::string line;
    // Read the data lines
    int row = readCsvIntoColumns(maxRows);

    for (size_t colIndex = 0; colIndex < columnTypes.size(); colIndex++)
    {
        if (columnTypes[colIndex] == 'd')
        {
            auto dateVec = dateCol(colIndex);
            for (auto &cell : columns[colIndex])
                dateVec->emplace_back(parseDate(cell));
        }
        else if (columnTypes[colIndex] == 'f')
        {
            auto doubleVec = doubleCol(colIndex);
            for (auto &cell : columns[colIndex])
                doubleVec->emplace_back(parseDouble(cell));
        }
        else
        {
            auto stringVec = stringCol(colIndex);
            for (auto &cell : columns[colIndex])
                stringVec->emplace_back(cell);
        }
    }
    if (row < maxRows)
        file.close();
    return row;
}
