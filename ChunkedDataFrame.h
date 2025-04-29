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

class ChunkedDataFrame
{
public:
    std::vector<std::string> columnNames;
    std::vector<char> columnTypes;
    std::vector<std::variant<std::vector<std::string> *, std::vector<double> *, std::vector<int64_t> *>> columnData;


    explicit ChunkedDataFrame(std::string &file_path);
    std::vector<int64_t> *dateCol(int colIndex);
    std::vector<double> *doubleCol(int colIndex);    
    std::vector<std::string> *stringCol(int colIndex);
    int readChunk(int maxRows);

private:    
    std::ifstream file;
    std::vector<std::vector<std::string>> columns;


    void loadColumnTypes();
    void allocateStorage();
    void clearData();
    void loadHeaders(std::ifstream &file);
    int64_t parseDate(std::string &time_string);
    double parseDouble(std::string double_string);
    bool is_number(const std::string &s);
    int readCsvIntoColumns(int maxRows);
};
    
#endif