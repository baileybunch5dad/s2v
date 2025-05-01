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

int main()
{
    // File path
    std::string csv_file = "/mnt/e/shared/mv_out.csv";
    ChunkedDataFrame cdf(csv_file);

    auto start_time = std::chrono::high_resolution_clock::now();
    int chunkSize = 50000;
    long totRows = 28229622;
    long totRowsRead = 0;
    for (int rowsRead = cdf.readChunk(chunkSize); rowsRead > 0; rowsRead = rowsRead = cdf.readChunk(chunkSize))
    {
        for (int colIndex = 0; colIndex < cdf.columnData.size(); colIndex++)
        {
            std::string name = cdf.columnData[colIndex].first;
            SingleColumn c = cdf.columnData[colIndex].second;
            // std::cout << name << " ";
            if(std::holds_alternative<std::vector<std::string>*>(c)) {
                std::vector<std::string>* sv = std::get<std::vector<std::string>*>(c);             
                for(auto s : *sv) {
                    auto s2 = s;
                    // std::cout << s << " ";
                }
            }
            else if(std::holds_alternative<std::vector<double>*>(c)) {
                std::vector<double>* dv = std::get<std::vector<double>*>(c);             
                for(auto d : *dv) {
                    auto d2 = d;
                    // std::cout << d << " ";
                }
            }
            else if(std::holds_alternative<std::vector<int64_t>*>(c)) {
                std::vector<int64_t>* lv = std::get<std::vector<int64_t>*>(c);             
                for(auto l : *lv) {
                    auto l2 = l;
                    // std::cout << l << " ";
                }
            }
            // std::cout << std::endl;
            // std::cout << cdf.columnNames[colIndex] << " ";
            // if (cdf.columnTypes[colIndex] == 'd')
            // {
            //     for (auto &dateItem : *cdf.dateCol(colIndex))
            //         std::cout << dateItem << " ";
            // }
            // else if (cdf.columnTypes[colIndex] == 'f')
            // {
            //     for (auto &doubleItem : *cdf.doubleCol(colIndex))
            //         std::cout << doubleItem << " ";
            // }
            // else
            // {
            //     for (auto &stringItem : *cdf.stringCol(colIndex))
            //         std::cout << stringItem << " ";
            // }
            // std::cout << std::endl;
        }
        auto cur_time = std::chrono::high_resolution_clock::now();
        totRowsRead += rowsRead;
        double percentComplete = 100.*(totRowsRead)/totRows;
        std::chrono::duration<double> elapsed_seconds = cur_time - start_time;
        std::cout << "Percent complete " << percentComplete << "% Elapsed " << elapsed_seconds.count() << " seconds." << std::endl;
    }

    return 0;
}