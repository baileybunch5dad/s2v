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