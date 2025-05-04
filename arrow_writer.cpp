#include <arrow/io/file.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <iostream>

void read_parquet(const std::string& file_path) {
    // Open the Parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    arrow::Status status = arrow::io::ReadableFile::Open(file_path, arrow::default_memory_pool(), &infile);
    if (!status.ok()) {
        std::cerr << "Error opening file: " << status.ToString() << std::endl;
        return;
    }

    // Create a Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    status = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
    if (!status.ok()) {
        std::cerr << "Error opening Parquet reader: " << status.ToString() << std::endl;
        return;
    }

    // Read the file into an Arrow Table
    std::shared_ptr<arrow::Table> table;
    status = reader->ReadTable(&table);
    if (!status.ok()) {
        std::cerr << "Error reading Parquet file into Arrow Table: " << status.ToString() << std::endl;
        return;
    }

    // Print table metadata
    std::cout << "Table Columns: " << table->num_columns() << ", Rows: " << table->num_rows() << std::endl;
}

int main() {
    read_parquet("/mnt/e/shared/parquet/MV_OUT_batch_11_80f37f0a-06e9-479e-9f88-f950302f3e34.parquet"); // Replace with your Parquet file path
    return 0;
}