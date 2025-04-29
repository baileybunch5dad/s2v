#include <iostream>
#include <pqxx/pqxx>
// # sudo yum install libpqxx
int main() {
    try {
        // Establish a connection to the database
        pqxx::connection conn("dbname=your_db user=your_user password=your_password host=your_host port=5432");
        if (!conn.is_open()) {
            std::cerr << "Failed to connect to the database" << std::endl;
            return 1;
        }
        std::cout << "Connected to the database successfully!" << std::endl;

        // Start a transaction
        pqxx::work txn(conn);

        // Define a query to fetch data in chunks
        std::string query = "SELECT * FROM your_table";

        // Execute the query and create a cursor for large datasets
        pqxx::result result = txn.exec(query);

        // Iterate through the rows of the result set
        std::cout << "Iterating through the table rows..." << std::endl;
        for (const auto &row : result) {
            // Access columns by index or column name
            std::cout << "Column 1: " << row[0].c_str() << ", Column 2: " << row[1].c_str() << std::endl;
        }

        // Commit the transaction
        txn.commit();

        // Close the connection
        conn.disconnect();
        std::cout << "Connection closed successfully!" << std::endl;

    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}