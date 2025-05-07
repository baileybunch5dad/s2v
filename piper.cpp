#include <iostream>
#include <thread>
#include <cstdio>
#include <memory>
#include <stdexcept>

void executePythonScript(const std::string& command) {
    std::string result;
    char buffer[128];

    // Open a pipe to run the Python script
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(command.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen failed!");
    }

    // Read the output of the Python script
    while (fgets(buffer, sizeof(buffer), pipe.get()) != nullptr) {
        std::cout << buffer;
    }

    // Print the output
    std::cout << "Python Script Output:\n" << result << std::endl;
}

int main() {
    std::string pythonScript = "python3 my_script.py";  // Adjust the command as needed

    // Launch Python script in a separate thread
    std::thread pythonThread(executePythonScript, pythonScript);

    // Continue with other tasks in main thread
    std::cout << "Python script is running in a separate thread...\n";

    // Wait for the Python thread to finish
    pythonThread.join();

    std::cout << "Python script execution completed.\n";
    return 0;
}