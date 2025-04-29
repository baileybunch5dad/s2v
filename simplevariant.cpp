#include <variant>
#include <vector>
#include <string>
#include <stdexcept>
#include <iostream>


std::variant<std::vector<std::string>, std::vector<double>> process_data(int choice) {
    if (choice == 1) {
        return std::vector<std::string>{"apple", "banana", "cherry"};
    } else if (choice == 2) {
        return std::vector<double>{1.1, 2.2, 3.3};
    } else {
      throw std::runtime_error("Invalid choice");
    }
}

int main() {
    std::variant<std::vector<std::string>, std::vector<double>> data;
    int choice;

    std::cout << "Enter 1 for strings, 2 for doubles: ";
    std::cin >> choice;

    try {
      data = process_data(choice);
    } catch (const std::runtime_error& error) {
      std::cerr << error.what() << std::endl;
      return 1;
    }
    

    if (std::holds_alternative<std::vector<std::string>>(data)) {
        std::vector<std::string> str_vec = std::get<std::vector<std::string>>(data);
        std::cout << "Strings: ";
        for (const auto& str : str_vec) {
            std::cout << str << " ";
        }
        std::cout << std::endl;
    } else if (std::holds_alternative<std::vector<double>>(data)) {
        std::vector<double> double_vec = std::get<std::vector<double>>(data);
        std::cout << "Doubles: ";
        for (const auto& num : double_vec) {
            std::cout << num << " ";
        }
        std::cout << std::endl;
    }

    return 0;
}