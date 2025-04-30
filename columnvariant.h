using SingleColumn = std::variant<std::vector<std::string>*, std::vector<double>*, std::vector<int64_t>*>;
using NamedColumns = std::vector<std::pair<std::string, SingleColumn>>;
