#ifndef SINGLECOL
#define SINGLECOL
using DynamicVector = std::variant<std::vector<std::string>, std::vector<double>, std::vector<int64_t>, std::vector<int32_t>>;
class SingleColumn
{
    public:
    std::string name;
    std::string type;
    DynamicVector values;

};
using NamedColumns = std::vector<SingleColumn>;
#endif