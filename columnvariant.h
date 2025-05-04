#ifndef SINGLECOL
#define SINGLECOL
using DynamicVector = std::variant<std::vector<std::string>, std::vector<double>, std::vector<int64_t>, std::vector<int32_t>>;
struct SingleColumn
{
    std::string name;
    std::string type;
    DynamicVector values;

    // SingleColumn(const std::string &nm, const std::string &tp, const DynamicVector &vls)
    //     : name(nm), type(tp), values(vls) {}
};
using NamedColumns = std::vector<SingleColumn>;
#endif