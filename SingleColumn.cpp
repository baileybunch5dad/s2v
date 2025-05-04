#include "SingleColumn.h"
    SingleColumn(const std::string &nm, const std::string &tp, const DynamicVector &vls)
        : name(nm), type(tp), values(vls) {}


                auto arrow_int32_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
                std::vector<int32_t> &int32Vec = std::get<std::vector<int32_t>>(dvp);
                for (int k = 0; k < arrowChunkLen; k++)
                {
                    int32_t arrow_val = arrow_int32_array->Value(k);
                    int32Vec[rowNum++] = arrow_val;
                }
