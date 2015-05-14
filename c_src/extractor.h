#include <string>
#include <set>

#include "filter.h"
#include "cmp.h"
#include "cmp_mem_access.h"

#ifndef extractor_h
#define extractor_h

class missing_value : public std::bad_cast { };
class Extractor {
    std::set<std::string> expr_fields;
    cmp_ctx_t cmp;
    cmp_mem_access_t ma;

public:
    Extractor() {}

    void add_field(std::string field);

    void extract(const std::string& data, ExpressionNode<bool>* root);
};



#endif

