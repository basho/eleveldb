#include <string>
#include <set>

#include "filter.h"
#include "cmp.h"
#include "cmp_mem_access.h"

#ifndef extractor_h
#define extractor_h

class Extractor {
    std::set<std::string> expr_fields;
    cmp_ctx_t cmp;

public:
    Extractor() {}

    void add_field(std::string field);

    void extract(const char *data, size_t size, ExpressionNode<bool>* root);
    void set_int_val(ExpressionNode<bool>* root, const std::string& key, int64_t val);
};



#endif
