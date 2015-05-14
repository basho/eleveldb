#include "extractor.h"

#include "cmp.h"
#include "cmp_mem_access.h"

void Extractor::add_field(std::string field) {
    expr_fields.insert(field);
}

void Extractor::extract(const std::string& data, ExpressionNode<bool>* root) {
    uint32_t map_size;
    char key[255];
    double value;
    cmp_mem_access_ro_init(&cmp, &ma, data.data(), data.size());
    if (!cmp_read_map(&cmp, &map_size)) {
        return; // TODO: Error Handling
    }
    for (int i=0;i<map_size;i++) {
        uint32_t string_size = 255;
        cmp_read_str(&cmp, key, &string_size);
        cmp_read_double(&cmp, &value);
        if (this->expr_fields.find(key) != this->expr_fields.end()) {
            root->set_value(key, &value);
        }
    }
}

