#include "extractor.h"

#include "cmp.h"
#include "cmp_mem_access.h"

void Extractor::add_field(std::string field) {
    expr_fields.insert(field);
}

void Extractor::extract(const std::string& data, ExpressionNode<bool>* root) {
    uint32_t map_size;
    char key[255];
    uint32_t key_length;
    cmp_mem_access_ro_init(&cmp, &ma, data.data(), data.size());
    if (!cmp_read_map(&cmp, &map_size)) {
        return; // TODO: Error Handling
    }
    for (int i=0;i<map_size;i++) {
        cmp_read_str(&cmp, key, &key_length);
        if (this->expr_fields.find(key) != this->expr_fields.end()) {
            extract_and_set_field(key, root);
        }
    }
}

void Extractor::extract_and_set_field(char *key, ExpressionNode<bool>* root) {
    void* value;
    extract_value(&value);
    root->set_value(key, &value);
}

void Extractor::extract_value(void *val) {
    cmp_object_t obj;

    if (!cmp_read_object(&cmp, &obj)) {
        switch (obj.type) {
            case CMP_TYPE_NIL:
                 val = (void*)0; // Ugh - C++ < 11, no nullptr
                 break;
            case CMP_TYPE_DOUBLE:
                 *(double*)val = obj.as.dbl;
                 break;
            case CMP_TYPE_SINT64:
                 *(uint64_t*)val = obj.as.s64;
                 break;
            }
    }
    else {
        // TODO: What's the right answer for dealing with errors here?
    }
}

