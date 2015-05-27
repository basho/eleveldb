#include "extractor.h"

#include "cmp.h"
#include "cmp_mem_access.h"

void Extractor::add_field(std::string field) {
    expr_fields.insert(field);
}

void Extractor::extract(const std::string& data, ExpressionNode<bool>* root) {
    root->clear();
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
            cmp_object_t obj;
            cmp_read_object(&cmp, &obj);
            switch (obj.type) {
                case CMP_TYPE_NIL:
                    // Don't need to do anything for nil as the expression is already cleared
                    break;
                case CMP_TYPE_DOUBLE:
                     root->set_value(key, (void *)(&obj.as.dbl));
                     break;
                case CMP_TYPE_SINT64:
                     set_int_val(root, key, obj.as.s64);
                     break;
                case CMP_TYPE_POSITIVE_FIXNUM:
                case CMP_TYPE_UINT8:
                     set_int_val(root, key, obj.as.u8);
                     break;
                case CMP_TYPE_UINT16:
                     set_int_val(root, key, obj.as.u16);
                     break;
            }
        }
    }
}

void Extractor::set_int_val(ExpressionNode<bool>* root, const std::string& key, int64_t val) {
    root->set_value(key, (void*)val);
}


