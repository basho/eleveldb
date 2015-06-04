#include "extractor.h"

#include "cmp.h"
#include "cmp_mem_access.h"

#include <inttypes.h>
void Extractor::add_field(std::string field) {
    expr_fields.insert(field);
}

void Extractor::extract(const char *data, size_t size, ExpressionNode<bool>* root) {
    root->clear();
    cmp_mem_access_t ma;
    uint32_t map_size;
    char key[255];
    uint32_t key_length;
    cmp_mem_access_ro_init(&cmp, &ma, data, size);
    cmp_object_t map;
    cmp_read_object(&cmp, &map);
    if (!cmp_object_as_map(&map, &map_size)) {
        return; // TODO: Error Handling
    }
    for (int i=0;i<map_size;i++) {
        cmp_object_t key_obj;
        key_length = sizeof(key);
        if (!cmp_read_object(&cmp, &key_obj) || !cmp_object_is_str(&key_obj)) {
            printf("Failed to read key!\n");
            return;
        }
        if (!cmp_object_to_str(&cmp, &key_obj, key, sizeof(key))) {
            printf("Error reading key as string!\n");
            return;
        }
        cmp_object_t obj;
        cmp_read_object(&cmp, &obj);
        if (this->expr_fields.find(key) != this->expr_fields.end()) {
            if (cmp_object_is_nil(&obj)) {
                continue;
            } else if (cmp_object_is_long(&obj)) {
                int64_t val;
                if (cmp_object_as_long(&obj, &val)) {
                    set_int_val(root, key, val);
                }
            } else if (cmp_object_is_ulong(&obj)) {
                uint64_t val;
                if (cmp_object_as_ulong(&obj, &val)) {
                    set_int_val(root, key, (int64_t)val);
                }
            }
            else {
                printf("Unsupported value type found!\n");
            }
        }
    }
}

void Extractor::set_int_val(ExpressionNode<bool>* root, const std::string& key, int64_t val) {
    root->set_value(key, (void*)val);
}


