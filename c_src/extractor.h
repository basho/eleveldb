#ifndef extractor_h
#define extractor_h

#include <stdint.h>
#include <map>
#include <string>
#include <set>

#include "filter.h"
#include "cmp.h"
#include "cmp_mem_access.h"

#include "erl_nif.h"

#include "DataType.h"
#include "Encoding.h"

//=======================================================================
// A base class for extracting data
//=======================================================================

class Extractor {
public:
    
    Extractor();
    virtual ~Extractor();
  
    void add_field(std::string field);
    
    // Get the DataType::Type corresponding to the ts atom

    eleveldb::DataType::Type tsAtomToType(std::string tsType, bool throwIfInvalid);

    // Convert from ts atom to supported C-type 

    eleveldb::DataType::Type tsAtomToCtype(std::string type, bool throwIfInvalid);

    eleveldb::DataType::Type cTypeOf(ErlNifEnv* env, ERL_NIF_TERM operand,
                                    bool throwIfInvalid);

    eleveldb::DataType::Type cTypeOf(ErlNifEnv* env, 
                                     ERL_NIF_TERM operand1, 
                                     ERL_NIF_TERM operand2,
                                     bool throwIfInvalid);

    eleveldb::DataType::Type cTypeOf(std::string field);
    
    virtual void parseTypes(const char *data, size_t size) = 0;
    virtual void parseRiakObjectTypes(const char *data, size_t size) = 0;
    virtual void extract(const char *data, size_t size, ExpressionNode<bool>* root) = 0;
    virtual void extractRiakObject(const char *data, size_t size, ExpressionNode<bool>* root) = 0;
    
    bool riakObjectContentsCanBeParsed(const char* data, size_t size);
    void getToRiakObjectContents(const char* data, size_t size, 
                                 const char** contentsPtr, size_t& contentsSize);
    
    DataType::Type convertToSupportedCType(DataType::Type type);
    void printMap(std::map<std::string, DataType::Type>& keyTypeMap);

    std::map<std::string, eleveldb::DataType::Type> expr_fields_;
    std::map<std::string, eleveldb::DataType::Type> expr_field_specs_;
    std::map<std::string, eleveldb::DataType::Type> field_types_;

    bool typesParsed_;
};

//=======================================================================
// A base class for extracting data encoded in msgpack format
//=======================================================================

class ExtractorMsgpack : public Extractor {
private:
    
    cmp_ctx_t cmp_;
    
public:

    ExtractorMsgpack();
    ~ExtractorMsgpack();
    
    void parseTypes(const char *data, size_t size);
    void parseRiakObjectTypes(const char *data, size_t size);
    void extract(const char *data, size_t size, ExpressionNode<bool>* root);
    void extractRiakObject(const char *data, size_t size, ExpressionNode<bool>* root);

    void setBinaryVal(ExpressionNode<bool>* root, char* key, 
                      cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* obj, bool includeMarker);

    void setStringVal(ExpressionNode<bool>* root, char* key, 
                      cmp_object_t* obj);
};

#endif
