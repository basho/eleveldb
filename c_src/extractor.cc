#include "CmpUtil.h"
#include "ErlUtil.h"
#include "StringBuf.h"

#include "cmp.h"
#include "cmp_mem_access.h"
#include "exceptionutils.h"
#include "extractor.h"
#include "filter_parser.h"
#include "workitems.h"

#include <arpa/inet.h>
#include <inttypes.h>

#define MSGPACK_MAGIC 2

using namespace eleveldb;

//=======================================================================
// Macro to unpack a value of type 'cType' from msgpack, potentially
// cast it to type 'castType', and set it in an expression node
//=======================================================================

#define CMP_GET_SET(cType, cmpType, castType, dataType) {       \
        cType val;                                              \
        castType castVal;                                       \
        if(cmp_object_as_##cmpType(&obj, &val)) {               \
            castVal = (castType)val;                            \
            root->set_value(key, (void*)&castVal, dataType);    \
        } else {                                                \
            ThrowRuntimeError("Error extracting value");        \
        }                                                       \
    }

//=======================================================================
// Methods of Extractor base class
//=======================================================================

Extractor::Extractor() 
{
    typesParsed_    = false;
}

Extractor::~Extractor() {}

/**.......................................................................
 * Add a field to the map of expression fields.  If a type was
 * specified as ANY with the filter, store the original specified type
 * in the map.  Else store the (possibly converted to a different
 * supported type) inferred type from the data structure.
 *
 * This will be used later to distinguish binaries that were specified
 * as binaries from opaque items that are treated as binaries because
 * we don't know what they are (type 'any').
 */
void Extractor::add_field(std::string field) 
{
    if(expr_field_specs_.find(field) != expr_field_specs_.end()) {

        if(expr_field_specs_[field] == DataType::ANY) {
            expr_fields_[field] = expr_field_specs_[field];
        } else {
            expr_fields_[field] = cTypeOf(field);
        }

    } else {
        expr_fields_[field] = cTypeOf(field);
    }
}

/**.......................................................................
 * Convert from a Time-Series type-specifier to a DataType enum
 */
DataType::Type Extractor::tsAtomToType(std::string tsAtom, bool throwIfInvalid)
{
    DataType::Type type = DataType::UNKNOWN;

    // Used to be 'binary', now it's 'varchar'

    if(tsAtom == "varchar") {
        type = DataType::BIN;

        // Used to be 'integer', now it's 'sint64'

    } else if(tsAtom == "sint64") {
        type = DataType::INT64;

        // Used to be 'float', now it's 'double'

    } else if(tsAtom == "double") {
        type = DataType::DOUBLE;

    } else if(tsAtom == "boolean") {
        type = DataType::BOOL;
    } else if(tsAtom == "timestamp") {
        type = DataType::TIMESTAMP;
    } else if(tsAtom == "any") {
        type = DataType::ANY;
    } else {
        if(throwIfInvalid) {
            ThrowRuntimeError("Unsupported data type: '" << tsAtom << "'");
        }
    }

    return type;
}

/**.......................................................................
 * Convert from a Time-Series type-specifier to a supported C-style DataType
 */
DataType::Type Extractor::tsAtomToCtype(std::string tsAtom, bool throwIfInvalid)
{
    return convertToSupportedCType(tsAtomToType(tsAtom, throwIfInvalid));
}

/**.......................................................................
 * Return the data type of the requested field.
 */
DataType::Type Extractor::cTypeOf(std::string fieldName)
{
    //------------------------------------------------------------
    // If we haven't already parsed a value, we don't know what type
    // this field is
    //------------------------------------------------------------

    if(!typesParsed_)
        return DataType::UNKNOWN;

    //------------------------------------------------------------
    // If the field wasn't found, we don't know what type this field is
    //------------------------------------------------------------

    if(field_types_.find(fieldName) == field_types_.end())
        return DataType::UNKNOWN;

    //------------------------------------------------------------
    // Else retrieve the stored type, potentially converting to a
    // supported type if this is not a native type we support
    //------------------------------------------------------------

    DataType::Type type = field_types_[fieldName];
    return convertToSupportedCType(type);
}

/**.......................................................................
 * Convert a field of type 'type' to one of the C-types we support
 */
DataType::Type Extractor::convertToSupportedCType(DataType::Type type)
{
    //------------------------------------------------------------
    // Else try to upcast to a supported type.  
    //------------------------------------------------------------
    
    switch (type) {

        //------------------------------------------------------------
        // Basic types are supported
        //------------------------------------------------------------

    case DataType::DOUBLE:
    case DataType::STRING:
    case DataType::INT64:
        return type;
        break;

        //------------------------------------------------------------
        // While booleans are supported, we convert to uint8_t.
        // This is because the templatized parse_expression_node
        // method for bool is overloaded to evaluate 'boolean
        // expressions' (ie, comparators, like A <= B) rather than
        // 'expressions that return boolean values' (like A)
        //------------------------------------------------------------
        
    case DataType::BOOL:
        return DataType::UINT8;
        break;

        //------------------------------------------------------------
        // Binaries are treated as UCHAR_PTR types
        //------------------------------------------------------------

    case DataType::BIN:
        return DataType::UCHAR_PTR;
        break;

        //------------------------------------------------------------
        // Until clarified, timestamps are treated as uint64_t
        //------------------------------------------------------------

    case DataType::UINT64:
    case DataType::TIMESTAMP:
        return DataType::UINT64;
        break;

        //------------------------------------------------------------
        // Because the riak client layer supports only int64_t and
        // floating point for numeric types, we upcast all other
        // integers to int64_t
        //------------------------------------------------------------
        
    case DataType::UINT8:
    case DataType::UINT16:
    case DataType::UINT32:
    case DataType::CHAR:
    case DataType::INT8:
    case DataType::INT16:
    case DataType::INT32:
        return DataType::INT64;
        break;
        
        //------------------------------------------------------------
        // All other types are treated as opaque binaries
        //------------------------------------------------------------
        
    case DataType::ANY:
        return DataType::UCHAR_PTR;
        break;
    default:
        return DataType::UCHAR_PTR;
        break;
    }
}

void Extractor::printMap(std::map<std::string, DataType::Type>& keyTypeMap)
{
    for(std::map<std::string, DataType::Type>::iterator iter = keyTypeMap.begin();
        iter != keyTypeMap.end(); iter++) {
        COUT(iter->first << " " << convertToSupportedCType(iter->second));
    }
}

/**.......................................................................
 * Return the data type of the operands to a binary operator
 */
DataType::Type Extractor::cTypeOf(ErlNifEnv* env, ERL_NIF_TERM oper1, ERL_NIF_TERM oper2, bool throwIfInvalid)
{
    DataType::Type type1 = cTypeOf(env, oper1, throwIfInvalid);
    DataType::Type type2 = cTypeOf(env, oper2, throwIfInvalid);

    //------------------------------------------------------------
    // If both are const expressions, default to double comparisons
    //------------------------------------------------------------

    if(type1 == DataType::CONST && type2 == DataType::CONST)
        return DataType::DOUBLE;

    //------------------------------------------------------------
    // If either is unknown, return unknown -- we can't compare anything
    //------------------------------------------------------------

    else if(type1 == DataType::UNKNOWN || type2 == DataType::UNKNOWN)
        return DataType::UNKNOWN;

    //------------------------------------------------------------
    // If both are non-const, then they had better match for comparisons
    //------------------------------------------------------------

    else if(type1 != DataType::CONST && type2 != DataType::CONST && (type1 != type2))
        return DataType::UNKNOWN;

    //------------------------------------------------------------
    // Else return whichever type is non-const
    //------------------------------------------------------------

    else
        return type1 == DataType::CONST ? type2 : type1;
}

/**.......................................................................
 * Parse the field name out of a tuple, and return its type.
 *
 * Old-style tuples were of the form:
 *
 *    {field, "fieldname"} or {const, val}
 *
 * New-style tuples should be of the form:
 *
 *    {field, "fieldname", type} or {const, val}
 *
 */
DataType::Type Extractor::cTypeOf(ErlNifEnv* env, ERL_NIF_TERM tuple, bool throwIfInvalid)
{
    int arity=0;
    const ERL_NIF_TERM* op_args=0;

    //------------------------------------------------------------
    // ErlUtil::get methods will throw if the args are not the correct
    // type.  Capture this and return UNKNOWN if the tuple is
    // malformed
    //------------------------------------------------------------

    try {
        if(enif_get_tuple(env, tuple, &arity, &op_args)) {

            std::string op = ErlUtil::getAtom(env, op_args[0]);
            std::string fieldName;

            if(!(op == eleveldb::filter::FIELD_OP || op == eleveldb::filter::CONST_OP)) {
                if(throwIfInvalid)
                    ThrowRuntimeError("Invalid operand type: '" << op << "' while parsing expression: '" 
                                      << ErlUtil::formatTerm(env, tuple) << "'");
            }

            if(op == eleveldb::filter::FIELD_OP)
                fieldName = ErlUtil::getBinaryAsString(env, op_args[1]);
            
            //------------------------------------------------------------
            // Check 2-tuples
            //------------------------------------------------------------

            if(arity == 2) {
                
                // If this is a constant expression, we defer to the field value
                // against which we will be comparing it
                
                if(op == eleveldb::filter::CONST_OP)
                    return DataType::CONST;
                
                // Else a field -- parse the field name, and return the type of
                // the datum for that field
                
                if(op == eleveldb::filter::FIELD_OP)
                    return cTypeOf(fieldName);

            //------------------------------------------------------------
            // Check 3-tuples
            //------------------------------------------------------------

            } else if(arity == 3) {

                // If this is a constant expression, and a type has
                // been specified, we still defer to the field value
                // against which we will be comparing it
                
                if(op == eleveldb::filter::CONST_OP) {
                    return DataType::CONST;
                } else {
                    std::string type = ErlUtil::getAtom(env, op_args[2]);
                    
                    // Store the type as-specified
                    
                    expr_field_specs_[fieldName] = tsAtomToType(type, throwIfInvalid);
                    
                    // Overwrite any inferences we may have made from parsing the data
                    
                    DataType::Type specType = tsAtomToCtype(type, throwIfInvalid);

                    field_types_[fieldName] = specType;

                    // NB: Now that we are not inferring data types
                    // from the decoded data, set expr_fields_ to the
                    // explicit type, for use during data extraction

                    expr_fields_[fieldName] = specType;

                    // And return the type
                    
                    return tsAtomToCtype(type, throwIfInvalid);
                }
            }
        }

        if(throwIfInvalid)
            ThrowRuntimeError("Invalid field or const specifier: " << ErlUtil::formatTerm(env, tuple));

    } catch(std::runtime_error& err) {
        if(throwIfInvalid)
            throw err;
    }

    return DataType::UNKNOWN;
}

/**.......................................................................
 * Given the start of a key data binary, return the start and size of the
 * contents portion of an encoded riak object
 */
bool Extractor::riakObjectContentsCanBeParsed(const char* data, size_t size)
{
    const char* ptr = data;

    //------------------------------------------------------------
    // Skip the magic number and version
    //------------------------------------------------------------

    unsigned char magic    = (*ptr++);
    unsigned char vers     = (*ptr++);

    if(!(magic == 53 && vers == 1))
        return false;

    //------------------------------------------------------------
    // Skip the vclock len and vclock contents
    //------------------------------------------------------------

    unsigned int vClockLen = ntohl(*((unsigned int*)ptr));
    ptr += 4;
    ptr += vClockLen;

    //------------------------------------------------------------
    // Skip the sibling count
    //------------------------------------------------------------

    unsigned int sibCount =  ntohl(*((unsigned int*)ptr));
    ptr += 4;

    if(sibCount != 1)
        return false;

    //------------------------------------------------------------
    // Now we are on to the first (and only) sibling.  Skip the length of
    // the data contents for this sibling
    //------------------------------------------------------------

    ptr += 4;

    //------------------------------------------------------------
    // The next byte should now be the msgpack magic number (2).
    // Check that it is
    //------------------------------------------------------------

    unsigned char encMagic = (*ptr++);

    if(encMagic != MSGPACK_MAGIC)
        return false;

    return true;
}

/**.......................................................................
 * Given the start of a key data binary, return the start and size of the
 * contents portion of an encoded riak object
 */
void Extractor::getToRiakObjectContents(const char* data, size_t size, 
					const char** contentsPtr, size_t& contentsSize) 
{
    const char* ptr = data;

    //------------------------------------------------------------
    // Skip the magic number and version
    //------------------------------------------------------------

    unsigned char magic    = (*ptr++);
    unsigned char vers     = (*ptr++);

    if(!(magic == 53 && vers == 1))
        ThrowRuntimeError("Riak object contents can only be inspected for magic = 53 and v1 encoding");

    //------------------------------------------------------------
    // Skip the vclock len and vclock contents
    //------------------------------------------------------------

    unsigned int vClockLen = ntohl(*((unsigned int*)ptr));
    ptr += 4;
    ptr += vClockLen;

    //------------------------------------------------------------
    // Skip the sibling count
    //------------------------------------------------------------

    unsigned int sibCount =  ntohl(*((unsigned int*)ptr));
    ptr += 4;

    if(sibCount != 1)
        ThrowRuntimeError("Unexpected sibling count for time-series data: " << sibCount);

    //------------------------------------------------------------
    // Now we are on to the first (and only) sibling.  Get the length of
    // the data contents for this sibling
    //------------------------------------------------------------

    unsigned int valLen =  ntohl(*((unsigned int*)ptr));
    ptr += 4;

    //------------------------------------------------------------
    // The next byte should now be the msgpack magic number (2).
    // Check that it is
    //------------------------------------------------------------

    unsigned char encMagic = (*ptr++);

    if(encMagic != MSGPACK_MAGIC)
        ThrowRuntimeError("This is not msgpack-encoded data");

    //------------------------------------------------------------
    // Set the passed ptr pointing to the start of the contents for this
    // object, and set the returned length to be just the length of the
    // contents
    //------------------------------------------------------------

    *contentsPtr  = ptr;
     contentsSize = valLen;
}

//=======================================================================
// Methods of Msgpack extractor
//=======================================================================

ExtractorMsgpack::ExtractorMsgpack() {}
ExtractorMsgpack::~ExtractorMsgpack() {}

/**.......................................................................
 * Extract relevant fields from a riak object into the expression tree
 */
void ExtractorMsgpack::extractRiakObject(const char* data, size_t size, ExpressionNode<bool>* root) 
{
    const char* contentsPtr=0;
    size_t contentsSize=0;
    getToRiakObjectContents(data, size, &contentsPtr, contentsSize);
    extract(contentsPtr, contentsSize, root);
}

void ExtractorMsgpack::extract(const char* data, size_t size, ExpressionNode<bool>* root) 
{
    cmp_mem_access_t ma;
    cmp_object_t     map;
    uint32_t         map_size;

    root->clear();

    cmp_mem_access_ro_init(&cmp_, &ma, data, size);

    if(!cmp_read_object(&cmp_, &map))
        ThrowRuntimeError("Error reading msgpack map");

    if(!cmp_object_as_map(&map, &map_size))
        ThrowRuntimeError("Unable to parse data as a msgpack object");

    //------------------------------------------------------------
    // Iterate over the object, looking for fields
    //------------------------------------------------------------

    StringBuf sBuf;
    for(int i=0; i < map_size; i++) {

        //------------------------------------------------------------
        // First read the field key
        //------------------------------------------------------------

        cmp_object_t key_obj;

        if(!cmp_read_object(&cmp_, &key_obj) || !cmp_object_is_str(&key_obj))
            ThrowRuntimeError("Failed to read key");

	uint32_t len=0;
	if(!cmp_object_as_str(&key_obj, &len))
            ThrowRuntimeError("Error parsing object as a string");

        sBuf.resize(len+1);
        if(!cmp_object_to_str(&cmp_, &key_obj, sBuf.getBuf(), len+1))
            ThrowRuntimeError("Error reading key string");

        std::string key(sBuf.getBuf());

	//------------------------------------------------------------
	// Next read the field value
	//------------------------------------------------------------

        cmp_object_t obj;

        if(!cmp_read_object(&cmp_, &obj))
            ThrowRuntimeError("Unable to read value for field " << key);

	//------------------------------------------------------------
	// If this field is one of the fields in our filter, try to
	// process the value
	//------------------------------------------------------------

        if(expr_fields_.find(key) != expr_fields_.end()) {

            DataType::Type specType = expr_fields_[key];

            //------------------------------------------------------------
            // If there is no value for this field, do nothing
            //------------------------------------------------------------

            if(CmpUtil::isEmptyList(&obj)) {
                continue;

                //------------------------------------------------------------
                // Else set the appropriate value type for this field.
                //
                // We have to check the msgpack type every time
                // because msgpack encoding can in priciple convert
                // data of the same erlang type into different packed
                // types.  
                //
                // Thus we check the type specification for
                // this field, and convert from whichever type msgpack
                // chose to the appropriate type for our filter.
                //------------------------------------------------------------

            } else {

                try {

                    switch (specType) {
                    case DataType::UINT8:
                    {
                        //COUT("Converting " << CmpUtil::typeStrOf(&obj) << " to uint8");
                        uint8_t val = CmpUtil::objectToUint8(&obj);
                        root->set_value(key, (void*)&val, specType);
                    }
                    break;
                    case DataType::INT64:
                    {
                        //COUT("Converting " << CmpUtil::typeStrOf(&obj) << " to int64");
                        int64_t val = CmpUtil::objectToInt64(&obj);
                        root->set_value(key, (void*)&val, specType);
                    }
                    break;
                    case DataType::UINT64:
                    {
                        //COUT("Converting " << CmpUtil::typeStrOf(&obj) << " to uint64");
                        uint64_t val = CmpUtil::objectToUint64(&obj);
                        root->set_value(key, (void*)&val, DataType::UINT64);
                    }
                    break;
                    case DataType::DOUBLE:
                    {
                        //COUT("Converting " << CmpUtil::typeStrOf(&obj) << " to double");
                        double val = CmpUtil::objectToDouble(&obj);
                        root->set_value(key, (void*)&val, specType);
                    }
                    break;

                    //------------------------------------------------------------
                    // Type ANY means that the data for this field are
                    // opaque.  We don't try to interpret these, but treat
                    // them as binary blobs.  
                    //
                    // Because the value that they might be compared
                    // against in a filter could also be a msgpack-ed
                    // blob, we store the initial msgpack marker as well
                    // as the data contents.  
                    //
                    // This means that comparisons like:
                    //
                    //   {const, msgpack:pack([1,2,{<<"junk">>}], [{format, jsx}])}
                    //
                    // with 
                    // 
                    //   {field, "field1", any}
                    // 
                    // will work correctly if field1 contains the same blob that
                    // is msgpack formatted
                    //------------------------------------------------------------

                    case DataType::ANY:
                    {
                        //COUT("Converting " << CmpUtil::typeStrOf(&obj) << " to binary (any)");
                        setBinaryVal(root, key, &ma, &cmp_, &obj, true);
                    }
                    break;

                    //------------------------------------------------------------
                    // Other types that are identified as binary will be
                    // unpacked as binaries and the contents compared
                    //------------------------------------------------------------

                    default:
                    {
                        //COUT("Converting " << CmpUtil::typeStrOf(&obj) << " to binary");
                        setBinaryVal(root, key, &ma, &cmp_, &obj, false);
                    }
                    break;
                    }
                } catch(std::runtime_error& err) {
                    ThrowRuntimeError(err.what() 
                                      << std::endl << "While processing field: " << key);
                }
            }
            
            //------------------------------------------------------------
            // Skip over the last object if we didn't parse its value
            //------------------------------------------------------------

        } else {
            CmpUtil::skipLastReadObject(&ma, &cmp_, &obj);
        }
    }
}

/**.......................................................................
 * Set a string value decoded from msgpack as the expression value
 */
void ExtractorMsgpack::setStringVal(ExpressionNode<bool>* root, 
                                    std::string& key, cmp_object_t* obj)
{
    uint32_t len;
    if(!cmp_object_as_str(obj, &len))
        ThrowRuntimeError("Error parsing value as string");
    
    StringBuf val(len+1);
    char* buf = val.getBuf();
    if(!cmp_object_to_str(&cmp_, obj, buf, len+1))
        ThrowRuntimeError("Error extracting string value for key: " << key);
    
    root->set_value(key, (void*)&buf, DataType::UCHAR_PTR);
}

/**.......................................................................
 * Set a binary value decoded from msgpack as the expression value
 */
void ExtractorMsgpack::setBinaryVal(ExpressionNode<bool>* root, 
                                    std::string& key,
                                    cmp_mem_access_t* ma,
                                    cmp_ctx_t* cmp,
                                    cmp_object_t* obj,
                                    bool includeMarker)
{
    size_t size=0;
    unsigned char* ptr = CmpUtil::getDataPtr(ma, cmp, obj, size, includeMarker);
    root->set_value(key, &ptr, DataType::UCHAR_PTR, size);
}

/**.......................................................................
 * Extract relevant fields from the data array into the expression tree
 */
void ExtractorMsgpack::parseRiakObjectTypes(const char* data, size_t size) 
{
    const char* contentsPtr=0;
    size_t contentsSize=0;
    getToRiakObjectContents(data, size, &contentsPtr, contentsSize);

    parseTypes(contentsPtr, contentsSize);
}

/**.......................................................................
 * Read through a msgpack-encoded object, parsing the data types for
 * each field we encounter
 */
void ExtractorMsgpack::parseTypes(const char* data, size_t size) 
{
    field_types_ = CmpUtil::parseMap(data, size);

#if 0
    CmpUtil::printMap(field_types_);
    printMap(field_types_);
#endif

    typesParsed_ = true;
}

