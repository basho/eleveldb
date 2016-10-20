#include "CmpUtil.h"
#include "StringBuf.h"

#include "exceptionutils.h"

#include <sstream>
#include <cmath>
#include <iomanip>

#include <climits>

using namespace std;

using namespace eleveldb;

//=======================================================================
// Initialize static maps of conversion functions here
//=======================================================================

std::map<uint8_t, CONV_UINT8_FN(*)>  
CmpUtil::uint8ConvMap_ = CmpUtil::constructUint8Map();

std::map<uint8_t, CONV_INT64_FN(*)>  
CmpUtil::int64ConvMap_ = CmpUtil::constructInt64Map();

std::map<uint8_t, CONV_UINT64_FN(*)> 
CmpUtil::uint64ConvMap_ = CmpUtil::constructUint64Map();

std::map<uint8_t, CONV_DOUBLE_FN(*)> 
CmpUtil::doubleConvMap_ = CmpUtil::constructDoubleMap();

//=======================================================================
// A macro for declaring a template convert specialization
//=======================================================================

#define CONVERT_DECL(typeTo, typeFrom, cmpType, validation)             \
    namespace eleveldb {                                                \
        template<>                                                      \
        typeTo CmpUtil::convert<typeTo, typeFrom>(cmp_object_t* obj)    \
        {                                                               \
            typeFrom val=0;                                             \
            if(cmp_object_as_##cmpType(obj, &val)) {                    \
                validation;                                             \
            }                                                           \
                                                                        \
            ThrowRuntimeError("Object of type " << CmpUtil::typeStrOf(obj) \
                              << " can't be represented as a " << #typeTo); \
            return (typeTo)val;                                         \
        }                                                               \
    }\

//=======================================================================
// A macro for performing all possible conversions on a msgpack data
// type
//=======================================================================

#define CONVERT_OBJECT(typeTo)  \
    if(cmp_object_is_char(obj))                 \
        return convert<typeTo, int8_t>(obj);    \
                                                \
    if(cmp_object_is_short(obj))                \
        return convert<typeTo, int16_t>(obj);   \
                                                \
    if(cmp_object_is_int(obj))                  \
        return convert<typeTo, int32_t>(obj);   \
                                                \
    if(cmp_object_is_long(obj))                 \
        return convert<typeTo, int64_t>(obj);   \
                                                \
    if(cmp_object_is_uchar(obj))                \
        return convert<typeTo, uint8_t>(obj);   \
                                                \
    if(cmp_object_is_ushort(obj))               \
        return convert<typeTo, uint16_t>(obj);  \
                                                \
    if(cmp_object_is_uint(obj))                 \
        return convert<typeTo, uint32_t>(obj);  \
                                                \
    if(cmp_object_is_ulong(obj))                \
        return convert<typeTo, uint64_t>(obj);  \
                                                \
    if(cmp_object_is_float(obj))                \
        return convert<typeTo, float>(obj);     \
                                                \
    if(cmp_object_is_double(obj))               \
        return convert<typeTo, double>(obj);    \
                                                \
    if(cmp_object_is_bool(obj))                 \
        return convert<typeTo, bool>(obj);      \
                                                                        \
    ThrowRuntimeError("Object can't be converted to a #typeTo");        \
    return (typeTo) 0;\

//=======================================================================
// A macro for constructing a map of conversion functions to the
// specified type
//=======================================================================

#define CONSTRUCT_CONV_MAP(typeTo)                                      \
    /* char types */                                                    \
    convMap[CMP_TYPE_POSITIVE_FIXNUM] = CmpUtil::convert<typeTo, int8_t>; \
    convMap[CMP_TYPE_NEGATIVE_FIXNUM] = CmpUtil::convert<typeTo, int8_t>; \
    convMap[CMP_TYPE_SINT8]           = CmpUtil::convert<typeTo, int8_t>; \
    convMap[CMP_TYPE_UINT8]           = CmpUtil::convert<typeTo, int8_t>; \
                                                                        \
    /* short types */                                                   \
    convMap[CMP_TYPE_NEGATIVE_FIXNUM] = CmpUtil::convert<typeTo, int16_t>; \
    convMap[CMP_TYPE_SINT8]           = CmpUtil::convert<typeTo, int16_t>; \
    convMap[CMP_TYPE_SINT16]          = CmpUtil::convert<typeTo, int16_t>; \
                                                                        \
    /* int types */                                                     \
    convMap[CMP_TYPE_NEGATIVE_FIXNUM] = CmpUtil::convert<typeTo, int32_t>; \
    convMap[CMP_TYPE_SINT8]           = CmpUtil::convert<typeTo, int32_t>; \
    convMap[CMP_TYPE_SINT16]          = CmpUtil::convert<typeTo, int32_t>; \
    convMap[CMP_TYPE_SINT32]          = CmpUtil::convert<typeTo, int32_t>; \
                                                                        \
    /* long types */                                                    \
    convMap[CMP_TYPE_NEGATIVE_FIXNUM] = CmpUtil::convert<typeTo, int64_t>; \
    convMap[CMP_TYPE_SINT8]           = CmpUtil::convert<typeTo, int64_t>; \
    convMap[CMP_TYPE_SINT16]          = CmpUtil::convert<typeTo, int64_t>; \
    convMap[CMP_TYPE_SINT32]          = CmpUtil::convert<typeTo, int64_t>; \
    convMap[CMP_TYPE_SINT64]          = CmpUtil::convert<typeTo, int64_t>; \
                                                                        \
    /* uchar types */                                                   \
    convMap[CMP_TYPE_POSITIVE_FIXNUM] = CmpUtil::convert<typeTo, uint8_t>; \
    convMap[CMP_TYPE_UINT8]          = CmpUtil::convert<typeTo, uint8_t>; \
                                                                        \
    /* ushort types */                                                  \
    convMap[CMP_TYPE_POSITIVE_FIXNUM] = CmpUtil::convert<typeTo, uint16_t>; \
    convMap[CMP_TYPE_UINT8]           = CmpUtil::convert<typeTo, uint16_t>; \
    convMap[CMP_TYPE_UINT16]          = CmpUtil::convert<typeTo, uint16_t>; \
                                                                        \
    /* uint types */                                                    \
    convMap[CMP_TYPE_POSITIVE_FIXNUM] = CmpUtil::convert<typeTo, uint32_t>; \
    convMap[CMP_TYPE_UINT8]           = CmpUtil::convert<typeTo, uint32_t>; \
    convMap[CMP_TYPE_UINT16]          = CmpUtil::convert<typeTo, uint32_t>; \
    convMap[CMP_TYPE_UINT32]          = CmpUtil::convert<typeTo, uint32_t>; \
                                                                        \
    /* ulong types */                                                   \
    convMap[CMP_TYPE_POSITIVE_FIXNUM] = CmpUtil::convert<typeTo, uint64_t>; \
    convMap[CMP_TYPE_UINT8]           = CmpUtil::convert<typeTo, uint64_t>; \
    convMap[CMP_TYPE_UINT16]          = CmpUtil::convert<typeTo, uint64_t>; \
    convMap[CMP_TYPE_UINT32]          = CmpUtil::convert<typeTo, uint64_t>; \
    convMap[CMP_TYPE_UINT64]          = CmpUtil::convert<typeTo, uint64_t>; \
                                                                        \
    /* float types */                                                   \
    convMap[CMP_TYPE_FLOAT]          = CmpUtil::convert<typeTo, float>; \
                                                                        \
    /* double types */                                                  \
    convMap[CMP_TYPE_DOUBLE]         = CmpUtil::convert<typeTo, double>; \
                                                                        \
    /* bool types */                                                    \
    convMap[CMP_TYPE_BOOLEAN]        = CmpUtil::convert<typeTo, bool>;  \

/**.......................................................................
 * Constructor.
 */
CmpUtil::CmpUtil() {}

/**.......................................................................
 * Destructor.
 */
CmpUtil::~CmpUtil() {}

/**.......................................................................
 * Parse a map encoded as a msgpack object into component keys and
 * datatypes
 */
std::map<std::string, DataType::Type>
CmpUtil::parseMap(const char* data, size_t size)
{
    cmp_mem_access_t ma;
    cmp_object_t     map;
    uint32_t         map_size;
    cmp_ctx_t        cmp;

    std::map<std::string, DataType::Type> keyValMap;
    
    cmp_mem_access_ro_init(&cmp, &ma, data, size);

    if(!cmp_read_object(&cmp, &map))
      ThrowRuntimeError("Error reading msgpack map");

    if(!cmp_object_as_map(&map, &map_size))
      ThrowRuntimeError("Unable to parse data as a msgpack map");

    //------------------------------------------------------------
    // Iterate over the map, inspecting field names
    //------------------------------------------------------------

    StringBuf sBuf;
    for(unsigned int i=0; i < map_size; i++) {

        //------------------------------------------------------------
        // First read the field key
        //------------------------------------------------------------

        cmp_object_t key_obj;

        if(!cmp_read_object(&cmp, &key_obj) || !cmp_object_is_str(&key_obj))
          ThrowRuntimeError("Failed to read key");

        uint32_t len=0;
        if(!cmp_object_as_str(&key_obj, &len))
          ThrowRuntimeError("Error parsing object as a string");

        sBuf.resize(len+1);
        if(!cmp_object_to_str(&cmp, &key_obj, sBuf.getBuf(), len+1))
          ThrowRuntimeError("Error reading key string");

        std::string key(sBuf.getBuf());

        //------------------------------------------------------------
        // Next read the field value
        //------------------------------------------------------------

        cmp_object_t obj;

        if(!cmp_read_object(&cmp, &obj))
            ThrowRuntimeError("Unable to read value for field " << key);

        DataType::Type type = typeOf(&obj);
        keyValMap[key] = type;

        skipLastReadObject(&ma, &cmp, &obj);
    }

    return keyValMap;
}

/**.......................................................................
 * Return the type of this object
 */
DataType::Type CmpUtil::typeOf(cmp_object_t* obj)
{
    switch (obj->type) {
    case CMP_TYPE_POSITIVE_FIXNUM:
    case CMP_TYPE_UINT8:
        return DataType::UINT8;
        break;
    case CMP_TYPE_NEGATIVE_FIXNUM:
    case CMP_TYPE_SINT8:
        return DataType::INT8;
        break;
    case CMP_TYPE_FIXMAP:
    case CMP_TYPE_MAP16:
    case CMP_TYPE_MAP32:
        return DataType::MAP;
        break;
    case CMP_TYPE_FIXARRAY:
    case CMP_TYPE_ARRAY16:
    case CMP_TYPE_ARRAY32:
        return DataType::ARRAY;
        break;
    case CMP_TYPE_FIXSTR:
    case CMP_TYPE_STR8:
    case CMP_TYPE_STR16:
    case CMP_TYPE_STR32:
        return DataType::STRING;
        break;
    case CMP_TYPE_NIL:
        return DataType::NIL;
        break;
    case CMP_TYPE_BOOLEAN:
        return DataType::BOOL;
        break;
    case CMP_TYPE_BIN8:
    case CMP_TYPE_BIN16:
    case CMP_TYPE_BIN32:
        return DataType::BIN;
        break;
    case CMP_TYPE_FIXEXT1:
    case CMP_TYPE_FIXEXT2:
    case CMP_TYPE_FIXEXT4:
    case CMP_TYPE_FIXEXT8:
    case CMP_TYPE_FIXEXT16:
    case CMP_TYPE_EXT8:
    case CMP_TYPE_EXT16:
    case CMP_TYPE_EXT32:
        return DataType::EXT;
        break;
    case CMP_TYPE_FLOAT:
        return DataType::FLOAT;
        break;
    case CMP_TYPE_DOUBLE:
        return DataType::DOUBLE;
        break;
    case CMP_TYPE_UINT16:
        return DataType::UINT16;
        break;
    case CMP_TYPE_SINT16:
        return DataType::INT16;
        break;
    case CMP_TYPE_UINT32:
        return DataType::UINT32;
        break;
    case CMP_TYPE_SINT32:
        return DataType::INT32;
        break;
    case CMP_TYPE_UINT64:
        return DataType::UINT64;
        break;
    case CMP_TYPE_SINT64:
        return DataType::INT64;
        break;
    default:
        return DataType::UNKNOWN;
        break;
    }
}

/**.......................................................................
 * Return a string-ified version of the type of this object
 */
std::string CmpUtil::typeStrOf(cmp_object_t* obj)
{
    switch (obj->type) {
    case CMP_TYPE_POSITIVE_FIXNUM:
        return "CMP_TYPE_POSITIVE_FIXNUM";
        break;
    case CMP_TYPE_UINT8:
        return "CMP_TYPE_UINT8";
        break;
    case CMP_TYPE_NEGATIVE_FIXNUM:
        return "CMP_TYPE_NEGATIVE_FIXNUM";
        break;
    case CMP_TYPE_SINT8:
        return "CMP_TYPE_SINT8";
        break;
    case CMP_TYPE_FIXMAP:
        return "CMP_TYPE_FIXMAP";
        break;
    case CMP_TYPE_MAP16:
        return "CMP_TYPE_MAP16";
        break;
    case CMP_TYPE_MAP32:
        return "CMP_TYPE_MAP32";
        break;
    case CMP_TYPE_FIXARRAY:
        return "CMP_TYPE_FIXARRAY";
        break;
    case CMP_TYPE_ARRAY16:
        return "CMP_TYPE_ARRAY16";
        break;
    case CMP_TYPE_ARRAY32:
        return "CMP_TYPE_ARRAY32";
        break;
    case CMP_TYPE_FIXSTR:
        return "CMP_TYPE_FIXSTR";
        break;
    case CMP_TYPE_STR8:
        return "CMP_TYPE_STR8";
        break;
    case CMP_TYPE_STR16:
        return "CMP_TYPE_STR16";
        break;
    case CMP_TYPE_STR32:
        return "CMP_TYPE_STR32";
        break;
    case CMP_TYPE_NIL:
        return "CMP_TYPE_NIL";
        break;
    case CMP_TYPE_BOOLEAN:
        return "CMP_TYPE_BOOLEAN";
        break;
    case CMP_TYPE_BIN8:
        return "CMP_TYPE_BIN8";
        break;
    case CMP_TYPE_BIN16:
        return "CMP_TYPE_BIN16";
        break;
    case CMP_TYPE_BIN32:
        return "CMP_TYPE_BIN32";
        break;
    case CMP_TYPE_FIXEXT1:
        return "CMP_TYPE_FIXEXT1";
        break;
    case CMP_TYPE_FIXEXT2:
        return "CMP_TYPE_FIXEXT2";
        break;
    case CMP_TYPE_FIXEXT4:
        return "CMP_TYPE_FIXEXT4";
        break;
    case CMP_TYPE_FIXEXT8:
        return "CMP_TYPE_FIXEXT8";
        break;
    case CMP_TYPE_FIXEXT16:
        return "CMP_TYPE_FIXEXT16";
        break;
    case CMP_TYPE_EXT8:
        return "CMP_TYPE_EXT8";
        break;
    case CMP_TYPE_EXT16:
        return "CMP_TYPE_EXT16";
        break;
    case CMP_TYPE_EXT32:
        return "CMP_TYPE_EXT32";
        break;
    case CMP_TYPE_FLOAT:
        return "CMP_TYPE_FLOAT";
        break;
    case CMP_TYPE_DOUBLE:
        return "CMP_TYPE_DOUBLE";
        break;
    case CMP_TYPE_UINT16:
        return "CMP_TYPE_UINT16";
        break;
    case CMP_TYPE_SINT16:
        return "CMP_TYPE_SINT16";
        break;
    case CMP_TYPE_UINT32:
        return "CMP_TYPE_UINT32";
        break;
    case CMP_TYPE_SINT32:
        return "CMP_TYPE_SINT32";
        break;
    case CMP_TYPE_UINT64:
        return "CMP_TYPE_UINT64";
        break;
    case CMP_TYPE_SINT64:
        return "CMP_TYPE_SINT64";
        break;
    default:
        return "UNKNOWN";
        break;
    }
}

/**.......................................................................
 * Return the size, in bytes, of the passed object.  
 *
 * NB: After a call to this function, the memory pointer will be
 * advanced to the end of the current object.
 */
size_t CmpUtil::dataSizeOf(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* obj)
{
    switch (obj->type) {
    case CMP_TYPE_POSITIVE_FIXNUM:
    case CMP_TYPE_NEGATIVE_FIXNUM:
    case CMP_TYPE_UINT8:
    case CMP_TYPE_SINT8:
        return sizeof(int8_t);
        break;
    case CMP_TYPE_FIXMAP:
    case CMP_TYPE_MAP16:
    case CMP_TYPE_MAP32:
        return mapSize(ma, cmp, obj);
        break;
    case CMP_TYPE_FIXARRAY:
    case CMP_TYPE_ARRAY16:
    case CMP_TYPE_ARRAY32:
        return arraySize(ma, cmp, obj);
        break;
    case CMP_TYPE_FIXSTR:
    case CMP_TYPE_STR8:
    case CMP_TYPE_STR16:
    case CMP_TYPE_STR32:
        return stringSize(ma, cmp, obj);
        break;

    // These types are markers only (no data)

    case CMP_TYPE_NIL:
        return 0;
        break;
    case CMP_TYPE_BOOLEAN:
        return 0;
        break;
    case CMP_TYPE_BIN8:
    case CMP_TYPE_BIN16:
    case CMP_TYPE_BIN32:
        return binarySize(ma, cmp, obj);
        break;
    case CMP_TYPE_FIXEXT1:
    case CMP_TYPE_FIXEXT2:
    case CMP_TYPE_FIXEXT4:
    case CMP_TYPE_FIXEXT8:
    case CMP_TYPE_FIXEXT16:
    case CMP_TYPE_EXT8:
    case CMP_TYPE_EXT16:
    case CMP_TYPE_EXT32:
        ThrowRuntimeError("Unhandled type: EXT");
        break;
    case CMP_TYPE_FLOAT:
        return sizeof(float);
        break;
    case CMP_TYPE_DOUBLE:
        return sizeof(double);
        break;
    case CMP_TYPE_UINT16:
        return sizeof(uint16_t);
        break;
    case CMP_TYPE_SINT16:
        return sizeof(int16_t);
        break;
    case CMP_TYPE_UINT32:
        return sizeof(uint32_t);
        break;
    case CMP_TYPE_SINT32:
        return sizeof(int32_t);
        break;
    case CMP_TYPE_UINT64:
        return sizeof(uint64_t);
        break;
    case CMP_TYPE_SINT64:
        return sizeof(int64_t);
        break;
    default:
        ThrowRuntimeError("Can't determine a size for an unsupported type");
        break;
    }
}

/**.......................................................................
 * Return the message-pack marker size
 */
size_t CmpUtil::markerSize()
{
    return sizeof(uint8_t);
}

/**.......................................................................
 * Return the size of any prefix data pre-pended to this object's data
 */
size_t CmpUtil::prefixSizeOf(cmp_object_t* obj)
{
    switch (obj->type) {
    case CMP_TYPE_BIN8:
    case CMP_TYPE_STR8:
        return sizeof(uint8_t);
        break;
    case CMP_TYPE_ARRAY16:
    case CMP_TYPE_BIN16:
    case CMP_TYPE_MAP16:
    case CMP_TYPE_STR16:
        return sizeof(uint16_t);
        break;
    case CMP_TYPE_ARRAY32:
    case CMP_TYPE_BIN32:
    case CMP_TYPE_MAP32:
    case CMP_TYPE_STR32:
        return sizeof(uint32_t);
        break;
    case CMP_TYPE_FIXEXT1:
    case CMP_TYPE_FIXEXT2:
    case CMP_TYPE_FIXEXT4:
    case CMP_TYPE_FIXEXT8:
    case CMP_TYPE_FIXEXT16:
    case CMP_TYPE_EXT8:
    case CMP_TYPE_EXT16:
    case CMP_TYPE_EXT32:
        ThrowRuntimeError("Unhandled type: EXT");
        break;
    default:
        return 0;
        break;
    }
}

/**.......................................................................
 * Return the size of a map object
 */
size_t CmpUtil::mapSize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* map)
{
    size_t size = 0;

    uint32_t map_size=0;
    if(!cmp_object_as_map(map, &map_size))
      ThrowRuntimeError("Unable to parse data as a msgpack map");

    //------------------------------------------------------------
    // Iterate over the map, which consists of pairs of objects
    //------------------------------------------------------------

    for(unsigned int i=0; i < map_size; i++) {

        //------------------------------------------------------------
        // Read each object in the pair
        //------------------------------------------------------------

        cmp_object_t obj;

        if(cmp_read_object(cmp, &obj))
            ThrowRuntimeError("Failed to read first object in element " 
                              << i << "of the map");

        size += markerSize() + prefixSizeOf(&obj) + dataSizeOf(ma, cmp, &obj);

        if(!cmp_read_object(cmp, &obj))
            ThrowRuntimeError("Failed to read second object in element " 
                              << i << "of the map");
        
        size += markerSize() + prefixSizeOf(&obj) + dataSizeOf(ma, cmp, &obj);
    }

    return size;
}

/**.......................................................................
 * Return the size of an array object
 */
size_t CmpUtil::arraySize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* arr)
{
    size_t size = 0;

    uint32_t arr_size=0;
    if(!cmp_object_as_array(arr, &arr_size))
      ThrowRuntimeError("Unable to parse data as a msgpack arr");

    //------------------------------------------------------------
    // Iterate over the arr, which consists of pairs of objects
    //------------------------------------------------------------

    for(unsigned int i=0; i < arr_size; i++) {

        //------------------------------------------------------------
        // Read each object in turn
        //------------------------------------------------------------

        cmp_object_t obj;

        if(!cmp_read_object(cmp, &obj))
            ThrowRuntimeError("Failed to read element " << i << "of the array");
        
        // Array data consists of sequential data of the same type --
        // no embedded markers, as with a map

        size += prefixSizeOf(&obj) + dataSizeOf(ma, cmp, &obj);
    }

    return size;
}

/**.......................................................................
 * Return the size of a msgpack-encoded string.  The pointer is
 * repositioned at the end of the binary, so that all size operations
 * have uniform behavior
 */
size_t CmpUtil::stringSize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* str)
{
    uint32_t str_size=0;
    if(!cmp_object_as_str(str, &str_size))
      ThrowRuntimeError("Unable to parse data as a msgpack string");

    cmp_mem_access_set_pos(ma, cmp_mem_access_get_pos(ma) + str_size);

    return str_size;
}

/**.......................................................................
 * Return the size of a msgpack-encoded binary.  The pointer is
 * repositioned at the end of the binary, so that all size operations
 * have uniform behavior
 */
size_t CmpUtil::binarySize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* bin)
{
    uint32_t bin_size=0;
    if(!cmp_object_as_bin(bin, &bin_size))
      ThrowRuntimeError("Unable to parse data as a msgpack binary");

    cmp_mem_access_set_pos(ma, cmp_mem_access_get_pos(ma) + bin_size);

    return bin_size;
}

/**.......................................................................
 * Skip over the last read object.  Just reads the size, since that
 * operation has the side effect of moving the pointer to the end of
 * the object
 */
void CmpUtil::skipLastReadObject(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* obj)
{
    dataSizeOf(ma, cmp, obj);
}

/**.......................................................................
 * Print a map of keys + datatypes
 */
void CmpUtil::printMap(std::map<std::string, DataType::Type>& keyValMap)
{
    for(std::map<std::string, DataType::Type>::iterator iter = keyValMap.begin();
        iter != keyValMap.end(); iter++) {
        COUT(iter->first << " " << iter->second);
    }
}

/**.......................................................................
 * Return a pointer to the data for this object, and its size
 */
unsigned char* CmpUtil::getDataPtr(cmp_mem_access_t* ma, cmp_ctx_t* cmp,
                                   cmp_object_t* obj, size_t& size, bool includeMarker)
{
    //------------------------------------------------------------
    // Store the current location of the pointer before querying the
    // size, since that can in principle advance the pointer
    //------------------------------------------------------------

    unsigned char* ptr = 
        (unsigned char*)cmp_mem_access_get_ptr_at_pos(ma, cmp_mem_access_get_pos(ma));

    //------------------------------------------------------------
    // Now query the size, which will advance the pointer
    //------------------------------------------------------------

    size = dataSizeOf(ma, cmp, obj) + (includeMarker ? 1 : 0);

    return ptr - (includeMarker ? 1 : 0);
}

/**.......................................................................
 * Return true if this field is empty -- i.e., [], used to signify
 * null value from riak_kv
 */
bool CmpUtil::isEmptyList(cmp_object_t* obj)
{
    if(obj->type != CMP_TYPE_FIXARRAY)
        return false;

    uint32_t size=0;
    if(!cmp_object_as_array(obj, &size))
        return false;

    return size == 0;
}

//=======================================================================
// Templatized convert specializations
//=======================================================================

//------------------------------------------------------------
// Conversions to uint8_t
//------------------------------------------------------------

CONVERT_DECL(uint8_t, bool, bool,
             return val;
    );

CONVERT_DECL(uint8_t, uint8_t, uchar, 
             return val;
    );

CONVERT_DECL(uint8_t, int8_t, char, 
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint8_t, int16_t, short, 
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint16_t, ushort, 
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, int32_t, int,
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint32_t, uint,
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, int64_t, long,
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint64_t, ulong,
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, float, float,
             if(val >= 0.0 && val <= (float)UCHAR_MAX && !(fabs(val - (uint8_t)val) > 0.0))
                 return val;
    );

CONVERT_DECL(uint8_t, double, double,
             if(val >= 0.0 && val <= (double)UCHAR_MAX && !(fabs(val - (uint8_t)val) > 0.0))
                 return val;
    );

//------------------------------------------------------------
// Conversions to int64_t
//------------------------------------------------------------

CONVERT_DECL(int64_t, bool, bool,
             return val;
    );

CONVERT_DECL(int64_t, uint8_t, uchar, 
             return val;
    );

CONVERT_DECL(int64_t, int8_t, char, 
             return val;
    );

CONVERT_DECL(int64_t, int16_t, short, 
             return val;
    );

CONVERT_DECL(int64_t, uint16_t, ushort, 
             return val;
    );

CONVERT_DECL(int64_t, int32_t, int,
             return val;
    );

CONVERT_DECL(int64_t, uint32_t, uint,
             return val;
    );

CONVERT_DECL(int64_t, int64_t, long,
             return val;
    );

CONVERT_DECL(int64_t, uint64_t, ulong,
             if(val <= LLONG_MAX)
                 return val;
    );

CONVERT_DECL(int64_t, float, float,
             if(val <= (float)LLONG_MAX && val >= (float)LLONG_MIN && !(fabs(val - (int64_t)val) > 0.0))
                 return val;
    );

CONVERT_DECL(int64_t, double, double,
             if(val <= (double)LLONG_MAX && val >= (double)LLONG_MIN && !(fabs(val - (int64_t)val) > 0.0))
                 return val;
    );


//------------------------------------------------------------
// Conversions to uint64_t
//------------------------------------------------------------

CONVERT_DECL(uint64_t, bool, bool,
             return val;
    );

CONVERT_DECL(uint64_t, uint8_t, uchar, 
             return val;
    );

CONVERT_DECL(uint64_t, int8_t, char, 
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, int16_t, short, 
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, uint16_t, ushort, 
             return val;
    );

CONVERT_DECL(uint64_t, int32_t, int,
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, uint32_t, uint,
             return val;
    );

CONVERT_DECL(uint64_t, int64_t, long,
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, uint64_t, ulong,
             return val;
    );

CONVERT_DECL(uint64_t, float, float,
             if(val >= 0.0 && val <= (float)ULONG_MAX && !(fabs(val - (uint64_t)val) > 0.0))
                 return val;
    );

CONVERT_DECL(uint64_t, double, double,
             if(val >= 0.0 && val <= (double)ULONG_MAX && !(fabs(val - (uint64_t)val) > 0.0))
                 return val;
    );

//------------------------------------------------------------
// Conversions to double
//------------------------------------------------------------

CONVERT_DECL(double, bool, bool,
             return val;
    );

CONVERT_DECL(double, uint8_t, uchar, 
             return val;
    );

CONVERT_DECL(double, int8_t, char, 
             return val;
    );

CONVERT_DECL(double, int16_t, short, 
             return val;
    );

CONVERT_DECL(double, uint16_t, ushort, 
             return val;
    );

CONVERT_DECL(double, int32_t, int,
             return val;
    );

CONVERT_DECL(double, uint32_t, uint,
             return val;
    );

CONVERT_DECL(double, int64_t, long,
             return val;
    );

CONVERT_DECL(double, uint64_t, ulong,
             return val;
    );

CONVERT_DECL(double, float, float,
             return val;
    );

CONVERT_DECL(double, double, double,
                 return val;
    );

uint8_t CmpUtil::objectToUint8(cmp_object_t* obj)
{
    if(CmpUtil::uint8ConvMap_.find(obj->type) != CmpUtil::uint8ConvMap_.end())
        return CmpUtil::uint8ConvMap_[obj->type](obj);
    else 
        ThrowRuntimeError("Object of type " << typeStrOf(obj) << " can't be converted to a uint8_t type");
    return 0;
}

int64_t CmpUtil::objectToInt64(cmp_object_t* obj)
{
    if(CmpUtil::int64ConvMap_.find(obj->type) != CmpUtil::int64ConvMap_.end())
        return CmpUtil::int64ConvMap_[obj->type](obj);
    else 
        ThrowRuntimeError("Object of type " << typeStrOf(obj) << " can't be converted to an int64_t type");
    return 0;
}

uint64_t CmpUtil::objectToUint64(cmp_object_t* obj)
{
    if(CmpUtil::uint64ConvMap_.find(obj->type) != CmpUtil::uint64ConvMap_.end())
        return CmpUtil::uint64ConvMap_[obj->type](obj);
    else 
        ThrowRuntimeError("Object of type " << typeStrOf(obj) << " can't be converted to a uint64_t type");
    return 0;
}

double CmpUtil::objectToDouble(cmp_object_t* obj)
{
    if(CmpUtil::doubleConvMap_.find(obj->type) != CmpUtil::doubleConvMap_.end())
        return CmpUtil::doubleConvMap_[obj->type](obj);
    else 
        ThrowRuntimeError("Object of type " << typeStrOf(obj) << " can't be converted to a double type");
    return 0.0;
}

std::map<uint8_t, CONV_UINT8_FN(*)>  CmpUtil::constructUint8Map()
{
    std::map<uint8_t, CONV_UINT8_FN(*)> convMap;
    CONSTRUCT_CONV_MAP(uint8_t);
    return convMap;
}

std::map<uint8_t, CONV_INT64_FN(*)>  CmpUtil::constructInt64Map()
{
    std::map<uint8_t, CONV_INT64_FN(*)> convMap;
    CONSTRUCT_CONV_MAP(int64_t);
    return convMap;
}

std::map<uint8_t, CONV_UINT64_FN(*)>  CmpUtil::constructUint64Map()
{
    std::map<uint8_t, CONV_UINT64_FN(*)> convMap;
    CONSTRUCT_CONV_MAP(uint64_t);
    return convMap;
}

std::map<uint8_t, CONV_DOUBLE_FN(*)>  CmpUtil::constructDoubleMap()
{
    std::map<uint8_t, CONV_DOUBLE_FN(*)> convMap;
    CONSTRUCT_CONV_MAP(double);
    return convMap;
}


