#include "DataType.h"
#include "EiUtil.h"

#include "exceptionutils.h"

#include <sstream>
#include <cmath>
#include <iomanip>

#include <climits>

using namespace std;

using namespace eleveldb;

#define THROW_ERR ThrowRuntimeError("Not a valid EI term")

#define FN_DEF(retType, name, body)             \
    retType EiUtil::name()                      \
    {                                           \
        checkBuf();                             \
        return name(buf_, &index_);             \
    }                                           \
                                                \
    retType EiUtil::name(char* buf, int* index) \
    {                                           \
        body;                                   \
    }
    
//=======================================================================
// Initialize static maps of conversion functions here
//=======================================================================

std::map<DataType::Type, EI_CONV_UINT8_FN(*)>  
EiUtil::uint8ConvMap_ = EiUtil::constructUint8Map();

std::map<DataType::Type, EI_CONV_INT64_FN(*)>  
EiUtil::int64ConvMap_ = EiUtil::constructInt64Map();

std::map<DataType::Type, EI_CONV_UINT64_FN(*)> 
EiUtil::uint64ConvMap_ = EiUtil::constructUint64Map();

std::map<DataType::Type, EI_CONV_DOUBLE_FN(*)> 
EiUtil::doubleConvMap_ = EiUtil::constructDoubleMap();

//=======================================================================
// A macro for declaring a template convert specialization
//=======================================================================

#define CONVERT_DECL(typeTo, typeFrom, getName, validation)             \
    namespace eleveldb {                                                \
        template<>                                                      \
        typeTo EiUtil::convert<typeTo, typeFrom>(char* buf, int* index) \
        {                                                               \
            typeFrom val;                                               \
            try {                                                       \
                val = getName(buf, index);                              \
                validation;                                             \
            } catch(...) {                                              \
                ThrowRuntimeError("Object of type " << typeOf(getType(buf, index), buf, index) << " typestr = " << EiUtil::typeStrOf(buf, index) \
                                  << " can't be represented as a " << #typeTo << " using " << #getName << " typeFrom = " << #typeFrom); \
            }                                                           \
            return (typeTo) val;                                        \
        }                                                               \
    }                                                                   \
                                                                        \
//=======================================================================
// A macro for constructing a map of conversion functions to the
// specified type
//=======================================================================

#define CONSTRUCT_EI_CONV_MAP(typeTo)                                   \
    /* int types */                                                     \
    convMap[DataType::INT64]  = EiUtil::convert<typeTo, int64_t>; \
                                                                        \
    /* float types */                                                   \
    convMap[DataType::DOUBLE] = EiUtil::convert<typeTo, double>; \
                                                                        \
    /* Boolean types */                                                 \
    convMap[DataType::BOOL]   = EiUtil::convert<typeTo, bool>;   
                                                                        
EiUtil::EiUtil()
{
    initialize(0, 0);
}

EiUtil::EiUtil(char* buf)
{
    initialize(buf, 0);
}

EiUtil::~EiUtil() {};

void EiUtil::initialize(char* buf, int index)
{
    buf_     = buf;
    index_   = index;
    version_ = -1;

    version_ = getVersion();
}

FN_DEF(int, getVersion,

       int version = -1;
       
       if(ei_decode_version(buf, index, &version))
           THROW_ERR;
       
       return version;
    )

FN_DEF(int, getTupleHeader,

       int arity = 0;
       
       if(ei_decode_tuple_header(buf, index, &arity))
           THROW_ERR;
       
       return arity;
    )

FN_DEF(int, getListHeader,

       int arity = 0;
       
       if(ei_decode_list_header(buf, index, &arity))
           THROW_ERR;
       
       return arity;
    )

FN_DEF(ei_term, decodeTerm,

       ei_term term;
       if(ei_decode_ei_term(buf, index, &term) < 0)
           THROW_ERR;
       
       return term;
    )

FN_DEF(std::string, printTerm,

       char* s=0;
       
       if(ei_s_print_term(&s, buf, index))
           THROW_ERR;
       
       std::string ret(s);
       
       if(s)
           free(s);
       
       return ret;
    )

/**.......................................................................
 * Return the type of this object
 */
FN_DEF(int, getType,
       int size=0;
       int type=0;
        
       if(ei_get_type(buf, index, &type, &size) < 0)
           THROW_ERR;
        
       return type;
    )
        
/**.......................................................................
 * Return the size of this object
 */
FN_DEF(int, getSize,
       
       int size=0;
       int type=0;
       
       if(ei_get_type(buf, index, &type, &size) < 0)
           THROW_ERR;
       
       return size;
    )

/**.......................................................................
 * Return true if this is an integer
 */
FN_DEF(bool, isInteger,
       return isInteger(getType(buf, index));
    )

bool EiUtil::isInteger(int type)
{
    switch(type) {
    case ERL_SMALL_INTEGER_EXT:
    case ERL_INTEGER_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a float
 */
FN_DEF(bool, isFloat,
       return isFloat(getType(buf, index));
    )

bool EiUtil::isFloat(int type)
{
    switch(type) {
    case ERL_FLOAT_EXT:
    case NEW_FLOAT_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is an atom
 */
FN_DEF(bool, isAtom,
       return isAtom(getType(buf, index));
    )

bool EiUtil::isAtom(int type)
{ 
    switch(type) {
    case ERL_ATOM_EXT:
    case ERL_SMALL_ATOM_EXT:
    case ERL_ATOM_UTF8_EXT:
    case ERL_SMALL_ATOM_UTF8_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a ref
 */
FN_DEF(bool, isRef,
       return isRef(getType(buf, index));
    )

bool EiUtil::isRef(int type)
{
    switch(type) {
    case ERL_REFERENCE_EXT:
    case ERL_NEW_REFERENCE_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a port
 */
FN_DEF(bool, isPort,
       return isPort(getType(buf, index));
    )

bool EiUtil::isPort(int type)
{
    switch(type) {
    case ERL_PORT_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a pid
 */
FN_DEF(bool, isPid,
       return isPid(getType(buf, index));
    )

bool EiUtil::isPid(int type)
{
    switch(type) {
    case ERL_PID_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is nil
 */
FN_DEF(bool, isNil,
       return isNil(getType(buf, index));
    )

bool EiUtil::isNil(int type)
{
    switch(type) {
    case ERL_NIL_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a tuple
 */
FN_DEF(bool, isTuple,
       return isTuple(getType(buf, index));
    )

bool EiUtil::isTuple(int type)
{
    switch(type) {
    case ERL_SMALL_TUPLE_EXT:
    case ERL_LARGE_TUPLE_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a string
 */
FN_DEF(bool, isString,
       return isString(getType(buf, index));
    )

bool EiUtil::isString(int type)
{
    switch(type) {
    case ERL_STRING_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a list
 */
FN_DEF(bool, isList,
       return isList(getType(buf, index));
    )

bool EiUtil::isList(int type)
{
    switch(type) {
    case ERL_LIST_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a binary
 */
FN_DEF(bool, isBinary,
       return isBinary(getType(buf, index));
    )

bool EiUtil::isBinary(int type)
{
    switch(type) {
    case ERL_BINARY_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a big
 */
FN_DEF(bool, isBig,
       return isBig(getType(buf, index));
    )

bool EiUtil::isBig(int type)
{
    switch(type) {
    case ERL_SMALL_BIG_EXT:
    case ERL_LARGE_BIG_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Return true if this is a fun
 */
FN_DEF(bool, isFun,
       return isFun(getType(buf, index));
    )

bool EiUtil::isFun(int type)
{
    switch(type) {
    case ERL_NEW_FUN_EXT:
    case ERL_FUN_EXT:
        return true;
        break;
    default:
        return false;
        break;
    }
}

/**.......................................................................
 * Get a string representation of the next type
 */
FN_DEF(std::string, typeStrOf,
       return typeStrOf(getType(buf, index));
    )

std::string EiUtil::typeStrOf(int type)
{
    switch(type) {
    case ERL_SMALL_INTEGER_EXT:
    case ERL_INTEGER_EXT:
        return "INT";
        break;
    case ERL_FLOAT_EXT:
    case NEW_FLOAT_EXT:
        return "FLOAT";
        break;
    case ERL_ATOM_EXT:
    case ERL_SMALL_ATOM_EXT:
    case ERL_ATOM_UTF8_EXT:
    case ERL_SMALL_ATOM_UTF8_EXT:
        return "ATOM";
        break;
    case ERL_REFERENCE_EXT:
    case ERL_NEW_REFERENCE_EXT:
        return "REF";
        break;
    case ERL_PORT_EXT:
        return "PORT";
        break;
    case ERL_PID_EXT:
        return "PID";
        break;
    case ERL_SMALL_TUPLE_EXT:
    case ERL_LARGE_TUPLE_EXT:
        return "TUPLE";
        break;
    case ERL_NIL_EXT:
        return "NIL";
        break;
    case ERL_STRING_EXT:
        return "STRING";
        break;
    case ERL_LIST_EXT:
        return "LIST";
        break;
    case ERL_BINARY_EXT:
        return "BINARY";
        break;
    case ERL_SMALL_BIG_EXT:
    case ERL_LARGE_BIG_EXT:
        return "BIG";
        break;
    case ERL_NEW_FUN_EXT:
    case ERL_FUN_EXT:
        return "FUN";
        break;
    default:
        return "UNKNOWN";
        break;
    }
}

/**.......................................................................
 * Get the next type
 */
FN_DEF(DataType::Type, typeOf,
       return typeOf(getType(buf, index), buf, index);
    )

DataType::Type EiUtil::typeOf(int type, char* buf, int* index)
{
    switch(type) {
    case ERL_SMALL_INTEGER_EXT:
    case ERL_INTEGER_EXT:
        return DataType::INT64;
        break;
    case ERL_FLOAT_EXT:
    case NEW_FLOAT_EXT:
        return DataType::DOUBLE;
        break;
    case ERL_ATOM_EXT:
    case ERL_SMALL_ATOM_EXT:
    case ERL_ATOM_UTF8_EXT:
    case ERL_SMALL_ATOM_UTF8_EXT:
    {
        if(buf == 0 || index == 0)
            THROW_ERR;

        int prev = *index;
        if(isBool(buf, index)) {
            *index = prev;
            return DataType::BOOL;
        } else {
            return DataType::STRING;
        }
    }
    break;
    case ERL_STRING_EXT:
        return DataType::STRING;
        break;
    case ERL_BINARY_EXT:
        return DataType::UCHAR_PTR;
        break;
    default:
        return DataType::UNKNOWN;
        break;
    }
}

FN_DEF(void, skipLastReadObject,

       int size = getSize(buf, index);

       // Skip is the size of the opcode (1), size of the size (4) and
       // size of the object itself

       *index += size + 4 + 1;
    )

//=======================================================================
// Format methods
//=======================================================================

/**.......................................................................
 * Format an encoded term for printing
 */
FN_DEF(std::string, formatTerm,

       int type = getType(buf, index);
       
       if(isAtom(type))
           return formatAtom(buf, index);
       
       if(isInteger(type))
           return formatInteger(buf, index);
       
       if(isFloat(type))
           return formatFloat(buf, index);
       
       if(isTuple(type)) 
           return formatTuple(buf, index);
       
       if(isBinary(type))
           return formatBinary(buf, index);
       
       if(isString(type))
           return formatString(buf, index);
       
       if(isList(type))
           return formatList(buf, index);
       
       if(isNil(type))
           return "[]";
       
       return "?";
    )

FN_DEF(std::string, formatAtom,
       
       std::ostringstream os;
       os << getAtom(buf, index);
       return os.str();
    )

FN_DEF(std::string, formatInteger,
       
       std::ostringstream os;
       os << getLong(buf, index);
       return os.str();
    )

FN_DEF(std::string, formatFloat,

       std::ostringstream os;
       os << getFloat(buf, index);
       return os.str();
    )

FN_DEF(std::string, formatTuple,

       int arity = getTupleHeader(buf, index);
       std::ostringstream os;

       os << "{";
       for(unsigned iCell=0; iCell < arity; iCell++) {
           os << formatTerm(buf, index);
           if(iCell < arity-1)
               os << ", ";
       }
       
       os << "}";
       
       return os.str();
    )

FN_DEF(std::string, formatList,

       int arity = getListHeader(buf, index);
       std::ostringstream os;

       os << "[";
       for(unsigned iCell=0; iCell < arity; iCell++) {
           os << formatTerm(buf, index);
           if(iCell < arity-1)
               os << ", ";
       }
       
       os << "]";
       
       return os.str();
    )

FN_DEF(std::string, formatBinary,

       std::ostringstream os;
       os << "<<";
       std::vector<unsigned char> bin = getBinary(buf, index);
       for(unsigned i=0; i < bin.size(); i++) {
           os << (int)bin[i];
           if(i < bin.size()-1)
               os << ", ";
       }
       os << ">>";
       
       return os.str();
    )

FN_DEF(std::string, formatString,

       std::ostringstream os;
       std::string str = getString(buf, index);
       os << "\"" << str << "\"";
       os << " (aka [";
       for(unsigned i=0; i < str.size(); i++) {
           os << (int)str[i];
           if(i < str.size()-1)
               os << ", ";
       }
       os << "])";
       
       return os.str();
    )

//=======================================================================
// Get methods
//=======================================================================

FN_DEF(std::string, getString,

       int size = getSize(buf, index);
       char str[size+1];

       if(ei_decode_string(buf, index, str) < 0)
           THROW_ERR;
       
       return str;
    )

FN_DEF(std::vector<unsigned char>, getBinary,

       int size = getSize(buf, index);
       std::vector<unsigned char> ret(size);
       
       long len = 0;
       if(ei_decode_binary(buf, index, (void*)&ret[0], &len))
           THROW_ERR;
       
       return ret;
    )

FN_DEF(std::string, getAtom,

       char str[MAXATOMLEN+1];

       if(ei_decode_atom(buf, index, str) < 0)
           THROW_ERR;
       
       return str;
    )

FN_DEF(bool, getBool,

       std::string atom = getAtom(buf, index);
       return atom == "false" || atom == "true";
    )

FN_DEF(bool, isBool,

       if(isAtom(buf, index)) {
           std::string atom = getAtom(buf, index);
           return atom == "false" || atom == "true";
       }
       
       return false;
    )

FN_DEF(double, getFloat,
       return getDouble(buf, index);
    )

FN_DEF(double, getDouble,

       double val;
       if(ei_decode_double(buf, index, &val) < 0)
           THROW_ERR;
       
       return val;
    )

FN_DEF(long, getLong,

       long val;
       if(ei_decode_long(buf, index, &val) < 0)
           THROW_ERR;

       return val;
    )

FN_DEF(unsigned long, getUlong,

       unsigned long val;
       if(ei_decode_ulong(buf, index, &val) < 0)
           THROW_ERR;
       
       return val;
    )

FN_DEF(int64_t, getInt64,

       long long val;
       if(ei_decode_longlong(buf, index, &val) < 0)
           THROW_ERR;

       return val;
    )

FN_DEF(uint64_t, getUint64,

       unsigned long long val;
       if(ei_decode_ulonglong(buf, index, &val) < 0)
           THROW_ERR;

       return val;
    )

/**.......................................................................
 * Parse a map encoded as a msgpack object into component keys and
 * datatypes
 */
std::map<std::string, DataType::Type> EiUtil::parseMap()
{
    checkBuf();
    return parseMap(buf_, &index_);
}

std::map<std::string, DataType::Type> EiUtil::parseMap(char* buf, int* index)
{
    std::map<std::string, DataType::Type> keyValMap;

    if(!isList(buf, index))
        ThrowRuntimeError("Binary data must contain a term_to_binary() formatted list");
    
    unsigned nVal = getListHeader(buf, index);
    
    for(unsigned int i=0; i < nVal; i++) {
        
        if(!isTuple(buf, index) || getTupleHeader(buf, index) != 2) {
            ThrowRuntimeError("List must consist of {field, val} tuples: ");
        }
        
        std::vector<unsigned char> bin = getBinary(buf, index);
        std::string str((char*)&bin[0]);
        
        keyValMap[str] = typeOf(buf, index);

        // Read past
        
        formatTerm(buf, index);
    }
    
    return keyValMap;
}

/**.......................................................................
 * Print a map of keys + datatypes
 */
void EiUtil::printMap(std::map<std::string, DataType::Type>& keyValMap)
{
    for(std::map<std::string, DataType::Type>::iterator iter = keyValMap.begin();
        iter != keyValMap.end(); iter++) {
        COUT(iter->first << " " << iter->second);
    }
}
                      
/**.......................................................................
 * Return a pointer to the data for the next object, and its size
 */
unsigned char* EiUtil::getDataPtr(char* buf, int* index, size_t& size, bool includeMarker)
{
    // If we are including the opcode and size prefix, the 'size' of
    // the data is the size returned by getSize(), plus the size of
    // the opcode and prefix (1 + 4)

    size = getSize(buf, index)  + (includeMarker ? 5 : 0);

    // If we are including the opcode and prefix, just return the
    // pointer at the current location.  Else advance by the opcode
    // and prefix length

    return (unsigned char*)(buf + *index + (includeMarker ? 0 : 5));
}
                                                 
void EiUtil::checkBuf() 
{
    if(!buf_)
        ThrowRuntimeError("No buffer has been set");
}

//=======================================================================
// Templatized convert specializations
//=======================================================================

//------------------------------------------------------------
// Conversions to uint8_t
//------------------------------------------------------------

CONVERT_DECL(uint8_t, bool, getBool,
             return val;
    );

CONVERT_DECL(uint8_t, uint8_t, getUlong, 
             return val;
    );

CONVERT_DECL(uint8_t, int8_t, getLong, 
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint8_t, int16_t, getLong, 
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint16_t, getUlong, 
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, int32_t, getLong,
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint32_t, getUlong,
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, int64_t, getInt64,
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint64_t, getUint64,
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, float, getDouble,
             if(val >= 0.0 && val <= (float)UCHAR_MAX && !(fabs(val - (uint8_t)val) > 0.0))
                 return val;
    );

CONVERT_DECL(uint8_t, double, getDouble,
             if(val >= 0.0 && val <= (double)UCHAR_MAX && !(fabs(val - (uint8_t)val) > 0.0))
                 return val;
    );

//------------------------------------------------------------
// Conversions to int64_t
//------------------------------------------------------------

CONVERT_DECL(int64_t, bool, getBool,
             return val;
    );

CONVERT_DECL(int64_t, uint8_t, getUlong, 
             return val;
    );

CONVERT_DECL(int64_t, int8_t, getLong, 
             return val;
    );

CONVERT_DECL(int64_t, int16_t, getLong, 
             return val;
    );

CONVERT_DECL(int64_t, uint16_t, getUlong, 
             return val;
    );

CONVERT_DECL(int64_t, int32_t, getLong,
             return val;
    );

CONVERT_DECL(int64_t, uint32_t, getUlong,
             return val;
    );

CONVERT_DECL(int64_t, int64_t, getInt64,
             return val;
    );

CONVERT_DECL(int64_t, uint64_t, getUint64,
             if(val <= LLONG_MAX)
                 return val;
    );

CONVERT_DECL(int64_t, float, getDouble,
             if(val <= (float)LLONG_MAX && val >= (float)LLONG_MIN && !(fabs(val - (int64_t)val) > 0.0))
                 return val;
    );

CONVERT_DECL(int64_t, double, getDouble,
             if(val <= (double)LLONG_MAX && val >= (double)LLONG_MIN && !(fabs(val - (int64_t)val) > 0.0))
                 return val;
    );


//------------------------------------------------------------
// Conversions to uint64_t
//------------------------------------------------------------

CONVERT_DECL(uint64_t, bool, getBool,
             return val;
    );

CONVERT_DECL(uint64_t, uint8_t, getUlong, 
             return val;
    );

CONVERT_DECL(uint64_t, int8_t, getLong, 
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, int16_t, getLong, 
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, uint16_t, getUlong, 
             return val;
    );

CONVERT_DECL(uint64_t, int32_t, getLong,
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, uint32_t, getUlong,
             return val;
    );

CONVERT_DECL(uint64_t, int64_t, getInt64,
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, uint64_t, getUint64,
             return val;
    );

CONVERT_DECL(uint64_t, float, getDouble,
             if(val >= 0.0 && val <= (float)ULONG_MAX && !(fabs(val - (uint64_t)val) > 0.0))
                 return val;
    );

CONVERT_DECL(uint64_t, double, getDouble,
             if(val >= 0.0 && val <= (double)ULONG_MAX && !(fabs(val - (uint64_t)val) > 0.0))
                 return val;
    );

//------------------------------------------------------------
// Conversions to double
//------------------------------------------------------------

CONVERT_DECL(double, bool, getBool,
             return val;
    );

CONVERT_DECL(double, uint8_t, getUlong, 
             return val;
    );

CONVERT_DECL(double, int8_t, getLong, 
             return val;
    );

CONVERT_DECL(double, int16_t, getLong, 
             return val;
    );

CONVERT_DECL(double, uint16_t, getUlong, 
             return val;
    );

CONVERT_DECL(double, int32_t, getLong,
             return val;
    );

CONVERT_DECL(double, uint32_t, getUlong,
             return val;
    );

CONVERT_DECL(double, int64_t, getInt64,
             return val;
    );

CONVERT_DECL(double, uint64_t, getUint64,
             return val;
    );

CONVERT_DECL(double, float, getDouble,
             return val;
    );

CONVERT_DECL(double, double, getDouble,
             return val;
    );

uint8_t EiUtil::objectToUint8(char* buf, int* index)
{
    DataType::Type type = typeOf(getType(buf, index), buf, index);
    if(EiUtil::uint8ConvMap_.find(type) != EiUtil::uint8ConvMap_.end())
        return EiUtil::uint8ConvMap_[type](buf, index);
    else 
        ThrowRuntimeError("Object of type " << typeStrOf(buf, index) << " can't be converted to a uint8_t type");
    return 0;
}

int64_t EiUtil::objectToInt64(char* buf, int* index)
{
    DataType::Type type = typeOf(getType(buf, index), buf, index);
    if(EiUtil::int64ConvMap_.find(type) != EiUtil::int64ConvMap_.end())
        return EiUtil::int64ConvMap_[type](buf, index);
    else 
        ThrowRuntimeError("Object of type " << typeStrOf(buf, index) << " can't be converted to a int64_t type");
    return 0;
}

uint64_t EiUtil::objectToUint64(char* buf, int* index)
{
    DataType::Type type = typeOf(getType(buf, index), buf, index);
    if(EiUtil::uint64ConvMap_.find(type) != EiUtil::uint64ConvMap_.end())
        return EiUtil::uint64ConvMap_[type](buf, index);
    else 
        ThrowRuntimeError("Object of type " << typeStrOf(buf, index) << " can't be converted to a uint64_t type");
    return 0;
}

double EiUtil::objectToDouble(char* buf, int* index)
{
    DataType::Type type = typeOf(getType(buf, index), buf, index);
    if(EiUtil::doubleConvMap_.find(type) != EiUtil::doubleConvMap_.end())
        return EiUtil::doubleConvMap_[type](buf, index);
    else 
        ThrowRuntimeError("Object of type " << typeStrOf(buf, index) << " can't be converted to a double type");
    return 0;
}

std::map<DataType::Type, EI_CONV_UINT8_FN(*)>  EiUtil::constructUint8Map()
{
    std::map<DataType::Type, EI_CONV_UINT8_FN(*)> convMap;
    CONSTRUCT_EI_CONV_MAP(uint8_t);
    return convMap;
}

std::map<DataType::Type, EI_CONV_INT64_FN(*)>  EiUtil::constructInt64Map()
{
    std::map<DataType::Type, EI_CONV_INT64_FN(*)> convMap;
    CONSTRUCT_EI_CONV_MAP(int64_t);
    return convMap;
}

std::map<DataType::Type, EI_CONV_UINT64_FN(*)>  EiUtil::constructUint64Map()
{
    std::map<DataType::Type, EI_CONV_UINT64_FN(*)> convMap;
    CONSTRUCT_EI_CONV_MAP(uint64_t);
    return convMap;
}

std::map<DataType::Type, EI_CONV_DOUBLE_FN(*)>  EiUtil::constructDoubleMap()
{
    std::map<DataType::Type, EI_CONV_DOUBLE_FN(*)> convMap;
    CONSTRUCT_EI_CONV_MAP(double);
    return convMap;
}
