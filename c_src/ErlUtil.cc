#include "ErlUtil.h"
#include "StringBuf.h"

#include "cmp.h"
#include "cmp_mem_access.h"

#include <cctype>
#include <climits>
#include <cmath>

#include <arpa/inet.h>

using namespace std;

using namespace eleveldb;

/**.......................................................................
 * Constructors
 */
ErlUtil::ErlUtil(ErlNifEnv* env) 
{
    setEnv(env);
    hasTerm_ = 0;
}

ErlUtil::ErlUtil(ErlNifEnv* env, ERL_NIF_TERM term) 
{
    setEnv(env);
    setTerm(term);
}

/**.......................................................................
 * Destructor.
 */
ErlUtil::~ErlUtil() {}

void ErlUtil::setEnv(ErlNifEnv* env) 
{
    env_ = env;
}

void ErlUtil::setTerm(ERL_NIF_TERM term) 
{
    term_ = term;
    hasTerm_ = true;
}

void ErlUtil::checkEnv() 
{
    if(!env_)
        ThrowRuntimeError("No environment has been set");
}

void ErlUtil::checkTerm() 
{
    if(!hasTerm_)
        ThrowRuntimeError("No term has been set");
}

bool ErlUtil::isAtom()
{
    checkTerm();
    return isAtom(term_);
}

bool ErlUtil::isAtom(ERL_NIF_TERM term)
{
    checkEnv();
    return isAtom(env_, term);
}

bool ErlUtil::isAtom(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return enif_is_atom(env, term);
}

bool ErlUtil::isList()
{
    checkTerm();
    return isList(term_);
}

bool ErlUtil::isList(ERL_NIF_TERM term)
{
    checkEnv();
    return isList(env_, term);
}

bool ErlUtil::isList(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return enif_is_list(env, term);
}

bool ErlUtil::isBinary()
{
    checkTerm();
    return isBinary(term_);
}

bool ErlUtil::isBinary(ERL_NIF_TERM term)
{
    checkEnv();
    return isBinary(env_, term);
}

bool ErlUtil::isBinary(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return enif_is_binary(env, term);
}

bool ErlUtil::isTuple()
{
    checkTerm();
    return isTuple(term_);
}

bool ErlUtil::isTuple(ERL_NIF_TERM term)
{
    checkEnv();
    return isTuple(env_, term);
}

bool ErlUtil::isTuple(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return enif_is_tuple(env, term);
}

bool ErlUtil::isString()
{
    checkTerm();
    return isString(term_);
}

bool ErlUtil::isString(ERL_NIF_TERM term)
{
    checkEnv();
    return isString(env_, term);
}

bool ErlUtil::isString(ErlNifEnv* env, ERL_NIF_TERM term)
{
    unsigned size=0;

    if(isAtom(env, term)) {
        return true;
        
        //------------------------------------------------------------
        // Erlang represents strings internally as lists, so use native
        // erlang conversion to check a list
        //------------------------------------------------------------

    } else if(isList(env, term)) {

        if(enif_get_list_length(env, term, &size) == 0) {
            ThrowRuntimeError("Failed to get list length");
        }

        // Note that enif_get_string expects the buffer size, not the
        // string length, and will return less than the length of the
        // string if the passed size isn't large enough to include the
        // string + null terminator

        StringBuf sBuf(size+1);
        return enif_get_string(env, term, sBuf.getBuf(), sBuf.bufSize(), ERL_NIF_LATIN1);

        //------------------------------------------------------------
        // Else not a string
        //------------------------------------------------------------

    } else {
        return false;
    }
}

bool ErlUtil::isRepresentableAsString()
{
    checkTerm();
    return isRepresentableAsString(term_);
}

bool ErlUtil::isRepresentableAsString(ERL_NIF_TERM term)
{
    checkEnv();
    return isRepresentableAsString(env_, term);
}

bool ErlUtil::isRepresentableAsString(ErlNifEnv* env, ERL_NIF_TERM term)
{
    unsigned size=0;

    //------------------------------------------------------------
    // If this is an atom, it can be represented as a string
    //------------------------------------------------------------

    if(isAtom(env, term)) {
        return true;
        
        //------------------------------------------------------------
        // If this is a binary, it _could_ be a string
        //------------------------------------------------------------
        
    } else if(isBinary(env, term)) {
        return true;

        //------------------------------------------------------------
        // Erlang represents strings internally as lists, so use native
        // erlang conversion to check that a list is a string
        //------------------------------------------------------------

    } else if(isList(env, term)) {

        if(enif_get_list_length(env, term, &size) == 0) {
            ThrowRuntimeError("Failed to get list length");
        }

        // Note that enif_get_string expects the buffer size, not the
        // string length, and will return less than the length of the
        // string if the passed size isn't large enough to include the
        // string + null terminator

        StringBuf sBuf(size+1);
        return enif_get_string(env, term, sBuf.getBuf(), sBuf.bufSize(), ERL_NIF_LATIN1);

        //------------------------------------------------------------
        // Else not a string
        //------------------------------------------------------------

    } else {
        return false;
    }
}

bool ErlUtil::isNumber()
{
    checkTerm();
    return isNumber(term_);
}

bool ErlUtil::isNumber(ERL_NIF_TERM term)
{
    checkEnv();
    return isNumber(env_, term);
}

bool ErlUtil::isNumber(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return enif_is_number(env, term);
}

unsigned ErlUtil::listLength()
{
    checkTerm();
    return listLength(term_);
}

unsigned ErlUtil::listLength(ERL_NIF_TERM term)
{
    checkEnv();
    return listLength(env_, term);
}

unsigned ErlUtil::listLength(ErlNifEnv* env, ERL_NIF_TERM term)
{
    unsigned len=0;

    if(enif_get_list_length(env, term,  &len) == 0) {
        ThrowRuntimeError("Unable to get list length");
    }

    return len;
}

std::string ErlUtil::getAtom()
{
    checkTerm();
    return getAtom(term_);
}

std::string ErlUtil::getAtom(ERL_NIF_TERM term)
{
    checkEnv();
    return getAtom(env_, term);
}

std::string ErlUtil::getAtom(ErlNifEnv* env, ERL_NIF_TERM term)
{
    unsigned len=0;
    if(!enif_get_atom_length(env, term, &len, ERL_NIF_LATIN1))
        ThrowRuntimeError("Unable to encode atom");

    StringBuf sBuf(len+1);
    char* buf = sBuf.getBuf();

    if(!enif_get_atom(env, term, buf, sBuf.bufSize(), ERL_NIF_LATIN1))
        ThrowRuntimeError("Unable to encode atom");
        
    // At this point, buf will contain a null-terminated version of
    // the converted atom, and can be returned as the argument to
    // std::string() constructor

    return buf;
}

std::vector<unsigned char> ErlUtil::getBinary()
{
    checkTerm();
    return getBinary(term_);
}

std::vector<unsigned char> ErlUtil::getBinary(ERL_NIF_TERM term)
{
    checkEnv();
    return getBinary(env_, term);
}

std::vector<unsigned char> ErlUtil::getBinary(ErlNifEnv* env, ERL_NIF_TERM term)
{
    std::vector<unsigned char> ret;

    ErlNifBinary bin;

    if(enif_inspect_binary(env, term, &bin) == 0)
        ThrowRuntimeError("Failed to inspect '" << formatTerm(env, term)
                          << "' as a binary");

    ret.resize(bin.size);
    memcpy(&ret[0], bin.data, bin.size);

    return ret;
}

std::string ErlUtil::getString()
{
    checkTerm();
    return getString(term_);
}

std::string ErlUtil::getString(ERL_NIF_TERM term)
{
    checkEnv();
    return getString(env_, term);
}

std::string ErlUtil::getString(ErlNifEnv* env, ERL_NIF_TERM term)
{
    unsigned size=0;

    //------------------------------------------------------------
    // Atoms are represented as strings
    //------------------------------------------------------------

    if(isAtom(env, term)) {
        return getAtom(env, term);

        //------------------------------------------------------------
        // Erlang represents strings internally as lists, so use native
        // erlang conversion to check a list
        //------------------------------------------------------------

    } else if(isList(env, term)) {

        if(enif_get_list_length(env, term, &size) == 0) {
            ThrowRuntimeError("Failed to get list length");
        }

        StringBuf sBuf(size+1);
        char* buf = sBuf.getBuf();

        if(enif_get_string(env, term, buf, size+1, ERL_NIF_LATIN1) == 0) {
            ThrowRuntimeError("Unable to encode string");
        }

        // At this point, buf will contain a null-terminated version of
        // the converted atom, and can be returned as the argument to
        // std::string() constructor

        return buf;

        //------------------------------------------------------------
        // Else not a string
        //------------------------------------------------------------

    } else {
        ThrowRuntimeError("Term '" << formatTerm(env, term) 
                          << "' cannot be interpreted as a string");
    }
}

std::string ErlUtil::getAsString()
{
    checkTerm();
    return getAsString(term_);
}

std::string ErlUtil::getAsString(ERL_NIF_TERM term)
{
    checkEnv();
    return getAsString(env_, term);
}

std::string ErlUtil::getAsString(ErlNifEnv* env, ERL_NIF_TERM term)
{
    //------------------------------------------------------------
    // Atoms are represented as strings
    //------------------------------------------------------------

    if(isAtom(env, term)) {
        return getAtom(env, term);

        //------------------------------------------------------------
        // We allow generic binaries to be interpreted as strings
        //------------------------------------------------------------

    } else if(isBinary(env, term)) {
        return getBinaryAsString(env, term);

        //------------------------------------------------------------
        // Erlang represents strings internally as lists, so use native
        // erlang conversion to check a list
        //------------------------------------------------------------

    } else if(isList(env, term)) {
        return getListAsString(env, term);

        //------------------------------------------------------------
        // Else not a string
        //------------------------------------------------------------

    } else {
        ThrowRuntimeError("Term '" << formatTerm(env, term) 
                          << "' cannot be interpreted as a string");
    }
}

/**.......................................................................
 * Return an erlang binary as a string
 */
std::string ErlUtil::getBinaryAsString(ErlNifEnv* env, ERL_NIF_TERM term)
{
    ErlNifBinary bin;

    if(enif_inspect_binary(env, term, &bin) == 0) {
        ThrowRuntimeError("Failed to inspect binary");
    }

    StringBuf sBuf(bin.size+1);
    char* buf = sBuf.getBuf();

    memcpy(buf, bin.data, bin.size);
    
    buf[bin.size] = '\0';

    return buf;
}

/**.......................................................................
 * Return an erlang list as a string.
 */
std::string ErlUtil::getListAsString(ErlNifEnv* env, ERL_NIF_TERM term)
{
    unsigned size=0;

    if(enif_get_list_length(env, term, &size) == 0) {
        ThrowRuntimeError("Failed to get list length");
    }
    
    StringBuf sBuf(size+1);
    char* buf = sBuf.getBuf();
    
    if(enif_get_string(env, term, buf, size+1, ERL_NIF_LATIN1) == 0) {
        ThrowRuntimeError("Unable to encode string");
    }
    
    return buf;
}

/**.......................................................................
 * Return a vector of items from an erlang list
 */
std::vector<ERL_NIF_TERM> ErlUtil::getListCells()
{
    checkTerm();
    return getListCells(term_);
}

std::vector<ERL_NIF_TERM> ErlUtil::getListCells(ERL_NIF_TERM term)
{
    checkEnv();
    return getListCells(env_, term);
}

std::vector<ERL_NIF_TERM> ErlUtil::getListCells(ErlNifEnv* env, ERL_NIF_TERM term)
{
    if(!isList(env, term)) {
        ThrowRuntimeError("Not a list");
    }

    unsigned len = listLength(env, term);

    std::vector<ERL_NIF_TERM> cells(len);
    ERL_NIF_TERM curr, rest = term;

    for(unsigned iCell=0; iCell < len; iCell++) {

        if(enif_get_list_cell(env, rest, &curr, &rest) == 0) {
            ThrowRuntimeError("Unable to get next list cell");
        }

        cells[iCell] = curr;
    }

    return cells;
}

/**.......................................................................
 * Return a vector of items from an erlang tuple
 */
std::vector<ERL_NIF_TERM> ErlUtil::getTupleCells()
{
    checkTerm();
    return getTupleCells(term_);
}

std::vector<ERL_NIF_TERM> ErlUtil::getTupleCells(ERL_NIF_TERM term)
{
    checkEnv();
    return getTupleCells(env_, term);
}

std::vector<ERL_NIF_TERM> ErlUtil::getTupleCells(ErlNifEnv* env, ERL_NIF_TERM term)
{
    if(!isTuple(env, term)) {
        ThrowRuntimeError("Not a tuple");
    }

    int arity=0;
    const ERL_NIF_TERM* array=0;

    if(!enif_get_tuple(env, term, &arity, &array)) 
        ThrowRuntimeError("Unable to parse a tuple");

    std::vector<ERL_NIF_TERM> cells(arity);

    for(int iCell=0; iCell < arity; iCell++)
        cells[iCell] = array[iCell];

    return cells;
}

/**.......................................................................
 * Return a vector of name-value pairs from an erlang list of tuples, ie:
 *
 * [{name, value}, {name, value}, {name, value}]
 */
std::vector<std::pair<std::string, ERL_NIF_TERM> > ErlUtil::getListTuples()
{
    checkTerm();
    return getListTuples(term_);
}

std::vector<std::pair<std::string, ERL_NIF_TERM> > ErlUtil::getListTuples(ERL_NIF_TERM term)
{
    checkEnv();

    if(!isList(term))
        ThrowRuntimeError("Not a list");

    unsigned len = listLength(term);

    std::vector<std::pair<std::string, ERL_NIF_TERM> > cells(len);
    ERL_NIF_TERM curr, rest = term;

    for(unsigned iCell=0; iCell < len; iCell++) {

        if(enif_get_list_cell(env_, rest, &curr, &rest) == 0)
            ThrowRuntimeError("Unable to get next list cell");

        if(!enif_is_tuple(env_, curr))
            ThrowRuntimeError("List cell is not a tuple");

        int arity=0;
        const ERL_NIF_TERM* array=0;
        if(enif_get_tuple(env_, curr, &arity, &array)==0)
            ThrowRuntimeError("Unable to get tuple");
  
        if(arity != 2)
            ThrowRuntimeError("Malformed tuple");

        try {
            cells[iCell].first  = getString(array[0]);
        } catch(std::runtime_error err) {
            std::ostringstream os;
            os << err.what() << " (while processing tuple " << iCell+1 << ")";
            throw std::runtime_error(os.str());
        }

        cells[iCell].second = array[1];
    }

    return cells;
}

/**.......................................................................
 * Try to convert the erlang value in term to an integer
 */
int32_t ErlUtil::getValAsInt32(ErlNifEnv* env, ERL_NIF_TERM term, bool exact)
{
    // Integers can be converted to integers

    int intVal;
    if(enif_get_int(env, term, &intVal))
        return intVal;

    // Unsigned integers can be converted to integers, as long as they
    // are not greater than INT_MAX

    unsigned int uintVal;
    if(enif_get_uint(env, term, &uintVal))
        if(uintVal <= INT_MAX)
            return (int)uintVal;

    // int64_ts can be converted to integers, as long as they
    // are within the representable range of ints

    ErlNifSInt64 int64Val;
    if(enif_get_int64(env, term, &int64Val))
        if(int64Val <= INT_MAX && int64Val >= INT_MIN)
            return (int)int64Val;

    // uint64_ts can be converted to integers, as long as they
    // are not greater than INT_MAX

    ErlNifUInt64 uint64Val;
    if(enif_get_uint64(env, term, &uint64Val))
        if(uint64Val <= INT_MAX)
            return (int)uint64Val;

    // Doubles can be converted to ints, as long as they are within the
    // representable range of ints and have no fractional component

    double doubleVal;
    if(enif_get_double(env, term, &doubleVal)) {

        if(doubleVal <= (double)INT_MAX && doubleVal >= (double)INT_MIN) {
            if(!exact || !(fabs(doubleVal - (int)doubleVal) > 0.0))
                return (int)doubleVal;
        }
    } 

    //------------------------------------------------------------
    // Atoms can be represented if they are boolean values
    //------------------------------------------------------------

    if(ErlUtil::isAtom(env, term)) {
        std::string atom = ErlUtil::getAtom(env, term);
        if(atom == "true") {
            return 1;
        } else if(atom == "false") {
            return 0;
        }
    }

    ThrowRuntimeError("Erlang value " << formatTerm(env, term) 
                      << " can't be represented as an integer");
    return 0;
}

/**.......................................................................
 * Try to convert the erlang value in term to a 64-bit integer
 */
int64_t ErlUtil::getValAsInt64(ErlNifEnv* env, ERL_NIF_TERM term, bool exact)
{
    // int64_ts can always be converted to 64-bit integers

    ErlNifSInt64 int64Val;
    if(enif_get_int64(env, term, &int64Val))
        return int64Val;

    // Integers can always be converted to 64-bit integers

    int intVal;
    if(enif_get_int(env, term, &intVal))
        return intVal;

    // Unsigned integers can always be converted to 64-bit integers

    unsigned int uintVal;
    if(enif_get_uint(env, term, &uintVal))
        return (int64_t)uintVal;

    // uint64_ts can be converted to 64-bit integers, as long as they
    // are not greater than LLONG_MAX

    ErlNifUInt64 uint64Val;
    if(enif_get_uint64(env, term, &uint64Val))
        if(uint64Val <= LLONG_MAX)
            return (int64_t)uint64Val;

    // Doubles can be converted to 64-bit ints, as long as they are
    // within the representable range of 64-bit ints and have no fractional
    // component

    double doubleVal;
    if(enif_get_double(env, term, &doubleVal)) {

        if(doubleVal <= (double)LLONG_MAX && doubleVal >= (double)LLONG_MIN) {
            if(!exact || !(fabs(doubleVal - (int)doubleVal) > 0.0))
                return (int64_t)doubleVal;
        }
    } 

    //------------------------------------------------------------
    // Atoms can be represented if they are boolean values
    //------------------------------------------------------------

    if(ErlUtil::isAtom(env, term)) {
        std::string atom = ErlUtil::getAtom(env, term);
        if(atom == "true") {
            return 1;
        } else if(atom == "false") {
            return 0;
        }
    }

    ThrowRuntimeError("Erlang value " << formatTerm(env, term) 
                      << " can't be represented as an int64_t");
    return 0;
}

/**.......................................................................
 * Try to convert the erlang value in term to an unsigned integer
 */
uint32_t ErlUtil::getValAsUint32(ErlNifEnv* env, ERL_NIF_TERM term, bool exact)
{
    //------------------------------------------------------------
    // Unsigned integers can be converted to unsigned integers
    //------------------------------------------------------------

    unsigned int uintVal;
    if(enif_get_uint(env, term, &uintVal))
        return uintVal;

    //------------------------------------------------------------
    // Integers can be converted to unsigned integers, as long as they
    // as positive
    //------------------------------------------------------------
    
    int intVal;
    if(enif_get_int(env, term, &intVal)) {
        if(intVal >= 0)
            return (unsigned int)intVal;
    }

    //------------------------------------------------------------
    // int64_t can be converted to integers, as long as they are
    // positive, and within the representable range of unsigned ints
    //------------------------------------------------------------

    ErlNifSInt64 int64Val;
    if(enif_get_int64(env, term, &int64Val))
        if(int64Val >= 0 && int64Val <= UINT_MAX)
            return (unsigned int)int64Val;

    //------------------------------------------------------------
    // uint64_ts can be converted to integers, as long as they
    // are not greater than UINT_MAX
    //------------------------------------------------------------

    ErlNifUInt64 uint64Val;
    if(enif_get_uint64(env, term, &uint64Val))
        if(uint64Val <= UINT_MAX)
            return (unsigned int)uint64Val;

    //------------------------------------------------------------
    // Doubles can be converted to unsigned ints, as long as they are
    // within the representable range of unsigned ints and have no
    // fractional component
    //------------------------------------------------------------

    double doubleVal;
    if(enif_get_double(env, term, &doubleVal)) {
        if(doubleVal >= 0.0) {
            if(doubleVal <= (double)UINT_MAX) {
                if(!exact || !(fabs(doubleVal - (int)doubleVal) > 0.0))
                    return (int)doubleVal;
            }
        }
    } 

    //------------------------------------------------------------
    // Atoms can be represented if they are boolean values
    //------------------------------------------------------------

    if(ErlUtil::isAtom(env, term)) {
        std::string atom = ErlUtil::getAtom(env, term);
        if(atom == "true") {
            return 1;
        } else if(atom == "false") {
            return 0;
        }
    }

    ThrowRuntimeError("Erlang value " << formatTerm(env, term) 
                      << " can't be represented as an unsigned integer");

    return 0;
}

/**.......................................................................
 * Try to convert the erlang value in term to a uint8_t
 */
uint8_t ErlUtil::getValAsUint8(ErlNifEnv* env, ERL_NIF_TERM term, bool exact)
{
    //------------------------------------------------------------
    // Unsigned integers can be converted to uint8_t, as long as they
    // don't exceed UCHAR_MAX
    //------------------------------------------------------------

    unsigned int uintVal;
    if(enif_get_uint(env, term, &uintVal))
        if(uintVal <= UCHAR_MAX)
            return uintVal;

    //------------------------------------------------------------
    // Integers can be converted to uint8_t, as long as they
    // as positive and don't exceed UCHAR_MAX
    //------------------------------------------------------------
    
    int intVal;
    if(enif_get_int(env, term, &intVal)) {
        if(intVal >= 0 && intVal <= UCHAR_MAX)
           return intVal;
    }

    //------------------------------------------------------------
    // int64_t can be converted to integers, as long as they are
    // positive, and within the representable range of uint8_t
    //------------------------------------------------------------

    ErlNifSInt64 int64Val;
    if(enif_get_int64(env, term, &int64Val))
        if(int64Val >= 0 && int64Val <= UCHAR_MAX)
            return int64Val;

    //------------------------------------------------------------
    // uint64_ts can be converted to integers, as long as they
    // are not greater than UCHAR_MAX
    //------------------------------------------------------------

    ErlNifUInt64 uint64Val;
    if(enif_get_uint64(env, term, &uint64Val))
        if(uint64Val <= UCHAR_MAX)
            return uint64Val;

    //------------------------------------------------------------
    // Doubles can be converted to unsigned ints, as long as they are
    // within the representable range of uint8_t and have no
    // fractional component
    //------------------------------------------------------------

    double doubleVal;
    if(enif_get_double(env, term, &doubleVal)) {
        if(doubleVal >= 0.0) {
            if(doubleVal <= (double)UCHAR_MAX) {
                if(!exact || !(fabs(doubleVal - (int)doubleVal) > 0.0))
                    return doubleVal;
            }
        }
    } 

    //------------------------------------------------------------
    // Atoms can be represented as uint8_t if they are boolean values
    //------------------------------------------------------------

    if(ErlUtil::isAtom(env, term)) {
        std::string atom = ErlUtil::getAtom(env, term);
        if(atom == "true") {
            return 1;
        } else if(atom == "false") {
            return 0;
        }
    }

    ThrowRuntimeError("Erlang value " << formatTerm(env, term) 
                      << " can't be represented as a uint8_t");

    return 0;
}

/**.......................................................................
 * Try to convert the erlang value in term to a 64-bit unsigned integer
 */
uint64_t ErlUtil::getValAsUint64(ErlNifEnv* env, ERL_NIF_TERM term, bool exact)
{
    //------------------------------------------------------------
    // uint64_t can be converted to 64-bit integers
    //------------------------------------------------------------

    ErlNifUInt64 uint64Val;
    if(enif_get_uint64(env, term, &uint64Val))
        return uint64Val;

    //------------------------------------------------------------
    // Integers can be converted to 64-bit unsigned integers, as long as
    // they as positive
    //------------------------------------------------------------

    int intVal;
    if(enif_get_int(env, term, &intVal)) {
        if(intVal >= 0)
            return (uint64_t)intVal;
    }

    //------------------------------------------------------------
    // Unsigned integers can be converted to 64-bit unsigned integers
    //------------------------------------------------------------

    unsigned int uintVal;
    if(enif_get_uint(env, term, &uintVal))
        return (uint64_t)uintVal;

    //------------------------------------------------------------
    // int64_t can be converted to integers, as long as they are
    // positive
    //------------------------------------------------------------

    ErlNifSInt64 int64Val;
    if(enif_get_int64(env, term, &int64Val))
        if(int64Val >= 0)
            return (uint64_t)int64Val;

    //------------------------------------------------------------
    // Doubles can be converted to 64-bit unsigned ints, as long as they
    // are within the representable range of 64-bit unsigned ints and
    // have no fractional component
    //------------------------------------------------------------

    double doubleVal;
    if(enif_get_double(env, term, &doubleVal)) {
        if(doubleVal >= 0.0) {
            if(doubleVal <= (double)ULLONG_MAX) {
                if(!exact || !(fabs(doubleVal - (int)doubleVal) > 0.0))
                    return (int)doubleVal;
            }
        }
    } 

    //------------------------------------------------------------
    // Atoms can be represented if they are boolean values
    //------------------------------------------------------------

    if(ErlUtil::isAtom(env, term)) {
        std::string atom = ErlUtil::getAtom(env, term);
        if(atom == "true") {
            return 1;
        } else if(atom == "false") {
            return 0;
        }
    }

    ThrowRuntimeError("Erlang value " << formatTerm(env, term) 
                      << " can't be represented as a uint64_t");

    return 0;
}

/**.......................................................................
 * Try to convert the erlang value in term to a 64-bit unsigned integer
 */
double ErlUtil::getValAsDouble(ErlNifEnv* env, ERL_NIF_TERM term, bool exact)
{
    double doubleVal;
    if(enif_get_double(env, term, &doubleVal))
        return doubleVal;

    int intVal;
    if(enif_get_int(env, term, &intVal)) {
        return (double)intVal;
    }

    unsigned int uintVal;
    if(enif_get_uint(env, term, &uintVal))
        return (double)uintVal;

    ErlNifSInt64 int64Val;
    if(enif_get_int64(env, term, &int64Val))
        return (double)int64Val;

    ErlNifUInt64 uint64Val;
    if(enif_get_uint64(env, term, &uint64Val))
        return (double)uint64Val;

    //------------------------------------------------------------
    // Atoms can be represented if they are boolean values
    //------------------------------------------------------------

    if(ErlUtil::isAtom(env, term)) {
        std::string atom = ErlUtil::getAtom(env, term);
        if(atom == "true") {
            return 1.0;
        } else if(atom == "false") {
            return 0.0;
        }
    }

    ThrowRuntimeError("Erlang value " << formatTerm(env, term) 
                      << " can't be represented as a double");

    return 0.0;
}

/**.......................................................................
 * Format an erlang value
 */
std::string ErlUtil::formatTerm(ErlNifEnv* env, ERL_NIF_TERM term)
{
    if(isAtom(env, term)) {
        return formatAtom(env, term);
    }

    if(isNumber(env, term)) {
        return formatNumber(env, term);
    }

    if(isTuple(env, term)) {
        return formatTuple(env, term);
    }

    if(isBinary(env, term)) {
        return formatBinary(env, term);
    }

    if(isString(env, term)) {
        return formatString(env, term);
    }

    if(isList(env, term)) {
        return formatList(env, term);
    }

    return "?";
}

std::string ErlUtil::formatAtom(ErlNifEnv* env, ERL_NIF_TERM term)
{
    std::ostringstream os;
    os << getAtom(env, term);

    return os.str();
}

std::string ErlUtil::formatNumber(ErlNifEnv* env, ERL_NIF_TERM term)
{
    std::ostringstream os;

    int intVal;
    if(enif_get_int(env, term, &intVal)) {
        os << intVal;
        return os.str();
    }

    unsigned int uintVal;
    if(enif_get_uint(env, term, &uintVal)) {
        os << uintVal;
        return os.str();
    }

    ErlNifSInt64 int64Val;
    if(enif_get_int64(env, term, &int64Val)) {
        os << int64Val;
        return os.str();
    }

    ErlNifUInt64 uint64Val;
    if(enif_get_uint64(env, term, &uint64Val)) {
        os << uint64Val;
        return os.str();
    }

    double doubleVal;
    if(enif_get_double(env, term, &doubleVal)) {
        os << doubleVal;
        return os.str();
    }

    return "?";
}

std::string ErlUtil::formatString(ErlNifEnv* env, ERL_NIF_TERM term)
{
    std::ostringstream os;
    os << "\"" << getString(env, term) << "\"";

    return os.str();
}

std::string ErlUtil::formatList(ErlNifEnv* env, ERL_NIF_TERM term)
{
    std::vector<ERL_NIF_TERM> cells = getListCells(env, term);
    std::ostringstream os;

    os << "[";
    for(unsigned iCell=0; iCell < cells.size(); iCell++) {
        os << formatTerm(env, cells[iCell]);
        if(iCell < cells.size()-1)
            os << ", ";
    }
    os << "]";

    return os.str();
}

std::string ErlUtil::formatBinary(ErlNifEnv* env, ERL_NIF_TERM term)
{
    ErlNifBinary bin;
    if(enif_inspect_binary(env, term, &bin) == 0)
        ThrowRuntimeError("Failed to inspect binary");

    std::ostringstream os;

    os << "<<";
    for(unsigned iByte=0; iByte < bin.size; iByte++) {
        os << (int)bin.data[iByte];
        if(iByte < bin.size-1)
            os << ", ";
    }
    os << ">>";

    return os.str();
}

std::string ErlUtil::formatBinary(char* buf, size_t size)
{
    return formatBinary((unsigned char*) buf, size);
}

std::string ErlUtil::formatBinary(unsigned char* buf, size_t size)
{
    std::ostringstream os;

    os << "<<";
    for(unsigned iByte=0; iByte < size; iByte++) {
        os << (int)buf[iByte];
        if(iByte < size-1)
            os << ", ";
    }
    os << ">>";

    return os.str();
}

std::string ErlUtil::formatTupleVec(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& cells)
{
    std::ostringstream os;
    
    os << "{";
    for(unsigned iCell=0; iCell < cells.size(); iCell++) {
        os << formatTerm(env, cells[iCell]);
        if(iCell < cells.size()-1)
            os << ", ";
    }

    os << "}";

    return os.str();
}

std::string ErlUtil::formatTuple(ErlNifEnv* env, ERL_NIF_TERM term)
{
    std::vector<ERL_NIF_TERM> cells = getTupleCells(env, term);
    return formatTupleVec(env, cells);
}

