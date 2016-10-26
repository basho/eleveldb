#ifndef ELEVELDB_CMPUTIL_H
#define ELEVELDB_CMPUTIL_H

/**
 * CmpUtil
 *
 *   A class for operating on msgpack-encoded data, using the cmp
 *   library (Original can be found at https://github.com/camgunz/cmp)
 * 
 * Created: Wed Sep  9 17:32:28 PDT 2015
 * 
 * Original author: eleitch@basho.com
 */
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>

#include <map>
#include <vector>

#include "cmp.h"
#include "cmp_mem_access.h"

#include "DataType.h"

#define CONV_UINT8_FN(fn)  uint8_t  (fn)(cmp_object_t* obj)
#define CONV_INT64_FN(fn)  int64_t  (fn)(cmp_object_t* obj)
#define CONV_UINT64_FN(fn) uint64_t (fn)(cmp_object_t* obj)
#define CONV_DOUBLE_FN(fn) double   (fn)(cmp_object_t* obj)

namespace eleveldb {

    class CmpUtil {
    public:
        
        /**
         * Constructor.
         */
        CmpUtil();

        /**
         * Destructor.
         */
        virtual ~CmpUtil();
        
        // Return the data type of this object

        static DataType::Type typeOf(cmp_object_t* obj);

        // Return a string representing the type of this object

        static std::string typeStrOf(cmp_object_t* obj);

        // Return the size of prefix and marker for this object

        static size_t prefixSizeOf(cmp_object_t* obj);
        static size_t markerSize();

        // Return the size of the data for this object

        static size_t dataSizeOf(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* obj);
        static size_t binarySize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* bin);
        static size_t stringSize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* str);
        static size_t arraySize( cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* arr);
        static size_t mapSize(   cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* map);

        // Advance internal mem_access pointers past the data for the
        // last-read object

        static void skipLastReadObject(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* obj);

        // Return a pointer to the data for the passed object

        static unsigned char* getDataPtr(cmp_mem_access_t* ma, cmp_ctx_t* cmp,
                                         cmp_object_t* obj, size_t& size, bool includeMarker);

        // Parse a msgpack-encoded map into a C++ map of field/type pairs

        static std::map<std::string, DataType::Type> parseMap(const char* data, size_t size);

        // Print a map of field/type pairs

        static void printMap(std::map<std::string, DataType::Type>& keyValMap);

        // Return true if object is an empty erlang list

        static bool isEmptyList(cmp_object_t* obj);

        // Object conversion functions

        static uint8_t  objectToUint8( cmp_object_t* obj);
        static int64_t  objectToInt64( cmp_object_t* obj);
        static uint64_t objectToUint64(cmp_object_t* obj);
        static double   objectToDouble(cmp_object_t* obj);

        // Templatized type-conversion functions

        template<typename typeTo, typename typeFrom> 
        static typeTo convert(cmp_object_t* obj);

        // Return a map of conversion functions

        static std::map<uint8_t, CONV_UINT8_FN(*)>  constructUint8Map();
        static std::map<uint8_t, CONV_INT64_FN(*)>  constructInt64Map();
        static std::map<uint8_t, CONV_UINT64_FN(*)> constructUint64Map();
        static std::map<uint8_t, CONV_DOUBLE_FN(*)> constructDoubleMap();

        // Static maps of conversion functions

        static std::map<uint8_t, CONV_UINT8_FN(*)>  uint8ConvMap_;
        static std::map<uint8_t, CONV_INT64_FN(*)>  int64ConvMap_;
        static std::map<uint8_t, CONV_UINT64_FN(*)> uint64ConvMap_;
        static std::map<uint8_t, CONV_DOUBLE_FN(*)> doubleConvMap_;

    }; // End class CmpUtil
    
} // End namespace eleveldb

#endif // End #ifndefELEVELDB_CMPUTIL_H
