#ifndef ELEVELDB_STRINGBUF_H
#define ELEVELDB_STRINGBUF_H

#define STRING_BUF_MIN_SIZE 1024

/**
 * StringBuf
 * 
 *   A class for managing a char buffer, initially allocated on the
 *   stack, but grown from the heap if resized beyond a fixed limit.
 *
 * Original author: eleitch@basho.com
 */
#include <stdlib.h>
#include <stddef.h>

#include <string>

namespace eleveldb {

    class StringBuf {
    public:
        
        // Constructors

        StringBuf();
        StringBuf(size_t size);

        // Copy constructors

        StringBuf(const StringBuf& buf);
        StringBuf(StringBuf& buf);

        virtual ~StringBuf();

        // Assignment operators

        void operator=(StringBuf& buf);
        void operator=(const StringBuf& buf);

        // Resize our internal buffer

        void resize(size_t size);

        // Copy a byte array into our buffer

        void copy(char* buf, size_t size);
        void copyAsString(char* buf, size_t size);

        // Return the buffer size

        size_t bufSize();

        // Return the data size

        size_t dataSize();

        // Get a pointer to the buffer

        char*  getBuf();

        // Return a string representation of the internal buffer
        // (assumed to be null-terminated)
        
        std::string getString();
        
    private:

        void initialize();

        // A fixed-size internal buffer

        char   fixedBuf_[STRING_BUF_MIN_SIZE];

        // If required, a heap-allocated buffer

        char*  heapBuf_;

        // A pointer to whichever buffer is currently relevant

        char*  bufPtr_;

        // The actual size of the buffer
        
        size_t bufSize_;
        
        // The size of the data in the buffer
        
        size_t dataSize_;

    }; // End class StringBuf

} // End namespace eleveldb



#endif // End #ifndef ELEVELDB_STRINGBUF_H
