#ifndef ELEVELDB_ENCODING_H
#define ELEVELDB_ENCODING_H

/**
 * Encoding
 * 
 *   A class for managing valid encodings used in writing TS records
 *   to disk.
 *
 * Created: Mon Sep 14 11:36:04 PDT 2015
 * 
 * Original author: eleitch@basho.com
 */
#include <sstream>

#define MSGPACK_MAGIC 2
#define ERLANG_MAGIC  0

namespace eleveldb {

    class Encoding {
    public:

        enum Type {
            NONE,
            UNKNOWN,
            ERLANG,
            MSGPACK
        };

        /**
         * Constructor.
         */
        Encoding();

        /**
         * Destructor.
         */
        virtual ~Encoding();

        static std::string encodingAtom(Encoding::Type type);
        static unsigned char encodingByte(Encoding::Type type);
        static Type typeOf(std::string str, bool doThrow);

        friend std::ostream& operator<<(std::ostream& os,  Encoding::Type type);

    }; // End class Encoding

    //------------------------------------------------------------
    // Operator for printing encoding types
    //------------------------------------------------------------
    
    std::ostream& operator<<(std::ostream& os,  Encoding::Type type);

} // End namespace eleveldb



#endif // End #ifndef ELEVELDB_ENCODING_H
