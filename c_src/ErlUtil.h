// $Id: $

#ifndef ELEVELDB_ERLUTIL_H
#define ELEVELDB_ERLUTIL_H

/**
 * @file ErlUtil.h
 * 
 * Tagged: Wed Sep  2 14:46:45 PDT 2015
 * 
 * @version: $Revision: $, $Date: $
 * 
 * @author /bin/bash: username: command not found
 */
#include "erl_nif.h"
#include "workitems.h"

#include<string>

#define MIN_BUF_SIZE 1024

namespace eleveldb {

    class ErlUtil {
    public:
      
        // Constructor.

        ErlUtil(ErlNifEnv* env=0);
        ErlUtil(ErlNifEnv* env, ERL_NIF_TERM term);
      
        /**
         * Destructor.
         */
        virtual ~ErlUtil();

        void setEnv(ErlNifEnv* env);
        void setTerm(ERL_NIF_TERM term);

        bool isAtom();
        bool isAtom(ERL_NIF_TERM term); 
        static bool isAtom(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isBinary();
        bool isBinary(ERL_NIF_TERM term); 
        static bool isBinary(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isList();
        bool isList(ERL_NIF_TERM term); 
        static bool isList(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isTuple();
        bool isTuple(ERL_NIF_TERM term); 
        static bool isTuple(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isNumber();
        bool isNumber(ERL_NIF_TERM term); 
        static bool isNumber(ErlNifEnv* env, ERL_NIF_TERM term);

        // Return true if term is a string.  Returns true if term is a
        // valid atom or erlang string (ie, encoded as a list)

        bool isString();
        bool isString(ERL_NIF_TERM term); 
        static bool isString(ErlNifEnv* env, ERL_NIF_TERM term);

        // Return true is term can be represented as a string.
        // Returns true if term is a valid atom, erlang string, or
        // erlang binary (no checking is done if the binary is valid
        // UTF)

        bool isRepresentableAsString();
        bool isRepresentableAsString(ERL_NIF_TERM term); 
        static bool isRepresentableAsString(ErlNifEnv* env, ERL_NIF_TERM term);

        unsigned listLength();
        unsigned listLength(ERL_NIF_TERM term); 
        static unsigned listLength(ErlNifEnv* env, ERL_NIF_TERM term); 

        std::string getAtom();
        std::string getAtom(ERL_NIF_TERM term);
        static std::string getAtom(ErlNifEnv* env, ERL_NIF_TERM term);

        std::vector<unsigned char> getBinary();
        std::vector<unsigned char> getBinary(ERL_NIF_TERM term);
        static std::vector<unsigned char> getBinary(ErlNifEnv* env, ERL_NIF_TERM term);

        std::vector<ERL_NIF_TERM> getListCells();
        std::vector<ERL_NIF_TERM> getListCells(ERL_NIF_TERM term);
        static std::vector<ERL_NIF_TERM> getListCells(ErlNifEnv* env, 
                                                      ERL_NIF_TERM term);

        std::vector<ERL_NIF_TERM> getTupleCells();
        std::vector<ERL_NIF_TERM> getTupleCells(ERL_NIF_TERM term);
        static std::vector<ERL_NIF_TERM> getTupleCells(ErlNifEnv* env, ERL_NIF_TERM term);

        // Given an iolist consisting of {name, value} tuples, return a
        // vector of name/value pairs

        std::vector<std::pair<std::string, ERL_NIF_TERM> > getListTuples();
        std::vector<std::pair<std::string, ERL_NIF_TERM> > getListTuples(ERL_NIF_TERM term);

        // Get a string version of an erlang term.  Throws if term
        // isString() would return false

        std::string getString();
        std::string getString(ERL_NIF_TERM term);
        static std::string getString(ErlNifEnv* env, ERL_NIF_TERM term);

        // Return whatever is in term as a string.  Throws if
        // isRepresentableAsString() would return false

        std::string getAsString();
        std::string getAsString(ERL_NIF_TERM term);
        static std::string getAsString(ErlNifEnv* env, ERL_NIF_TERM term);

        // Return an erlang list as a string.  Throws if not a list,
        // or if the list can't be encoded as a valid UTF string

        static std::string getListAsString(ErlNifEnv* env, ERL_NIF_TERM term);

        // Return an erlang binary as a null-terminated string.  No
        // checking if term is valid UTF is done

        static std::string getBinaryAsString(ErlNifEnv* env, ERL_NIF_TERM term);

        static int32_t  getValAsInt32(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static int64_t  getValAsInt64(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint32_t getValAsUint32(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint8_t  getValAsUint8(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint64_t getValAsUint64(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static double   getValAsDouble(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);

        static std::string formatTerm(ErlNifEnv* env, ERL_NIF_TERM term);

        static std::string formatAtom(  ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatBinary(ErlNifEnv* env, ERL_NIF_TERM term);

        static std::string formatBinary(unsigned char* buf, size_t size);
        static std::string formatBinary(char* buf, size_t size);
        
        static std::string formatList(  ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatNumber(ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatString(ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatTuple( ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatTupleVec(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& tuple);

    private:

        void checkEnv();
        void checkTerm();

        ErlNifEnv* env_;

        bool hasTerm_;
        ERL_NIF_TERM term_;

    }; // End class ErlUtil

} // End namespace eleveldb



#endif // End #ifndef ELEVELDB_ERLUTIL_H
