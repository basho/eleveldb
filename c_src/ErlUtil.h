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

        bool isString();
        bool isString(ERL_NIF_TERM term); 
        static bool isString(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isNumber();
        bool isNumber(ERL_NIF_TERM term); 
        static bool isNumber(ErlNifEnv* env, ERL_NIF_TERM term);

        unsigned listLength();
        unsigned listLength(ERL_NIF_TERM term); 
        static unsigned listLength(ErlNifEnv* env, ERL_NIF_TERM term); 

        std::string getAtom();
        std::string getAtom(ERL_NIF_TERM term);
        static std::string getAtom(ErlNifEnv* env, ERL_NIF_TERM term, bool toLower=false);

        unsigned char* getBinary(size_t& size);
        unsigned char* getBinary(ERL_NIF_TERM term, size_t& size);
        static unsigned char* getBinary(ErlNifEnv* env, 
                                        ERL_NIF_TERM term, size_t& size);

        std::string getString();
        std::string getString(ERL_NIF_TERM term);
        static std::string getString(ErlNifEnv* env, ERL_NIF_TERM term);

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

        void decodeRiakObject(ERL_NIF_TERM obj, ERL_NIF_TERM encoding);
        void parseSiblingData(unsigned char* ptr, unsigned len);
        void parseSiblingDataMsgpack(unsigned char* ptr, unsigned len);

        static int32_t  getValAsInt32(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static int64_t  getValAsInt64(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint32_t getValAsUint32(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint8_t  getValAsUint8(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint64_t getValAsUint64(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static double   getValAsDouble(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);

        static std::string formatTerm(ErlNifEnv* env, ERL_NIF_TERM term);

        static std::string formatAtom(  ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatBinary(ErlNifEnv* env, ERL_NIF_TERM term);
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
