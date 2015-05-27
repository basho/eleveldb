#ifndef INCL_FILTER_PARSER_H
#define INCL_FILTER_PARSER_H

#include <string>
#include "erl_nif.h"
#include "filter.h"
#include "extractor.h"

namespace eleveldb {
    namespace filter {
        static const char* const EQ_OP = "==";
        static const char* const LTE_OP = "<=";
        static const char* const FIELD_OP = "field";
        static const char* const CONST_OP = "const";
    }
}

ExpressionNode<bool>* parse_range_filter_opts(ErlNifEnv* env, ERL_NIF_TERM options, Extractor& ext);

template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM root, Extractor& ext);
template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM rooti, Extractor& ext);
template<typename T>
ExpressionNode<T>* parse_field_expr(ErlNifEnv* env, ERL_NIF_TERM operand, Extractor& ext);
template<typename T>
ExpressionNode<T>* parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand, Extractor& ext);


ExpressionNode<bool>* parse_equals_expr(ErlNifEnv* env, ERL_NIF_TERM operands, Extractor& ext);
ExpressionNode<bool>* parse_lte_expr(ErlNifEnv* env, ERL_NIF_TERM operands, Extractor& ext);
#endif
