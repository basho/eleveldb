#ifndef INCL_FILTER_PARSER_H
#define INCL_FILTER_PARSER_H

#include <string>
#include "erl_nif.h"
#include "filter.h"
#include "extractor.h"

namespace eleveldb {
    namespace filter {
        static const char* const EQ_OP    = "=";
        static const char* const EQEQ_OP  = "==";
        static const char* const NEQ_OP   = "!=";
        static const char* const LT_OP    = "<";
        static const char* const GT_OP    = ">";
        static const char* const LTE_OP   = "<=";
        static const char* const ELT_OP   = "=<";
        static const char* const GTE_OP   = ">=";

        static const char* const AND_OP   = "and";
        static const char* const AND__OP  = "and_";
        static const char* const OR_OP    = "or";
        static const char* const OR__OP   = "or_";
        static const char* const FIELD_OP = "field";
        static const char* const CONST_OP = "const";
    }
}

ExpressionNode<bool>* parse_range_filter_opts(ErlNifEnv* env, ERL_NIF_TERM options, ExtractorMap& ext, bool throwIfInvalid);

template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM root, ExtractorMap& ext, bool throwIfInvalid);

template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM rooti, ExtractorMap& ext, bool throwIfInvalid);

template<typename T>
ExpressionNode<T>* parse_field_expr(ErlNifEnv* env, ERL_NIF_TERM operand, ExtractorMap& ext);

template<typename T>
ExpressionNode<T>* parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand, ExtractorMap& ext);


ExpressionNode<bool>* parse_equals_expr( ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, ExtractorMap& ext, bool throwIfInvalid);
ExpressionNode<bool>* parse_nequals_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, ExtractorMap& ext, bool throwIfInvalid);
ExpressionNode<bool>* parse_lt_expr(     ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, ExtractorMap& ext, bool throwIfInvalid);
ExpressionNode<bool>* parse_lte_expr(    ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, ExtractorMap& ext, bool throwIfInvalid);
ExpressionNode<bool>* parse_gt_expr(     ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, ExtractorMap& ext, bool throwIfInvalid);
ExpressionNode<bool>* parse_gte_expr(    ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, ExtractorMap& ext, bool throwIfInvalid);
ExpressionNode<bool>* parse_and_expr(    ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, ExtractorMap& ext, bool throwIfInvalid);
ExpressionNode<bool>* parse_or_expr(     ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, ExtractorMap& ext, bool throwIfInvalid);
#endif
