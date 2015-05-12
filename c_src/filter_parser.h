#ifndef INCL_FILTER_PARSER_H
#define INCL_FILTER_PARSER_H

#include <string>
#include "erl_nif.h"
#include "filter.h"


namespace eleveldb {
    namespace filter {
        static const char* const EQ_OP = "=";
        static const char* const FIELD_OP = "field";
        static const char* const CONST_OP = "const";
    }
}


template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM root);
template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM* root);
ExpressionNode<bool>* parse_equals_expr(ErlNifEnv* env, ERL_NIF_TERM operands);
template<typename T>
ExpressionNode<T>* parse_field_expr(ErlNifEnv* env, ERL_NIF_TERM operand);
template<typename T>
ExpressionNode<T>* parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand);


ExpressionNode<bool>* parse_range_filter_opts(ErlNifEnv* env, ERL_NIF_TERM options) {
    int arity;
    const ERL_NIF_TERM* filter;
    if (enif_get_tuple(env, options, &arity, &filter) && 2 == arity) {
        return parse_expression_node<bool>(env, filter[1]);
    }
    return nullptr;
}

template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM* root) {
    char op[20];
    if (enif_get_string(env, root[0], op, sizeof(op), ERL_NIF_LATIN1)) {
        printf("ELEVEL OPERATOR: %s\n", op);
        if (strcmp(op, eleveldb::filter::EQ_OP)==0) {
            return parse_equals_expr(env, root[1]);
        }
        if (strcmp(op, eleveldb::filter::FIELD_OP)==0) {
            return parse_field_expr<double>(env, root[1]);
        }
        if (strcmp(op, eleveldb::filter::CONST_OP) == 0) {
            return parse_const_expr<double>(env, root[1]);
        }
    }
}

ExpressionNode<bool>* parse_equals_expr(ErlNifEnv* env, ERL_NIF_TERM operands) {
    unsigned int oplen;
    ERL_NIF_TERM lhs, rhs;
    if (enif_get_list_length(env, operands, &oplen) && oplen==2) {
        enif_get_list_cell(env, operands, &lhs, &rhs);
        enif_get_list_cell(env, operands, &rhs, &rhs);
        return new EqOperator<double>(parse_expression_node<double>(env, lhs), 
                parse_expression_node<double>(env, rhs));
    }
    return nullptr;
}

template<typename T>
ExpressionNode<T>* parse_field_expr(ErlNifEnv* env, ERL_NIF_TERM operand) {
    char field_name[255];
    if (enif_get_string(env, operand, field_name, sizeof(field_name), ERL_NIF_LATIN1)) {
        return new FieldValue<T>(field_name);
    }
    return nullptr;
}

template<typename T>
ExpressionNode<T>* parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand) {
    return nullptr;
}

#endif
