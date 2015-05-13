#ifndef INCL_FILTER_PARSER_H
#define INCL_FILTER_PARSER_H

#include <string>
#include "erl_nif.h"
#include "filter.h"


namespace eleveldb {
    namespace filter {
        static const char* const EQ_OP = "==";
        static const char* const FIELD_OP = "field";
        static const char* const CONST_OP = "const";
    }
}


template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM root);
template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM root);
ExpressionNode<bool>* parse_equals_expr(ErlNifEnv* env, ERL_NIF_TERM operands);
template<typename T>
ExpressionNode<T>* parse_field_expr(ErlNifEnv* env, ERL_NIF_TERM operand);
template<typename T>
ExpressionNode<T>* parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand);

template<typename T>
ExpressionNode<T>* parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand) {
    printf("Called the wrong version of parce_constexpr\n");
    return nullptr;
}

template<> ExpressionNode<double>* parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand) {
    double val;
    enif_get_double(env, operand, &val);
    printf("Returning a new ConstantValue\n");
    return new ConstantValue<double>(val);
}

template<typename T>
ExpressionNode<T>* parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM root) {
    char op[20];
    const ERL_NIF_TERM* op_args;
    int arity;
    if (enif_get_tuple(env, root, &arity, &op_args) && arity==2) {
        if (enif_get_atom(env, op_args[0], op, sizeof(op), ERL_NIF_LATIN1)) {
            if (strcmp(op, eleveldb::filter::CONST_OP)==0) {
                return parse_const_expr<T>(env, op_args[1]);
            }
            if (strcmp(op, eleveldb::filter::FIELD_OP)==0) {
                return parse_field_expr<T>(env, op_args[1]);
            }
        }
    }
    return nullptr;
}

template<> ExpressionNode<bool>* parse_expression_node<bool>(ErlNifEnv* env, ERL_NIF_TERM root) {
char op[20];
    const ERL_NIF_TERM* op_args;
    int arity;
    if (enif_get_tuple(env, root, &arity, &op_args) && arity==2) {
        if (enif_get_string(env, op_args[0], op, sizeof(op), ERL_NIF_LATIN1)) {
            if (strcmp(op, eleveldb::filter::EQ_OP)==0) {
                return parse_equals_expr(env, op_args[1]);
            }
        }
    }
    return nullptr;
}

ExpressionNode<bool>* parse_equals_expr(ErlNifEnv* env, ERL_NIF_TERM operands) {
    unsigned int oplen;
    ERL_NIF_TERM lhs, rhs, rest = operands;
    if (enif_get_list_length(env, operands, &oplen) && oplen==2) {
        if (enif_get_list_cell(env, rest, &lhs, &rest) &&
                enif_get_list_cell(env, rest, &rhs, &rest)) {
            return new EqOperator<double>(parse_expression_node<double>(env, lhs),
                    parse_expression_node<double>(env, rhs));
        }
    }
    return nullptr;
}

template<typename T>
ExpressionNode<T>* parse_field_expr(ErlNifEnv* env, ERL_NIF_TERM operand) {
    char field_name[255];
    if (enif_get_string(env, operand, field_name, sizeof(field_name), ERL_NIF_LATIN1)) {
        printf("Returning a new FieldValue\n");
        return new FieldValue<T>(field_name);
    }
    return nullptr;
}



ExpressionNode<bool>* parse_range_filter_opts(ErlNifEnv* env, ERL_NIF_TERM options) {
    return parse_expression_node<bool>(env, options);
}


#endif
