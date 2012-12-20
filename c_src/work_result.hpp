#ifndef __WORK_RESULT_HPP
 #define __WORK_RESULT_HPP 1

#include "erl_nif.h"

namespace basho { namespace async_nif {

/* Type returned from functors (needs to be fleshed out a bit...): */
class work_result
{
 ERL_NIF_TERM _result;

 public:

 // Literally copy a single term:
 work_result(const ERL_NIF_TERM& result_)
  : _result(result_)
 {}

 // Make tuples:
 work_result(ErlNifEnv *env, const ERL_NIF_TERM& p0)
 {
    _result = enif_make_tuple1(env, p0);
 }

 work_result(ErlNifEnv *env, const ERL_NIF_TERM& p0, const ERL_NIF_TERM& p1)
 {
    _result = enif_make_tuple2(env, p0, p1);
 }

 work_result(ErlNifEnv *env, const ERL_NIF_TERM& p0, const ERL_NIF_TERM& p1, const ERL_NIF_TERM& p2)
 {
    _result = enif_make_tuple3(env, p0, p1, p2);
 }

 public:
 const ERL_NIF_TERM& result() const { return _result; }
};

}} // namespace basho::async_nif

#endif
