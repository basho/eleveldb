// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------


#ifndef __WORK_RESULT_HPP
 #define __WORK_RESULT_HPP 1

#include "erl_nif.h"
#include "leveldb/status.h"

#ifndef ATOMS_H
    #include "atoms.h"
#endif

namespace basho { namespace async_nif {

/* Type returned from functors (needs to be fleshed out a bit...): */
class work_result
{
 ERL_NIF_TERM _result;
 bool _is_set;

 public:
  work_result()
   : _is_set(false)
  {};

 // Literally copy a single term:
 work_result(const ERL_NIF_TERM& result_)
  : _result(result_), _is_set(true)
 {}

 // Make tuples:
 work_result(ErlNifEnv *env, const ERL_NIF_TERM& p0)
  : _is_set(true)
 {
    _result = enif_make_tuple1(env, p0);
 }

 work_result(ErlNifEnv *env, const ERL_NIF_TERM& p0, const ERL_NIF_TERM& p1)
  : _is_set(true)
 {
    _result = enif_make_tuple2(env, p0, p1);
 }

work_result(ErlNifEnv *env, const ERL_NIF_TERM& error, leveldb::Status& status)
  : _is_set(true)
 {
    ERL_NIF_TERM reason = enif_make_string(env, status.ToString().c_str(),
                                           ERL_NIF_LATIN1);
    _result = enif_make_tuple2(env, eleveldb::ATOM_ERROR,
                            enif_make_tuple2(env, error, reason));
 }

 work_result(ErlNifEnv *env, const ERL_NIF_TERM& p0, const ERL_NIF_TERM& p1, const ERL_NIF_TERM& p2)
  : _is_set(true)
 {
    _result = enif_make_tuple3(env, p0, p1, p2);
 }

 public:
 const ERL_NIF_TERM& result() const { return _result; }
 bool is_set() const {return(_is_set);};
};

}} // namespace basho::async_nif

#endif
