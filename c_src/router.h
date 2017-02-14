// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
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

#ifndef INCL_ROUTER_H
#define INCL_ROUTER_H

#ifndef ATOMS_H
    #include "atoms.h"
#endif

// options.h brings in expiry.h
#include "leveldb/options.h"

namespace eleveldb {

// leveldb's interface to Riak functions
bool leveldb_callback(leveldb::EleveldbRouterActions_t, int , const void **);

ERL_NIF_TERM property_cache(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM set_metadata_pid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

extern ERL_NIF_TERM gCallbackRouterPid;

} // namespace eleveldb


#endif  // INCL_ROUTER_H
