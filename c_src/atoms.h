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

#ifndef ATOMS_H
#define ATOMS_H

namespace eleveldb {

// Atoms (initialized in on_load)
extern ERL_NIF_TERM ATOM_TRUE;
extern ERL_NIF_TERM ATOM_FALSE;
extern ERL_NIF_TERM ATOM_OK;
extern ERL_NIF_TERM ATOM_ERROR;
extern ERL_NIF_TERM ATOM_EINVAL;
extern ERL_NIF_TERM ATOM_BADARG;
extern ERL_NIF_TERM ATOM_CREATE_IF_MISSING;
extern ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;
extern ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
extern ERL_NIF_TERM ATOM_MAX_OPEN_FILES;
extern ERL_NIF_TERM ATOM_BLOCK_SIZE;                    /* DEPRECATED */
extern ERL_NIF_TERM ATOM_SST_BLOCK_SIZE;
extern ERL_NIF_TERM ATOM_BLOCK_RESTART_INTERVAL;
extern ERL_NIF_TERM ATOM_ERROR_DB_OPEN;
extern ERL_NIF_TERM ATOM_ERROR_DB_PUT;
extern ERL_NIF_TERM ATOM_NOT_FOUND;
extern ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS;
extern ERL_NIF_TERM ATOM_FILL_CACHE;
extern ERL_NIF_TERM ATOM_SYNC;
extern ERL_NIF_TERM ATOM_ERROR_DB_DELETE;
extern ERL_NIF_TERM ATOM_CLEAR;
extern ERL_NIF_TERM ATOM_PUT;
extern ERL_NIF_TERM ATOM_DELETE;
extern ERL_NIF_TERM ATOM_ERROR_DB_WRITE;
extern ERL_NIF_TERM ATOM_BAD_WRITE_ACTION;
extern ERL_NIF_TERM ATOM_KEEP_RESOURCE_FAILED;
extern ERL_NIF_TERM ATOM_ITERATOR_CLOSED;
extern ERL_NIF_TERM ATOM_FIRST;
extern ERL_NIF_TERM ATOM_LAST;
extern ERL_NIF_TERM ATOM_NEXT;
extern ERL_NIF_TERM ATOM_PREV;
extern ERL_NIF_TERM ATOM_INVALID_ITERATOR;
extern ERL_NIF_TERM ATOM_CACHE_SIZE;
extern ERL_NIF_TERM ATOM_PARANOID_CHECKS;
extern ERL_NIF_TERM ATOM_ERROR_DB_DESTROY;
extern ERL_NIF_TERM ATOM_KEYS_ONLY;
extern ERL_NIF_TERM ATOM_COMPRESSION;
extern ERL_NIF_TERM ATOM_ERROR_DB_REPAIR;
extern ERL_NIF_TERM ATOM_USE_BLOOMFILTER;

}   // namespace eleveldb


// Erlang helpers:

ERL_NIF_TERM error_einval(ErlNifEnv* env);

template <typename Acc> ERL_NIF_TERM fold(ErlNifEnv* env, ERL_NIF_TERM list,
                                          ERL_NIF_TERM(*fun)(ErlNifEnv*, ERL_NIF_TERM, Acc&),
                                          Acc& acc)
{
    ERL_NIF_TERM head, tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail))
    {
        ERL_NIF_TERM result = fun(env, head, acc);
        if (result != eleveldb::ATOM_OK)
        {
            return result;
        }
    }

    return eleveldb::ATOM_OK;
}





#endif // ATOMS_H
