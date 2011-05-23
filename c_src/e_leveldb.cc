// -------------------------------------------------------------------
//
// e_leveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011 Basho Technologies, Inc. All Rights Reserved.
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
#include "e_leveldb.h"

#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/write_batch.h"
#include "leveldb/cache.h"

static ErlNifResourceType* e_leveldb_db_RESOURCE;
static ErlNifResourceType* e_leveldb_itr_RESOURCE;

typedef struct
{
    leveldb::DB* db;
    leveldb::Options options;
} e_leveldb_db_handle;

typedef struct
{
    leveldb::Iterator*   itr;
    ErlNifMutex*         itr_lock;
    const leveldb::Snapshot*   snapshot;
    e_leveldb_db_handle* db_handle;
} e_leveldb_itr_handle;

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_CREATE_IF_MISSING;
static ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;
static ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
static ERL_NIF_TERM ATOM_MAX_OPEN_FILES;
static ERL_NIF_TERM ATOM_BLOCK_SIZE;
static ERL_NIF_TERM ATOM_BLOCK_RESTART_INTERVAL;
static ERL_NIF_TERM ATOM_ERROR_DB_OPEN;
static ERL_NIF_TERM ATOM_ERROR_DB_PUT;
static ERL_NIF_TERM ATOM_NOT_FOUND;
static ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS;
static ERL_NIF_TERM ATOM_FILL_CACHE;
static ERL_NIF_TERM ATOM_SYNC;
static ERL_NIF_TERM ATOM_ERROR_DB_DELETE;
static ERL_NIF_TERM ATOM_CLEAR;
static ERL_NIF_TERM ATOM_PUT;
static ERL_NIF_TERM ATOM_DELETE;
static ERL_NIF_TERM ATOM_ERROR_DB_WRITE;
static ERL_NIF_TERM ATOM_BAD_WRITE_ACTION;
static ERL_NIF_TERM ATOM_KEEP_RESOURCE_FAILED;
static ERL_NIF_TERM ATOM_ITERATOR_CLOSED;
static ERL_NIF_TERM ATOM_FIRST;
static ERL_NIF_TERM ATOM_LAST;
static ERL_NIF_TERM ATOM_NEXT;
static ERL_NIF_TERM ATOM_PREV;
static ERL_NIF_TERM ATOM_INVALID_ITERATOR;
static ERL_NIF_TERM ATOM_CACHE_SIZE;
static ERL_NIF_TERM ATOM_PARANOID_CHECKS;
 

static ErlNifFunc nif_funcs[] =
{
    {"open", 2, e_leveldb_open},
    {"get", 3, e_leveldb_get},
    {"write", 3, e_leveldb_write},
    {"iterator", 2, e_leveldb_iterator},
    {"iterator_move", 2, e_leveldb_iterator_move},
    {"iterator_close", 1, e_leveldb_iterator_close},
    {"status", 2, e_leveldb_status},
/*    {"destroy", 2, e_leveldb_destroy},
    {"repair", 2, e_leveldb_repair} */
};

ERL_NIF_TERM parse_open_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::Options& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
    {
        if (option[0] == ATOM_CREATE_IF_MISSING)
            opts.create_if_missing = (option[1] == ATOM_TRUE);
        else if (option[0] == ATOM_ERROR_IF_EXISTS)
            opts.error_if_exists = (option[1] == ATOM_TRUE);
        else if (option[0] == ATOM_PARANOID_CHECKS) 
            opts.paranoid_checks = (option[1] == ATOM_TRUE);
        else if (option[0] == ATOM_MAX_OPEN_FILES) 
        {
            int max_open_files;
            if (enif_get_int(env, option[1], &max_open_files))
                opts.max_open_files = max_open_files;
        }
        else if (option[0] == ATOM_WRITE_BUFFER_SIZE) 
        { 
            size_t write_buffer_sz;
            if (enif_get_ulong(env, option[1], &write_buffer_sz))
                opts.write_buffer_size = write_buffer_sz;
        }
        else if (option[0] == ATOM_BLOCK_SIZE) 
        { 
            size_t block_sz;
            if (enif_get_ulong(env, option[1], &block_sz)) 
                opts.block_size = block_sz;
        }
        else if (option[0] == ATOM_BLOCK_RESTART_INTERVAL) 
        { 
            int block_restart_interval;
            if (enif_get_int(env, option[1], &block_restart_interval))
                opts.block_restart_interval = block_restart_interval;
        }
        else if (option[0] == ATOM_CACHE_SIZE) 
        {
            size_t cache_sz;
            if (enif_get_ulong(env, option[1], &cache_sz)) 
                if (cache_sz != 0) 
                    opts.block_cache = leveldb::NewLRUCache(cache_sz);
        }
    }

    return ATOM_OK;
}

ERL_NIF_TERM parse_read_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::ReadOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
    {
        if (option[0] == ATOM_VERIFY_CHECKSUMS)
            opts.verify_checksums = (option[1] == ATOM_TRUE);
        else if (option[0] == ATOM_FILL_CACHE)
            opts.fill_cache = (option[1] == ATOM_TRUE);
    }

    return ATOM_OK;
}

ERL_NIF_TERM parse_write_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::WriteOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
    {
        if (option[0] == ATOM_SYNC)
            opts.sync = (option[1] == ATOM_TRUE);
    }

    return ATOM_OK;
}

ERL_NIF_TERM write_batch_item(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::WriteBatch& batch)
{
    int arity;
    const ERL_NIF_TERM* action;
    if (enif_get_tuple(env, item, &arity, &action) ||
        enif_is_atom(env, item))
    {
        if (item == ATOM_CLEAR)
        {
            batch.Clear();
            return ATOM_OK;
        }

        ErlNifBinary key, value;

        if (action[0] == ATOM_PUT && arity == 3 &&
            enif_inspect_binary(env, action[1], &key) &&
            enif_inspect_binary(env, action[2], &value))
        {
            leveldb::Slice key_slice((const char*)key.data, key.size);
            leveldb::Slice value_slice((const char*)value.data, value.size);
            batch.Put(key_slice, value_slice);
            return ATOM_OK;
        }

        if (action[0] == ATOM_DELETE && arity == 2 &&
            enif_inspect_binary(env, action[1], &key))
        {
            leveldb::Slice key_slice((const char*)key.data, key.size);
            batch.Delete(key_slice);
            return ATOM_OK;
        }
    }

    // Failed to match clear/put/delete; return the failing item
    return item;
}

template <typename Acc> ERL_NIF_TERM fold(ErlNifEnv* env, ERL_NIF_TERM list,
                                          ERL_NIF_TERM(*fun)(ErlNifEnv*, ERL_NIF_TERM, Acc&),
                                          Acc& acc)
{
    ERL_NIF_TERM head, tail = list;
    while (enif_get_list_cell(env, tail, &head, &tail))
    {
        ERL_NIF_TERM result = fun(env, head, acc);
        if (result != ATOM_OK)
        {
            return result;
        }
    }

    return ATOM_OK;
}


ERL_NIF_TERM error_tuple(ErlNifEnv* env, ERL_NIF_TERM error, leveldb::Status& status)
{
    ERL_NIF_TERM reason = enif_make_string(env, status.ToString().c_str(),
                                           ERL_NIF_LATIN1);
    return enif_make_tuple2(env, ATOM_ERROR,
                            enif_make_tuple2(env, error, reason));
}

ERL_NIF_TERM e_leveldb_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name[4096];
    if (enif_get_string(env, argv[0], name, sizeof(name), ERL_NIF_LATIN1) &&
        enif_is_list(env, argv[1]))
    {
        // Parse out the options
        leveldb::Options opts;
        fold(env, argv[1], parse_open_option, opts);

        // Open the database
        leveldb::DB* db;
        leveldb::Status status = leveldb::DB::Open(opts, name, &db);
        if (!status.ok())
        {
            return error_tuple(env, ATOM_ERROR_DB_OPEN, status);
        }

        // Setup handle
        e_leveldb_db_handle* handle =
            (e_leveldb_db_handle*) enif_alloc_resource(e_leveldb_db_RESOURCE,
                                                       sizeof(e_leveldb_db_handle));
        memset(handle, '\0', sizeof(e_leveldb_db_handle));
        handle->db = db;
        handle->options = opts;
        ERL_NIF_TERM result = enif_make_resource(env, handle);
        enif_release_resource(handle);
        return enif_make_tuple2(env, ATOM_OK, result);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM e_leveldb_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    e_leveldb_db_handle* handle;
    ErlNifBinary key;
    if (enif_get_resource(env, argv[0], e_leveldb_db_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_is_list(env, argv[2]))
    {
        leveldb::DB* db = handle->db;
        leveldb::Slice key_slice((const char*)key.data, key.size);

        // Parse out the read options
        leveldb::ReadOptions opts;
        fold(env, argv[2], parse_read_option, opts);

        // The DB* does provide a Get() method, but that requires us to copy the
        // value first to a string value and then into an erlang binary. A
        // little digging reveals that Get() is (currently) a convenience
        // wrapper around iterators. So, drop into iterators and avoid that
        // unnecessary alloc/copy/free of the value
        leveldb::Iterator* itr = db->NewIterator(opts);
        itr->Seek(key_slice);
        if (itr->Valid() && handle->options.comparator->Compare(key_slice, itr->key()) == 0)
        {
            // Exact match on our key. Allocate a binary for the result
            leveldb::Slice v = itr->value();
            ERL_NIF_TERM value_bin;
            unsigned char* value = enif_make_new_binary(env, v.size(), &value_bin);
            memcpy(value, v.data(), v.size());

            delete itr;
            return enif_make_tuple2(env, ATOM_OK, value_bin);
        }
        else
        {
            // Either iterator was invalid OR comparison was not exact. Either way,
            // we didn't find the value
            delete itr;
            return ATOM_NOT_FOUND;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM e_leveldb_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    e_leveldb_db_handle* handle;
    if (enif_get_resource(env, argv[0], e_leveldb_db_RESOURCE, (void**)&handle) &&
        enif_is_list(env, argv[1]) && // Actions
        enif_is_list(env, argv[2]))   // Opts
    {
        // Traverse actions and build a write batch
        leveldb::WriteBatch batch;
        ERL_NIF_TERM result = fold(env, argv[1], write_batch_item, batch);
        if (result == ATOM_OK)
        {
            // Was able to fold across all items cleanly -- apply the batch

            // Parse out the write options
            leveldb::WriteOptions opts;
            fold(env, argv[2], parse_write_option, opts);

            // TODO: Why does the API want a WriteBatch* versus a ref?
            leveldb::Status status = handle->db->Write(opts, &batch);
            if (status.ok())
            {
                return ATOM_OK;
            }
            else
            {
                return error_tuple(env, ATOM_ERROR_DB_WRITE, status);
            }
        }
        else
        {
            // Failed to parse out batch commands; bad item was returned from fold.
            return enif_make_tuple2(env, ATOM_ERROR,
                                    enif_make_tuple2(env, ATOM_BAD_WRITE_ACTION,
                                                     result));
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM e_leveldb_iterator(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    e_leveldb_db_handle* db_handle;
    if (enif_get_resource(env, argv[0], e_leveldb_db_RESOURCE, (void**)&db_handle) &&
        enif_is_list(env, argv[1])) // Options
    {
        // Increment references to db_handle for duration of the iterator
        enif_keep_resource(db_handle);

        // Parse out the read options
        leveldb::ReadOptions opts;
        fold(env, argv[1], parse_read_option, opts);

        // Setup handle
        e_leveldb_itr_handle* itr_handle =
            (e_leveldb_itr_handle*) enif_alloc_resource(e_leveldb_itr_RESOURCE,
                                                        sizeof(e_leveldb_itr_handle));
        memset(itr_handle, '\0', sizeof(e_leveldb_itr_handle));

        // Initialize itr handle
        // TODO: Should it be possible to iterate WITHOUT a snapshot?
        itr_handle->itr_lock = enif_mutex_create((char*)"e_leveldb_itr_lock");
        itr_handle->db_handle = db_handle;
        itr_handle->snapshot = db_handle->db->GetSnapshot();
        opts.snapshot = itr_handle->snapshot;
        itr_handle->itr = db_handle->db->NewIterator(opts);

        ERL_NIF_TERM result = enif_make_resource(env, itr_handle);
        enif_release_resource(itr_handle);
        return enif_make_tuple2(env, ATOM_OK, result);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM slice_to_binary(ErlNifEnv* env, leveldb::Slice s)
{
    ERL_NIF_TERM result;
    unsigned char* value = enif_make_new_binary(env, s.size(), &result);
    memcpy(value, s.data(), s.size());
    return result;
}

ERL_NIF_TERM e_leveldb_iterator_move(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    e_leveldb_itr_handle* itr_handle;
    if (enif_get_resource(env, argv[0], e_leveldb_itr_RESOURCE, (void**)&itr_handle))
    {
        enif_mutex_lock(itr_handle->itr_lock);

        leveldb::Iterator* itr = itr_handle->itr;

        if (itr == NULL)
        {
            enif_mutex_unlock(itr_handle->itr_lock);
            return enif_make_tuple2(env, ATOM_ERROR, ATOM_ITERATOR_CLOSED);
        }

        ErlNifBinary key;

        if (argv[1] == ATOM_FIRST)
        {
            itr->SeekToFirst();
        }
        else if (argv[1] == ATOM_LAST)
        {
            itr->SeekToLast();
        }
        else if (argv[1] == ATOM_NEXT && itr->Valid())
        {
            itr->Next();
        }
        else if (argv[1] == ATOM_PREV && itr->Valid())
        {
            itr->Prev();
        }
        else if (enif_inspect_binary(env, argv[1], &key))
        {
            leveldb::Slice key_slice((const char*)key.data, key.size);
            itr->Seek(key_slice);
        }

        ERL_NIF_TERM result;
        if (itr->Valid())
        {
            result = enif_make_tuple3(env, ATOM_OK,
                                      slice_to_binary(env, itr->key()),
                                      slice_to_binary(env, itr->value()));
        }
        else
        {
            result = enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);
        }

        enif_mutex_unlock(itr_handle->itr_lock);
        return result;
    }
    else
    {
        return enif_make_badarg(env);
    }
}


ERL_NIF_TERM e_leveldb_iterator_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    e_leveldb_itr_handle* itr_handle;
    if (enif_get_resource(env, argv[0], e_leveldb_itr_RESOURCE, (void**)&itr_handle))
    {
        enif_mutex_lock(itr_handle->itr_lock);

        if (itr_handle->itr != 0)
        {
            delete itr_handle->itr;
            itr_handle->itr = 0;
            itr_handle->db_handle->db->ReleaseSnapshot(itr_handle->snapshot);
            enif_release_resource(itr_handle->db_handle);
        }

        enif_mutex_unlock(itr_handle->itr_lock);
        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM e_leveldb_status(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    e_leveldb_db_handle* db_handle;
    ErlNifBinary name_bin;
    if (enif_get_resource(env, argv[0], e_leveldb_db_RESOURCE, (void**)&db_handle) &&
        enif_inspect_binary(env, argv[1], &name_bin))
    {
        leveldb::Slice name((const char*)name_bin.data, name_bin.size);
        std::string value;
        if (db_handle->db->GetProperty(name, &value))
        {
            ERL_NIF_TERM result;
            unsigned char* result_buf = enif_make_new_binary(env, value.size(), &result);
            memcpy(result_buf, value.c_str(), value.size());
            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
            return ATOM_ERROR;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

static void e_leveldb_db_resource_cleanup(ErlNifEnv* env, void* arg)
{
    // Delete any dynamically allocated memory stored in e_leveldb_db_handle
    e_leveldb_db_handle* handle = (e_leveldb_db_handle*)arg;
    delete handle->db;
}

static void e_leveldb_itr_resource_cleanup(ErlNifEnv* env, void* arg)
{
    // Delete any dynamically allocated memory stored in e_leveldb_itr_handle
    e_leveldb_itr_handle* itr_handle = (e_leveldb_itr_handle*)arg;
    if (itr_handle->itr != 0)
    {
        delete itr_handle->itr;
        itr_handle->itr = 0;
        itr_handle->db_handle->db->ReleaseSnapshot(itr_handle->snapshot);
        enif_release_resource(itr_handle->db_handle);
    }

    enif_mutex_destroy(itr_handle->itr_lock);
}

#define ATOM(Id, Value) { Id = enif_make_atom(env, Value); }

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    e_leveldb_db_RESOURCE = enif_open_resource_type(env, NULL, "e_leveldb_db_resource",
                                                    &e_leveldb_db_resource_cleanup,
                                                    flags, NULL);
    e_leveldb_itr_RESOURCE = enif_open_resource_type(env, NULL, "e_leveldb_itr_resource",
                                                     &e_leveldb_itr_resource_cleanup,
                                                     flags, NULL);

    // Initialize common atoms
    ATOM(ATOM_OK, "ok");
    ATOM(ATOM_ERROR, "error");
    ATOM(ATOM_TRUE, "true");
    ATOM(ATOM_CREATE_IF_MISSING, "create_if_missing");
    ATOM(ATOM_ERROR_IF_EXISTS, "error_if_exists");
    ATOM(ATOM_WRITE_BUFFER_SIZE, "write_buffer_size");
    ATOM(ATOM_MAX_OPEN_FILES, "max_open_files");
    ATOM(ATOM_BLOCK_SIZE, "block_size");
    ATOM(ATOM_BLOCK_RESTART_INTERVAL, "block_restart_interval");
    ATOM(ATOM_ERROR_DB_OPEN,"db_open");
    ATOM(ATOM_ERROR_DB_PUT, "db_put");
    ATOM(ATOM_NOT_FOUND, "not_found");
    ATOM(ATOM_VERIFY_CHECKSUMS, "verify_checksums");
    ATOM(ATOM_FILL_CACHE,"fill_cache");
    ATOM(ATOM_SYNC, "sync");
    ATOM(ATOM_ERROR_DB_DELETE, "db_delete");
    ATOM(ATOM_CLEAR, "clear");
    ATOM(ATOM_PUT, "put");
    ATOM(ATOM_DELETE, "delete");
    ATOM(ATOM_ERROR_DB_WRITE, "db_write");
    ATOM(ATOM_BAD_WRITE_ACTION, "bad_write_action");
    ATOM(ATOM_KEEP_RESOURCE_FAILED, "keep_resource_failed");
    ATOM(ATOM_ITERATOR_CLOSED, "iterator_closed");
    ATOM(ATOM_FIRST, "first");
    ATOM(ATOM_LAST, "last");
    ATOM(ATOM_NEXT, "next");
    ATOM(ATOM_PREV, "prev");
    ATOM(ATOM_INVALID_ITERATOR, "invalid_iterator");
    ATOM(ATOM_CACHE_SIZE, "cache_size");
    ATOM(ATOM_PARANOID_CHECKS, "paranoid_checks");

    return 0;
}

extern "C" {
    ERL_NIF_INIT(e_leveldb, nif_funcs, &on_load, NULL, NULL, NULL);
}
