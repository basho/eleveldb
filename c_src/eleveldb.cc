// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2014 Basho Technologies, Inc. All Rights Reserved.
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

#include <syslog.h>

#include <new>
#include <set>
#include <stack>
#include <deque>
#include <sstream>
#include <utility>
#include <stdexcept>
#include <algorithm>
#include <vector>

#include "eleveldb.h"

#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/perf_count.h"

#ifndef INCL_THREADING_H
    #include "threading.h"
#endif

#ifndef INCL_WORKITEMS_H
    #include "workitems.h"
#endif

#ifndef ATOMS_H
    #include "atoms.h"
#endif

#include "work_result.hpp"

#include "detail.hpp"

static ErlNifFunc nif_funcs[] =
{
    {"async_close", 2, eleveldb::async_close},
    {"async_iterator_close", 2, eleveldb::async_iterator_close},
    {"status", 2, eleveldb_status},
    {"async_destroy", 3, eleveldb::async_destroy},
    {"repair", 2, eleveldb_repair},
    {"is_empty", 1, eleveldb_is_empty},

    {"async_open", 3, eleveldb::async_open},
    {"async_write", 4, eleveldb::async_write},
    {"async_get", 4, eleveldb::async_get},

    {"async_iterator", 3, eleveldb::async_iterator},
    {"async_iterator", 4, eleveldb::async_iterator},

    {"async_iterator_move", 3, eleveldb::async_iterator_move}
};


namespace eleveldb {

// Atoms (initialized in on_load)
ERL_NIF_TERM ATOM_TRUE;
ERL_NIF_TERM ATOM_FALSE;
ERL_NIF_TERM ATOM_OK;
ERL_NIF_TERM ATOM_ERROR;
ERL_NIF_TERM ATOM_EINVAL;
ERL_NIF_TERM ATOM_BADARG;
ERL_NIF_TERM ATOM_CREATE_IF_MISSING;
ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;
ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
ERL_NIF_TERM ATOM_SST_BLOCK_SIZE;
ERL_NIF_TERM ATOM_BLOCK_SIZE_STEPS;
ERL_NIF_TERM ATOM_BLOCK_RESTART_INTERVAL;
ERL_NIF_TERM ATOM_ERROR_DB_OPEN;
ERL_NIF_TERM ATOM_ERROR_DB_PUT;
ERL_NIF_TERM ATOM_NOT_FOUND;
ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS;
ERL_NIF_TERM ATOM_FILL_CACHE;
ERL_NIF_TERM ATOM_ITERATOR_REFRESH;
ERL_NIF_TERM ATOM_SYNC;
ERL_NIF_TERM ATOM_ERROR_DB_DELETE;
ERL_NIF_TERM ATOM_CLEAR;
ERL_NIF_TERM ATOM_PUT;
ERL_NIF_TERM ATOM_DELETE;
ERL_NIF_TERM ATOM_ERROR_DB_WRITE;
ERL_NIF_TERM ATOM_BAD_WRITE_ACTION;
ERL_NIF_TERM ATOM_KEEP_RESOURCE_FAILED;
ERL_NIF_TERM ATOM_ITERATOR_CLOSED;
ERL_NIF_TERM ATOM_FIRST;
ERL_NIF_TERM ATOM_LAST;
ERL_NIF_TERM ATOM_NEXT;
ERL_NIF_TERM ATOM_PREV;
ERL_NIF_TERM ATOM_PREFETCH;
ERL_NIF_TERM ATOM_PREFETCH_STOP;
ERL_NIF_TERM ATOM_INVALID_ITERATOR;
ERL_NIF_TERM ATOM_PARANOID_CHECKS;
ERL_NIF_TERM ATOM_VERIFY_COMPACTIONS;
ERL_NIF_TERM ATOM_ERROR_DB_DESTROY;
ERL_NIF_TERM ATOM_KEYS_ONLY;
ERL_NIF_TERM ATOM_COMPRESSION;
ERL_NIF_TERM ATOM_ERROR_DB_REPAIR;
ERL_NIF_TERM ATOM_USE_BLOOMFILTER;
ERL_NIF_TERM ATOM_TOTAL_MEMORY;
ERL_NIF_TERM ATOM_TOTAL_LEVELDB_MEM;
ERL_NIF_TERM ATOM_TOTAL_LEVELDB_MEM_PERCENT;
ERL_NIF_TERM ATOM_BLOCK_CACHE_THRESHOLD;
ERL_NIF_TERM ATOM_IS_INTERNAL_DB;
ERL_NIF_TERM ATOM_LIMITED_DEVELOPER_MEM;
ERL_NIF_TERM ATOM_ELEVELDB_THREADS;
ERL_NIF_TERM ATOM_FADVISE_WILLNEED;
ERL_NIF_TERM ATOM_DELETE_THRESHOLD;
ERL_NIF_TERM ATOM_TIERED_SLOW_LEVEL;
ERL_NIF_TERM ATOM_TIERED_FAST_PREFIX;
ERL_NIF_TERM ATOM_TIERED_SLOW_PREFIX;
}   // namespace eleveldb


using std::nothrow;

struct eleveldb_itr_handle;

class eleveldb_thread_pool;
class eleveldb_priv_data;

static volatile uint64_t gCurrentTotalMemory=0;

// Erlang helpers:
ERL_NIF_TERM error_einval(ErlNifEnv* env)
{
    return enif_make_tuple2(env, eleveldb::ATOM_ERROR, eleveldb::ATOM_EINVAL);
}

static ERL_NIF_TERM error_tuple(ErlNifEnv* env, ERL_NIF_TERM error, leveldb::Status& status)
{
    ERL_NIF_TERM reason = enif_make_string(env, status.ToString().c_str(),
                                           ERL_NIF_LATIN1);
    return enif_make_tuple2(env, eleveldb::ATOM_ERROR,
                            enif_make_tuple2(env, error, reason));
}

static ERL_NIF_TERM slice_to_binary(ErlNifEnv* env, leveldb::Slice s)
{
    ERL_NIF_TERM result;
    unsigned char* value = enif_make_new_binary(env, s.size(), &result);
    memcpy(value, s.data(), s.size());
    return result;
}

/** struct for grabbing eleveldb environment options via fold
 *   ... then loading said options into eleveldb_priv_data
 */
struct EleveldbOptions
{
    int m_EleveldbThreads;
    int m_LeveldbImmThreads;
    int m_LeveldbBGWriteThreads;
    int m_LeveldbOverlapThreads;
    int m_LeveldbGroomingThreads;

    int m_TotalMemPercent;
    size_t m_TotalMem;

    bool m_LimitedDeveloper;
    bool m_FadviseWillNeed;

    EleveldbOptions()
        : m_EleveldbThreads(71),
          m_LeveldbImmThreads(0), m_LeveldbBGWriteThreads(0),
          m_LeveldbOverlapThreads(0), m_LeveldbGroomingThreads(0),
          m_TotalMemPercent(0), m_TotalMem(0),
          m_LimitedDeveloper(false), m_FadviseWillNeed(false)
        {};

    void Dump()
    {
        syslog(LOG_ERR, "         m_EleveldbThreads: %d\n", m_EleveldbThreads);
        syslog(LOG_ERR, "       m_LeveldbImmThreads: %d\n", m_LeveldbImmThreads);
        syslog(LOG_ERR, "   m_LeveldbBGWriteThreads: %d\n", m_LeveldbBGWriteThreads);
        syslog(LOG_ERR, "   m_LeveldbOverlapThreads: %d\n", m_LeveldbOverlapThreads);
        syslog(LOG_ERR, "  m_LeveldbGroomingThreads: %d\n", m_LeveldbGroomingThreads);

        syslog(LOG_ERR, "         m_TotalMemPercent: %d\n", m_TotalMemPercent);
        syslog(LOG_ERR, "                m_TotalMem: %zd\n", m_TotalMem);

        syslog(LOG_ERR, "        m_LimitedDeveloper: %s\n", (m_LimitedDeveloper ? "true" : "false"));
        syslog(LOG_ERR, "         m_FadviseWillNeed: %s\n", (m_FadviseWillNeed ? "true" : "false"));
    }   // Dump
};  // struct EleveldbOptions


/** Module-level private data:
 *    singleton instance held by erlang and passed on API calls
 */
class eleveldb_priv_data
{
public:
    EleveldbOptions m_Opts;
    eleveldb::eleveldb_thread_pool thread_pool;

    explicit eleveldb_priv_data(EleveldbOptions & Options)
    : m_Opts(Options), thread_pool(Options.m_EleveldbThreads)
        {}

private:
    eleveldb_priv_data();                                      // no default constructor
    eleveldb_priv_data(const eleveldb_priv_data&);             // nocopy
    eleveldb_priv_data& operator=(const eleveldb_priv_data&);  // nocopyassign

};


ERL_NIF_TERM parse_init_option(ErlNifEnv* env, ERL_NIF_TERM item, EleveldbOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;

    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == eleveldb::ATOM_TOTAL_LEVELDB_MEM)
        {
            size_t memory_sz;
            if (enif_get_ulong(env, option[1], &memory_sz))
            {
                if (memory_sz != 0)
                {
                    opts.m_TotalMem = memory_sz;
                }
            }
        }
        else if (option[0] == eleveldb::ATOM_TOTAL_LEVELDB_MEM_PERCENT)
        {
            unsigned long memory_sz;
            if (enif_get_ulong(env, option[1], &memory_sz))
            {
                if (0 < memory_sz && memory_sz <= 100)
                 {
                     // this gets noticed later and applied against gCurrentTotalMemory
                     opts.m_TotalMemPercent = memory_sz;
                 }
            }
        }
        else if (option[0] == eleveldb::ATOM_LIMITED_DEVELOPER_MEM)
        {
            if (option[1] == eleveldb::ATOM_TRUE)
                opts.m_LimitedDeveloper = true;
            else
                opts.m_LimitedDeveloper = false;
        }
        else if (option[0] == eleveldb::ATOM_ELEVELDB_THREADS)
        {
            unsigned long temp;
            if (enif_get_ulong(env, option[1], &temp))
            {
                if (temp != 0)
                {
                    opts.m_EleveldbThreads = temp;
                }   // if
            }   // if
        }   // if
        else if (option[0] == eleveldb::ATOM_FADVISE_WILLNEED)
        {
            opts.m_FadviseWillNeed = (option[1] == eleveldb::ATOM_TRUE);
        }   // else if
    }

    return eleveldb::ATOM_OK;
}

ERL_NIF_TERM parse_open_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::Options& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == eleveldb::ATOM_CREATE_IF_MISSING)
            opts.create_if_missing = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_ERROR_IF_EXISTS)
            opts.error_if_exists = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_PARANOID_CHECKS)
            opts.paranoid_checks = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_VERIFY_COMPACTIONS)
            opts.verify_compactions = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_WRITE_BUFFER_SIZE)
        {
            unsigned long write_buffer_sz;
            if (enif_get_ulong(env, option[1], &write_buffer_sz))
                opts.write_buffer_size = write_buffer_sz;
        }
        else if (option[0] == eleveldb::ATOM_SST_BLOCK_SIZE)
        {
            unsigned long sst_block_sz(0);
            if (enif_get_ulong(env, option[1], &sst_block_sz))
             opts.block_size = sst_block_sz; // Note: We just set the "old" block_size option.
        }
        else if (option[0] == eleveldb::ATOM_BLOCK_RESTART_INTERVAL)
        {
            int block_restart_interval;
            if (enif_get_int(env, option[1], &block_restart_interval))
                opts.block_restart_interval = block_restart_interval;
        }
        else if (option[0] == eleveldb::ATOM_BLOCK_SIZE_STEPS)
        {
            unsigned long block_steps(0);
            if (enif_get_ulong(env, option[1], &block_steps))
             opts.block_size_steps = block_steps;
        }
        else if (option[0] == eleveldb::ATOM_BLOCK_CACHE_THRESHOLD)
        {
            size_t memory_sz;
            if (enif_get_ulong(env, option[1], &memory_sz))
            {
                if (memory_sz != 0)
                {
                    opts.block_cache_threshold = memory_sz;
                }
            }
        }
        else if (option[0] == eleveldb::ATOM_DELETE_THRESHOLD)
        {
            unsigned long threshold(0);
            if (enif_get_ulong(env, option[1], &threshold))
             opts.delete_threshold = threshold;
        }
        else if (option[0] == eleveldb::ATOM_COMPRESSION)
        {
            if (option[1] == eleveldb::ATOM_TRUE)
            {
                opts.compression = leveldb::kSnappyCompression;
            }
            else
            {
                opts.compression = leveldb::kNoCompression;
            }
        }
        else if (option[0] == eleveldb::ATOM_USE_BLOOMFILTER)
        {
            // By default, we want to use a 16-bit-per-key bloom filter on a
            // per-table basis. We only disable it if explicitly asked. Alternatively,
            // one can provide a value for # of bits-per-key.
            unsigned long bfsize = 16;
            if (option[1] == eleveldb::ATOM_TRUE || enif_get_ulong(env, option[1], &bfsize))
            {
                opts.filter_policy = leveldb::NewBloomFilterPolicy2(bfsize);
            }
        }
        else if (option[0] == eleveldb::ATOM_TOTAL_MEMORY)
        {
            // NOTE: uint64_t memory_sz and enif_get_uint64() do NOT compile
            // correctly on some platforms.  Why?  because it's Erlang.
            unsigned long memory_sz;
            if (enif_get_ulong(env, option[1], &memory_sz))
            {
                // ignoring memory size below 1G, going with defaults
                //  (because Erlang/Riak need 1G to themselves making
                //   percentage of memory unreliable)
                if (1024*1024*1024L < memory_sz)
                {
                    gCurrentTotalMemory = memory_sz;
                }
                // did a dynamic VM just have a memory resize?
                //  just in case reset the global
                else if (0 != memory_sz)
                {
                    gCurrentTotalMemory = 0;
                }   // else if
            }
        }
        else if (option[0] == eleveldb::ATOM_TOTAL_LEVELDB_MEM)
        {
            unsigned long memory_sz;
            if (enif_get_ulong(env, option[1], &memory_sz))
            {
                if (memory_sz != 0)
                 {
                     opts.total_leveldb_mem = memory_sz;
                 }
            }
        }
        else if (option[0] == eleveldb::ATOM_TOTAL_LEVELDB_MEM_PERCENT)
        {
            unsigned long memory_sz;
            if (enif_get_ulong(env, option[1], &memory_sz))
            {
                if (0 < memory_sz && memory_sz <= 100)
                 {
                     // this gets noticed later and applied against gCurrentTotalMemory
                     opts.total_leveldb_mem = memory_sz;
                 }
            }
        }
        else if (option[0] == eleveldb::ATOM_IS_INTERNAL_DB)
        {
            if (option[1] == eleveldb::ATOM_TRUE)
                opts.is_internal_db = true;
            else
                opts.is_internal_db = false;
        }
        else if (option[0] == eleveldb::ATOM_LIMITED_DEVELOPER_MEM)
        {
            if (option[1] == eleveldb::ATOM_TRUE)
                opts.limited_developer_mem = true;
            else
                opts.limited_developer_mem = false;
        }

        else if (option[0] == eleveldb::ATOM_TIERED_SLOW_LEVEL)
        {
            int tiered_level;
            if (enif_get_int(env, option[1], &tiered_level))
                opts.tiered_slow_level = tiered_level;
        }
        else if (option[0] == eleveldb::ATOM_TIERED_FAST_PREFIX)
        {
            char buffer[256];
            int ret_val;

            ret_val=enif_get_string(env, option[1], buffer, 256, ERL_NIF_LATIN1);
            if (0<ret_val && ret_val<256)
                opts.tiered_fast_prefix = buffer;
        }
        else if (option[0] == eleveldb::ATOM_TIERED_SLOW_PREFIX)
        {
            char buffer[256];
            int ret_val;

            ret_val=enif_get_string(env, option[1], buffer, 256, ERL_NIF_LATIN1);
            if (0<ret_val && ret_val<256)
                opts.tiered_slow_prefix = buffer;
        }

    }

    return eleveldb::ATOM_OK;
}

ERL_NIF_TERM parse_read_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::ReadOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == eleveldb::ATOM_VERIFY_CHECKSUMS)
            opts.verify_checksums = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_FILL_CACHE)
            opts.fill_cache = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_ITERATOR_REFRESH)
            opts.iterator_refresh = (option[1] == eleveldb::ATOM_TRUE);
    }

    return eleveldb::ATOM_OK;
}

ERL_NIF_TERM parse_write_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::WriteOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        if (option[0] == eleveldb::ATOM_SYNC)
            opts.sync = (option[1] == eleveldb::ATOM_TRUE);
    }

    return eleveldb::ATOM_OK;
}

ERL_NIF_TERM write_batch_item(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::WriteBatch& batch)
{
    int arity;
    const ERL_NIF_TERM* action;
    if (enif_get_tuple(env, item, &arity, &action) ||
        enif_is_atom(env, item))
    {
        if (item == eleveldb::ATOM_CLEAR)
        {
            batch.Clear();
            return eleveldb::ATOM_OK;
        }

        ErlNifBinary key, value;

        if (action[0] == eleveldb::ATOM_PUT && arity == 3 &&
            enif_inspect_binary(env, action[1], &key) &&
            enif_inspect_binary(env, action[2], &value))
        {
            leveldb::Slice key_slice((const char*)key.data, key.size);
            leveldb::Slice value_slice((const char*)value.data, value.size);
            batch.Put(key_slice, value_slice);
            return eleveldb::ATOM_OK;
        }

        if (action[0] == eleveldb::ATOM_DELETE && arity == 2 &&
            enif_inspect_binary(env, action[1], &key))
        {
            leveldb::Slice key_slice((const char*)key.data, key.size);
            batch.Delete(key_slice);
            return eleveldb::ATOM_OK;
        }
    }

    // Failed to match clear/put/delete; return the failing item
    return item;
}



namespace eleveldb {

ERL_NIF_TERM send_reply(ErlNifEnv *env, ERL_NIF_TERM ref, ERL_NIF_TERM reply)
{
    ErlNifPid pid;
    ErlNifEnv *msg_env = enif_alloc_env();
    ERL_NIF_TERM msg = enif_make_tuple2(msg_env,
                                        enif_make_copy(msg_env, ref),
                                        enif_make_copy(msg_env, reply));
    enif_self(env, &pid);
    enif_send(env, &pid, msg_env, msg);
    enif_free_env(msg_env);
    return ATOM_OK;
}

ERL_NIF_TERM
async_open(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char db_name[4096];

    if(!enif_get_string(env, argv[1], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
       !enif_is_list(env, argv[2]))
    {
        return enif_make_badarg(env);
    }   // if

    ERL_NIF_TERM caller_ref = argv[0];

    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    leveldb::Options *opts = new leveldb::Options;
    fold(env, argv[2], parse_open_option, *opts);
    opts->fadvise_willneed = priv.m_Opts.m_FadviseWillNeed;

    // convert total_leveldb_mem to byte count if it arrived as percent
    //  This happens now because there is no guarantee as to when the total_memory
    //  value would be read relative to total_leveldb_mem_percent in the option fold
    uint64_t use_memory;

    // 1. start with all memory
    use_memory=gCurrentTotalMemory;

    // 2. valid percentage given
    if (0 < priv.m_Opts.m_TotalMemPercent && priv.m_Opts.m_TotalMemPercent<=100)
        use_memory=(priv.m_Opts.m_TotalMemPercent * use_memory)/100;  // integer math for percentage

    // 3. adjust to specific memory size
    if (0!=priv.m_Opts.m_TotalMem)
        use_memory=priv.m_Opts.m_TotalMem;

    // 4. fail safe when no guidance given
    if (0==priv.m_Opts.m_TotalMem && 0==priv.m_Opts.m_TotalMemPercent)
    {
        if (8*1024*1024*1024L < gCurrentTotalMemory)
            use_memory=(gCurrentTotalMemory * 80)/100;  // integer percent
        else
            use_memory=(gCurrentTotalMemory * 25)/100;  // integer percent
    }   // if

    opts->total_leveldb_mem=use_memory;
    opts->limited_developer_mem=priv.m_Opts.m_LimitedDeveloper;

    eleveldb::WorkTask *work_item = new eleveldb::OpenTask(env, caller_ref,
                                                              db_name, opts);

    if(false == priv.thread_pool.submit(work_item))
    {
        delete work_item;
        return send_reply(env, caller_ref,
                          enif_make_tuple2(env, eleveldb::ATOM_ERROR, caller_ref));
    }

    return eleveldb::ATOM_OK;

}   // async_open


ERL_NIF_TERM
async_write(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& caller_ref = argv[0];
    const ERL_NIF_TERM& handle_ref = argv[1];
    const ERL_NIF_TERM& action_ref = argv[2];
    const ERL_NIF_TERM& opts_ref   = argv[3];

    ReferencePtr<DbObject> db_ptr;

    db_ptr.assign(DbObject::RetrieveDbObject(env, handle_ref));

    if(NULL==db_ptr.get()
       || !enif_is_list(env, action_ref)
       || !enif_is_list(env, opts_ref))
    {
        return enif_make_badarg(env);
    }

    // is this even possible?
    if(NULL == db_ptr->m_Db)
        return send_reply(env, caller_ref, error_einval(env));

    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    // Construct a write batch:
    leveldb::WriteBatch* batch = new leveldb::WriteBatch;

    // Seed the batch's data:
    ERL_NIF_TERM result = fold(env, argv[2], write_batch_item, *batch);
    if(eleveldb::ATOM_OK != result)
    {
        return send_reply(env, caller_ref,
                          enif_make_tuple3(env, eleveldb::ATOM_ERROR, caller_ref,
                                           enif_make_tuple2(env, eleveldb::ATOM_BAD_WRITE_ACTION,
                                                            result)));
    }   // if

    leveldb::WriteOptions* opts = new leveldb::WriteOptions;
    fold(env, argv[3], parse_write_option, *opts);

    eleveldb::WorkTask* work_item = new eleveldb::WriteTask(env, caller_ref,
                                                            db_ptr.get(), batch, opts);

    if(false == priv.thread_pool.submit(work_item))
    {
        delete work_item;
        return send_reply(env, caller_ref,
                          enif_make_tuple2(env, eleveldb::ATOM_ERROR, caller_ref));
    }   // if

    return eleveldb::ATOM_OK;
}


ERL_NIF_TERM
async_get(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& caller_ref = argv[0];
    const ERL_NIF_TERM& dbh_ref    = argv[1];
    const ERL_NIF_TERM& key_ref    = argv[2];
    const ERL_NIF_TERM& opts_ref   = argv[3];

    ReferencePtr<DbObject> db_ptr;

    db_ptr.assign(DbObject::RetrieveDbObject(env, dbh_ref));

    if(NULL==db_ptr.get()
       || !enif_is_list(env, opts_ref)
       || !enif_is_binary(env, key_ref))
    {
        return enif_make_badarg(env);
    }

    if(NULL == db_ptr->m_Db)
        return send_reply(env, caller_ref, error_einval(env));

    leveldb::ReadOptions opts;
    fold(env, opts_ref, parse_read_option, opts);

    eleveldb::WorkTask *work_item = new eleveldb::GetTask(env, caller_ref,
                                                          db_ptr.get(), key_ref, opts);

    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    if(false == priv.thread_pool.submit(work_item))
    {
        delete work_item;
        return send_reply(env, caller_ref,
                          enif_make_tuple2(env, eleveldb::ATOM_ERROR, caller_ref));
    }   // if

    return eleveldb::ATOM_OK;

}   // async_get


ERL_NIF_TERM
async_iterator(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& caller_ref  = argv[0];
    const ERL_NIF_TERM& dbh_ref     = argv[1];
    const ERL_NIF_TERM& options_ref = argv[2];

    const bool keys_only = ((argc == 4) && (argv[3] == ATOM_KEYS_ONLY));

    ReferencePtr<DbObject> db_ptr;

    db_ptr.assign(DbObject::RetrieveDbObject(env, dbh_ref));

    if(NULL==db_ptr.get() || 0!=db_ptr->m_CloseRequested
       || !enif_is_list(env, options_ref))
     {
        return enif_make_badarg(env);
     }

    // likely useless
    if(NULL == db_ptr->m_Db)
        return send_reply(env, caller_ref, error_einval(env));

    // Parse out the read options
    leveldb::ReadOptions opts;
    fold(env, options_ref, parse_read_option, opts);

    eleveldb::WorkTask *work_item = new eleveldb::IterTask(env, caller_ref,
                                                           db_ptr.get(), keys_only, opts);

    // Now-boilerplate setup (we'll consolidate this pattern soon, I hope):
    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    if(false == priv.thread_pool.submit(work_item))
    {
        delete work_item;
        return send_reply(env, caller_ref, enif_make_tuple2(env, ATOM_ERROR, caller_ref));
    }   // if

    return ATOM_OK;

}   // async_iterator


ERL_NIF_TERM
async_iterator_move(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    // const ERL_NIF_TERM& caller_ref       = argv[0];
    const ERL_NIF_TERM& itr_handle_ref   = argv[1];
    const ERL_NIF_TERM& action_or_target = argv[2];
    ERL_NIF_TERM ret_term;

    bool submit_new_request(true), prefetch_state;

    ReferencePtr<ItrObject> itr_ptr;

    itr_ptr.assign(ItrObject::RetrieveItrObject(env, itr_handle_ref));

    if(NULL==itr_ptr.get() || 0!=itr_ptr->m_CloseRequested)
        return enif_make_badarg(env);

    // Reuse ref from iterator creation
    const ERL_NIF_TERM& caller_ref = itr_ptr->itr_ref;

    /* We can be invoked with two different arities from Erlang. If our "action_atom" parameter is not
       in fact an atom, then it is actually a seek target. Let's find out which we are: */
    eleveldb::MoveTask::action_t action = eleveldb::MoveTask::SEEK;

    // If we have an atom, it's one of these (action_or_target's value is ignored):
    if(enif_is_atom(env, action_or_target))
    {
        if(ATOM_FIRST == action_or_target)  action = eleveldb::MoveTask::FIRST;
        if(ATOM_LAST == action_or_target)   action = eleveldb::MoveTask::LAST;
        if(ATOM_NEXT == action_or_target)   action = eleveldb::MoveTask::NEXT;
        if(ATOM_PREV == action_or_target)   action = eleveldb::MoveTask::PREV;
        if(ATOM_PREFETCH == action_or_target)   action = eleveldb::MoveTask::PREFETCH;
        if(ATOM_PREFETCH_STOP == action_or_target)   action = eleveldb::MoveTask::PREFETCH_STOP;
    }   // if

    // debug syslog(LOG_ERR, "move state: %d, %d, %d",
    //              action, itr_ptr->m_Iter->m_PrefetchStarted, itr_ptr->m_Iter->m_HandoffAtomic);

    // must set this BEFORE call to compare_and_swap ... or have potential
    //  for an "extra" message coming out of prefetch
    prefetch_state = itr_ptr->m_Iter->m_PrefetchStarted;
    itr_ptr->m_Iter->m_PrefetchStarted =  prefetch_state && (eleveldb::MoveTask::PREFETCH_STOP != action );

    //
    // Three situations:
    //  #1 not a PREFETCH next call
    //  #2 PREFETCH call and no prefetch waiting
    //  #3 PREFETCH call and prefetch is waiting
    //     (PREFETCH_STOP is basically a PREFETCH that turns off prefetch state)

    // case #1
    if (eleveldb::MoveTask::PREFETCH != action
        && eleveldb::MoveTask::PREFETCH_STOP != action )
    {
        // current move object could still be in later stages of
        //  worker thread completion ... race condition ...don't reuse
        itr_ptr->ReleaseReuseMove();

        submit_new_request=true;
        ret_term = enif_make_copy(env, itr_ptr->itr_ref);

        // force reply to be a message
        itr_ptr->m_Iter->m_HandoffAtomic=1;
        itr_ptr->m_Iter->m_PrefetchStarted=false;
    }   // if

    // case #2
    // before we launch a background job for "next iteration", see if there is a
    //  prefetch waiting for us
    else if (eleveldb::compare_and_swap(&itr_ptr->m_Iter->m_HandoffAtomic, 0, 1))
    {
        // nope, no prefetch ... await a message to erlang queue
        ret_term = enif_make_copy(env, itr_ptr->itr_ref);

        // leave m_HandoffAtomic as 1 so first response is via message

        // is this truly a wait for prefetch ... or actually the first prefetch request
        if (!prefetch_state)
        {
            submit_new_request=true;
            itr_ptr->ReleaseReuseMove();
        }   // if

        else
        {
            // await message that is already in the making
            submit_new_request=false;
        }   // else

        // redundant ... but clarifying where it really belongs in logic pattern
        itr_ptr->m_Iter->m_PrefetchStarted=(eleveldb::MoveTask::PREFETCH_STOP != action );
    }   // else if

    // case #3
    else
    {
        // why yes there is.  copy the key/value info into a return tuple before
        //  we launch the iterator for "next" again
        if(!itr_ptr->m_Iter->Valid())
            ret_term=enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);

        else if (itr_ptr->m_Iter->m_KeysOnly)
            ret_term=enif_make_tuple2(env, ATOM_OK, slice_to_binary(env, itr_ptr->m_Iter->key()));
        else
            ret_term=enif_make_tuple3(env, ATOM_OK,
                                      slice_to_binary(env, itr_ptr->m_Iter->key()),
                                      slice_to_binary(env, itr_ptr->m_Iter->value()));


        // reset for next race
        itr_ptr->m_Iter->m_HandoffAtomic=0;

        // old MoveItem could still be active on its thread, cannot
        //  reuse ... but the current Iterator is good
        itr_ptr->ReleaseReuseMove();

        if (eleveldb::MoveTask::PREFETCH_STOP != action )
        {
            submit_new_request=true;
        }   // if
        else
        {
            submit_new_request=false;
            itr_ptr->m_Iter->m_HandoffAtomic=0;
            itr_ptr->m_Iter->m_PrefetchStarted=false;
        }   // else


    }   // else


    // only build request if actually need to submit it
    if (submit_new_request)
    {
        eleveldb::MoveTask * move_item;

        move_item = new eleveldb::MoveTask(env, caller_ref,
                                           itr_ptr->m_Iter.get(), action);

        // prevent deletes during worker loop
        move_item->RefInc();
        itr_ptr->reuse_move=move_item;

        move_item->action=action;

        if (eleveldb::MoveTask::SEEK == action)
        {
            ErlNifBinary key;

            if(!enif_inspect_binary(env, action_or_target, &key))
            {
                itr_ptr->ReleaseReuseMove();
		itr_ptr->reuse_move=NULL;
                return enif_make_tuple2(env, ATOM_EINVAL, caller_ref);
            }   // if

            move_item->seek_target.assign((const char *)key.data, key.size);
        }   // else

        eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

        if(false == priv.thread_pool.submit(move_item))
        {
            itr_ptr->ReleaseReuseMove();
	    itr_ptr->reuse_move=NULL;
            return enif_make_tuple2(env, ATOM_ERROR, caller_ref);
        }   // if
    }   // if

    return ret_term;

}   // async_iter_move


ERL_NIF_TERM
async_close(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& caller_ref  = argv[0];
    const ERL_NIF_TERM& dbh_ref     = argv[1];
    bool term_ok=false;

    ReferencePtr<DbObject> db_ptr;

    db_ptr.assign(DbObject::RetrieveDbObject(env, dbh_ref, &term_ok));

    if(NULL==db_ptr.get() || 0!=db_ptr->m_CloseRequested)
    {
       return enif_make_badarg(env);
    }

    // verify that Erlang has not called DbObjectResourceCleanup
    //  already (that would be bad)
    if (NULL!=db_ptr->m_Db
//        && compare_and_swap(db_ptr->m_ErlangThisPtr, db_ptr.get(), (DbObject *)NULL))
        && db_ptr->ClaimCloseFromCThread())
    {
        eleveldb::WorkTask *work_item = new eleveldb::CloseTask(env, caller_ref,
                                                                db_ptr.get());

        // Now-boilerplate setup (we'll consolidate this pattern soon, I hope):
        eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

        if(false == priv.thread_pool.submit(work_item))
        {
            delete work_item;
            return send_reply(env, caller_ref, enif_make_tuple2(env, ATOM_ERROR, caller_ref));
        }   // if
    }   // if
    else if (!term_ok)
    {
        return send_reply(env, caller_ref, error_einval(env));
    }   // else

    return ATOM_OK;

}  // async_close


ERL_NIF_TERM
async_iterator_close(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& caller_ref  = argv[0];
    const ERL_NIF_TERM& itr_ref     = argv[1];

    ReferencePtr<ItrObject> itr_ptr;

    itr_ptr.assign(ItrObject::RetrieveItrObject(env, itr_ref));

    if(NULL==itr_ptr.get() || 0!=itr_ptr->m_CloseRequested)
    {
       return enif_make_badarg(env);
    }

    // verify that Erlang has not called ItrObjectResourceCleanup AND
    //  that a database close has not already started death proceedings
    if (itr_ptr->ClaimCloseFromCThread())
    {
        eleveldb::WorkTask *work_item = new eleveldb::ItrCloseTask(env, caller_ref,
                                                                   itr_ptr.get());

        // Now-boilerplate setup (we'll consolidate this pattern soon, I hope):
        eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

        if(false == priv.thread_pool.submit(work_item))
        {
            delete work_item;
            return send_reply(env, caller_ref, enif_make_tuple2(env, ATOM_ERROR, caller_ref));
        }   // if
    }   // if

    // this close/cleanup call is way late ... bad programmer!
    else
    {
        return send_reply(env, caller_ref, error_einval(env));
    }   // else

    return ATOM_OK;

}   // async_iterator_close


ERL_NIF_TERM
async_destroy(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char db_name[4096];

    if(!enif_get_string(env, argv[1], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
       !enif_is_list(env, argv[2]))
    {
        return enif_make_badarg(env);
    }   // if

    ERL_NIF_TERM caller_ref = argv[0];

    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    leveldb::Options *opts = new leveldb::Options;
    fold(env, argv[2], parse_open_option, *opts);

    eleveldb::WorkTask *work_item = new eleveldb::DestroyTask(env, caller_ref,
                                                              db_name, opts);

    if(false == priv.thread_pool.submit(work_item))
    {
        delete work_item;
        return send_reply(env, caller_ref,
                          enif_make_tuple2(env, eleveldb::ATOM_ERROR, caller_ref));
    }

    return eleveldb::ATOM_OK;

}   // async_destroy

} // namespace eleveldb


/**
 * HEY YOU ... please make async
 */
ERL_NIF_TERM
eleveldb_status(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ErlNifBinary name_bin;
    eleveldb::ReferencePtr<eleveldb::DbObject> db_ptr;

    db_ptr.assign(eleveldb::DbObject::RetrieveDbObject(env, argv[0]));

    if(NULL!=db_ptr.get()
       && enif_inspect_binary(env, argv[1], &name_bin))
    {
        if (db_ptr->m_Db == NULL)
        {
            return error_einval(env);
        }

        leveldb::Slice name((const char*)name_bin.data, name_bin.size);
        std::string value;
        if (db_ptr->m_Db->GetProperty(name, &value))
        {
            ERL_NIF_TERM result;
            unsigned char* result_buf = enif_make_new_binary(env, value.size(), &result);
            memcpy(result_buf, value.c_str(), value.size());

            return enif_make_tuple2(env, eleveldb::ATOM_OK, result);
        }
        else
        {
            return eleveldb::ATOM_ERROR;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}   // eleveldb_status


/**
 * HEY YOU ... please make async
 */
ERL_NIF_TERM
eleveldb_repair(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    char name[4096];
    if (enif_get_string(env, argv[0], name, sizeof(name), ERL_NIF_LATIN1))
    {
        // Parse out the options
        leveldb::Options opts;

        leveldb::Status status = leveldb::RepairDB(name, opts);
        if (!status.ok())
        {
            return error_tuple(env, eleveldb::ATOM_ERROR_DB_REPAIR, status);
        }
        else
        {
            return eleveldb::ATOM_OK;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}   // eleveldb_repair


ERL_NIF_TERM
eleveldb_is_empty(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    eleveldb::ReferencePtr<eleveldb::DbObject> db_ptr;

    db_ptr.assign(eleveldb::DbObject::RetrieveDbObject(env, argv[0]));

    if(NULL!=db_ptr.get())
    {
        if (db_ptr->m_Db == NULL)
        {
            return error_einval(env);
        }

        leveldb::ReadOptions opts;
        leveldb::Iterator* itr = db_ptr->m_Db->NewIterator(opts);
        itr->SeekToFirst();
        ERL_NIF_TERM result;
        if (itr->Valid())
        {
            result = eleveldb::ATOM_FALSE;
        }
        else
        {
            result = eleveldb::ATOM_TRUE;
        }
        delete itr;

        return result;
    }
    else
    {
        return enif_make_badarg(env);
    }
}   // eleveldb_is_empty


static void on_unload(ErlNifEnv *env, void *priv_data)
{
    eleveldb_priv_data *p = static_cast<eleveldb_priv_data *>(priv_data);
    delete p;

    leveldb::Env::Shutdown();
}


static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
try
{
    int ret_val;

    ret_val=0;
    *priv_data = NULL;

    // make sure the basic leveldb .so modules are in memory
    //  and initialized ... especially the perf counters
    leveldb::Env::Default();

    // inform erlang of our two resource types
    eleveldb::DbObject::CreateDbObjectType(env);
    eleveldb::ItrObject::CreateItrObjectType(env);

// must initialize atoms before processing options
#define ATOM(Id, Value) { Id = enif_make_atom(env, Value); }
    ATOM(eleveldb::ATOM_OK, "ok");
    ATOM(eleveldb::ATOM_ERROR, "error");
    ATOM(eleveldb::ATOM_EINVAL, "einval");
    ATOM(eleveldb::ATOM_BADARG, "badarg");
    ATOM(eleveldb::ATOM_TRUE, "true");
    ATOM(eleveldb::ATOM_FALSE, "false");
    ATOM(eleveldb::ATOM_CREATE_IF_MISSING, "create_if_missing");
    ATOM(eleveldb::ATOM_ERROR_IF_EXISTS, "error_if_exists");
    ATOM(eleveldb::ATOM_WRITE_BUFFER_SIZE, "write_buffer_size");
    ATOM(eleveldb::ATOM_SST_BLOCK_SIZE, "sst_block_size");
    ATOM(eleveldb::ATOM_BLOCK_RESTART_INTERVAL, "block_restart_interval");
    ATOM(eleveldb::ATOM_BLOCK_SIZE_STEPS, "block_size_steps");
    ATOM(eleveldb::ATOM_ERROR_DB_OPEN,"db_open");
    ATOM(eleveldb::ATOM_ERROR_DB_PUT, "db_put");
    ATOM(eleveldb::ATOM_NOT_FOUND, "not_found");
    ATOM(eleveldb::ATOM_VERIFY_CHECKSUMS, "verify_checksums");
    ATOM(eleveldb::ATOM_FILL_CACHE,"fill_cache");
    ATOM(eleveldb::ATOM_ITERATOR_REFRESH,"iterator_refresh");
    ATOM(eleveldb::ATOM_SYNC, "sync");
    ATOM(eleveldb::ATOM_ERROR_DB_DELETE, "db_delete");
    ATOM(eleveldb::ATOM_CLEAR, "clear");
    ATOM(eleveldb::ATOM_PUT, "put");
    ATOM(eleveldb::ATOM_DELETE, "delete");
    ATOM(eleveldb::ATOM_ERROR_DB_WRITE, "db_write");
    ATOM(eleveldb::ATOM_BAD_WRITE_ACTION, "bad_write_action");
    ATOM(eleveldb::ATOM_KEEP_RESOURCE_FAILED, "keep_resource_failed");
    ATOM(eleveldb::ATOM_ITERATOR_CLOSED, "iterator_closed");
    ATOM(eleveldb::ATOM_FIRST, "first");
    ATOM(eleveldb::ATOM_LAST, "last");
    ATOM(eleveldb::ATOM_NEXT, "next");
    ATOM(eleveldb::ATOM_PREV, "prev");
    ATOM(eleveldb::ATOM_PREFETCH, "prefetch");
    ATOM(eleveldb::ATOM_PREFETCH_STOP, "prefetch_stop");
    ATOM(eleveldb::ATOM_INVALID_ITERATOR, "invalid_iterator");
    ATOM(eleveldb::ATOM_PARANOID_CHECKS, "paranoid_checks");
    ATOM(eleveldb::ATOM_VERIFY_COMPACTIONS, "verify_compactions");
    ATOM(eleveldb::ATOM_ERROR_DB_DESTROY, "error_db_destroy");
    ATOM(eleveldb::ATOM_ERROR_DB_REPAIR, "error_db_repair");
    ATOM(eleveldb::ATOM_KEYS_ONLY, "keys_only");
    ATOM(eleveldb::ATOM_COMPRESSION, "compression");
    ATOM(eleveldb::ATOM_USE_BLOOMFILTER, "use_bloomfilter");
    ATOM(eleveldb::ATOM_TOTAL_MEMORY, "total_memory");
    ATOM(eleveldb::ATOM_TOTAL_LEVELDB_MEM, "total_leveldb_mem");
    ATOM(eleveldb::ATOM_TOTAL_LEVELDB_MEM_PERCENT, "total_leveldb_mem_percent");
    ATOM(eleveldb::ATOM_BLOCK_CACHE_THRESHOLD, "block_cache_threshold");
    ATOM(eleveldb::ATOM_IS_INTERNAL_DB, "is_internal_db");
    ATOM(eleveldb::ATOM_LIMITED_DEVELOPER_MEM, "limited_developer_mem");
    ATOM(eleveldb::ATOM_ELEVELDB_THREADS, "eleveldb_threads");
    ATOM(eleveldb::ATOM_FADVISE_WILLNEED, "fadvise_willneed");
    ATOM(eleveldb::ATOM_DELETE_THRESHOLD, "delete_threshold");
    ATOM(eleveldb::ATOM_TIERED_SLOW_LEVEL, "tiered_slow_level");
    ATOM(eleveldb::ATOM_TIERED_FAST_PREFIX, "tiered_fast_prefix");
    ATOM(eleveldb::ATOM_TIERED_SLOW_PREFIX, "tiered_slow_prefix");
#undef ATOM


    // read options that apply to global eleveldb environment
    if(enif_is_list(env, load_info))
    {
        EleveldbOptions load_options;

        fold(env, load_info, parse_init_option, load_options);

        /* Spin up the thread pool, set up all private data: */
        eleveldb_priv_data *priv = new eleveldb_priv_data(load_options);

        *priv_data = priv;

    }   // if

    else
    {
        // anything non-zero is "fail"
        ret_val=1;
    }   // else
    // Initialize common atoms

    return ret_val;
}


catch(std::exception& e)
{
    /* Refuse to load the NIF module (I see no way right now to return a more specific exception
    or log extra information): */
    return -1;
}
catch(...)
{
    return -1;
}


extern "C" {
    ERL_NIF_INIT(eleveldb, nif_funcs, &on_load, NULL, NULL, &on_unload);
}
