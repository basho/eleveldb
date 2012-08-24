// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
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

#include <set>
#include <queue>
#include <sstream>
#include <algorithm>

#include "eleveldb.h"

#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/write_batch.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"

using std::copy;
using std::nothrow;

static ErlNifResourceType* eleveldb_db_RESOURCE;
static ErlNifResourceType* eleveldb_itr_RESOURCE;
static ErlNifResourceType* eleveldb_thread_pool_RESOURCE;

struct eleveldb_db_handle;
struct eleveldb_itr_handle;
struct eleveldb_thread_pool_handle;
struct eleveldb_priv_data;

struct eleveldb_db_handle
{
    leveldb::DB* db;
    ErlNifMutex* db_lock; // protects access to db
    leveldb::Options options;
    std::set<struct eleveldb_itr_handle*>* iters;
};

struct eleveldb_itr_handle
{
    leveldb::Iterator*   itr;
    ErlNifMutex*         itr_lock; // acquire *after* db_lock if both needed
    const leveldb::Snapshot*   snapshot;
    eleveldb_db_handle* db_handle;
    bool keys_only;
};
typedef struct eleveldb_itr_handle eleveldb_itr_handle;

void *eleveldb_write_thread_worker(void *args);

struct eleveldb_thread_pool_handle
{
 typedef std::set<ErlNifTid *> thread_pool_t;

 // Group submitting DB handles and their work together:
 typedef std::pair<leveldb::WriteBatch, leveldb::WriteOptions> job_t;
 typedef std::pair<eleveldb_db_handle*, job_t>                 work_item_t;
 typedef std::queue<work_item_t>                               work_queue_t; 

 thread_pool_t* threads;

 /* JFW: I'd like to look at combining these lock/container pairs into a single type (eventually
    working toward scoped_lock<>, etc.): */
 ErlNifCond* work_queue_pending;    // flags job present in the work queue
 ErlNifMutex* work_queue_lock;      // protects access to work_queue
 work_queue_t*  work_queue;
 void lock()                    { enif_mutex_lock(work_queue_lock); }
 void unlock()                  { enif_mutex_unlock(work_queue_lock); }

 // JFW: I'm not sure I like these flag variables, but I'm going with them for now:
 bool initialized;              // were we already initialized?
 bool shutdown;                 // should we shut down?

 static eleveldb_thread_pool_handle* construct();      // create uninitialized object

 bool initialize(ErlNifEnv *nif_env);
 bool finalize();
};

eleveldb_thread_pool_handle* eleveldb_thread_pool_handle::construct()
{
 eleveldb_thread_pool_handle* ret(0);

 ret = static_cast<eleveldb_thread_pool_handle*>(
        enif_alloc_resource(eleveldb_thread_pool_RESOURCE, sizeof(eleveldb_thread_pool_handle)));
 if(0 == ret)
  return 0;

 /* JFW: Redundant if we do member initialization, but we might be able to use this idea 
 to make an enif-friendly factory later on: */
 memset(ret, 0, sizeof(eleveldb_thread_pool_handle));

 ret->threads = new(nothrow) thread_pool_t;
 if(0 == ret->threads)
  ; // JFW fixme

 ret->work_queue = new(nothrow) work_queue_t;
 if(0 == ret->work_queue)
  ; // JFW fixme

 ret->work_queue_pending = 0;
 ret->work_queue_lock = 0;

 ret->initialized = false;
 ret->shutdown = false;
 
 return ret; 
}

bool eleveldb_thread_pool_handle::initialize(ErlNifEnv *nif_env)
{
 if(initialized)
  return true; // don't double-init

 work_queue_pending = enif_cond_create(const_cast<char *>("work_queue_pending"));
 if(0 == work_queue_pending)
  ; // JFW: handle this

 work_queue_lock = enif_mutex_create(const_cast<char *>("work_queue_lock"));
 if(0 == work_queue_lock)
  ; // JFW: handle this

 const size_t nthreads = 3; // JFW: TODO: make configurable

 for(size_t i = nthreads; i; --i)
  {
        ErlNifTid *thread_id = static_cast<ErlNifTid *>(enif_alloc(sizeof(ErlNifTid)));
        if(0 == thread_id)
         ; // JFW: error, handle partial initialization

        std::ostringstream thread_name;
        thread_name << "eleveldb_write_thread_" << i;

std::cout << "JFW: spin up thread: " << thread_name.str() << std::endl;

        const int result = enif_thread_create(const_cast<char *>(thread_name.str().c_str()), thread_id, 
                                              eleveldb_write_thread_worker, 
                                              static_cast<void *>(this),
                                              0);

        if(0 != result)
         {
                // JFW: ruh-roh (important: be sure to handle partial construction-- need to release threads via Erlang)
         }

        threads->insert(thread_id);
  }

 // Don't repeat construction for this object:
 initialized = true;

 return true;
}

bool eleveldb_thread_pool_handle::finalize()
{
 // Signal shutdown (regardless of whether we are initialized):
 shutdown = true;

 if(not initialized)
  return true;  // nothing to do

 // The time of the Final Reckoning is here: raise all the threads:
 enif_cond_broadcast(work_queue_pending); 

 // Join all worker threads and release the pointers:
 for(thread_pool_t::iterator i = threads->begin(); threads->end() != i; ++i)
  {
        int *exit_value = 0;
        if(0 != enif_thread_join( *(*i), (void **)(&exit_value)))
         ;      // JFW: errno contains information about failure

        if(false == exit_value)
         ;      // JFW: handle failure

        enif_free(*i); // JFW: we might want to make these managed resources instead?
  }

 threads->clear();

 delete threads, threads = 0;
 delete work_queue, work_queue = 0;

 enif_mutex_destroy(work_queue_lock), work_queue_lock = 0;
 enif_cond_destroy(work_queue_pending), work_queue_pending = 0;

 initialized = false;

 return true;
}

struct eleveldb_priv_data
{
 eleveldb_thread_pool_handle *thread_pool; 

 static eleveldb_priv_data * construct();   // create uninitialized object
 bool initialize(ErlNifEnv *env);
 bool finalize();
};

eleveldb_priv_data* eleveldb_priv_data::construct()
{
 eleveldb_priv_data *ret = static_cast<eleveldb_priv_data *>(enif_alloc(sizeof(eleveldb_priv_data)));

 if(0 == ret)
  return 0; 

 ret->thread_pool = 0;

 return ret;
}

bool eleveldb_priv_data::initialize(ErlNifEnv *env)
{
// JFW: should we set an initialized flag, or avoid?
 // Construct our thread pool:
 thread_pool = eleveldb_thread_pool_handle::construct();

 if(0 == thread_pool)
  return false;

 if(0 == thread_pool->initialize(env))
  {
    thread_pool->finalize();
    return false;
  }

 return true;
}

bool eleveldb_priv_data::finalize()
{
 if(0 != thread_pool)
  thread_pool->finalize();

 thread_pool = 0;

 return true;
}

/* Poll the work queue and take a pending job: */
void *eleveldb_write_thread_worker(void *args)
{
 if(0 == args)
  return 0; // JFW: we should define nice return values

 eleveldb_thread_pool_handle& h = *reinterpret_cast<eleveldb_thread_pool_handle *>(args);

// JFW: Explore a way to simplify this a little:
 for(;;)
  {
    h.lock();

    if(h.shutdown)
     {
        h.unlock();
        break;
     }

    enif_cond_wait(h.work_queue_pending, h.work_queue_lock);

    /* Note that it's possible for enif_cond_wait() to return even though the condition has not, so
    we still need to check this and re-wait if there's no work: */
    if(h.shutdown)
     {
        h.unlock();
        break;
     }

    if(h.work_queue->empty())
     {
        h.unlock(); 
        continue;
     }

    // Accept the job and remove it from the queue:    
    const eleveldb_thread_pool_handle::work_item_t&  submission = h.work_queue->front(); 

    // Make copies of the work item (may be problematic-- job entries can be up to 15kb):
    eleveldb_db_handle* dbh = submission.first;
    eleveldb_thread_pool_handle::job_t job = submission.second;

    h.work_queue->pop(), h.unlock();

    // Write to the database:
    enif_mutex_lock(dbh->db_lock);
    leveldb::Status status = dbh->db->Write(job.second, &job.first);
    if(not status.ok())
     ; // JFW: need to find a way to handle the error

    driver_binary_dec_refc(reinterpret_cast<ErlDrvBinary*>(dbh));
    enif_mutex_unlock(dbh->db_lock);
  }

 return 0; // JFW: consistent return values would be nice...
}

// Atoms (initialized in on_load)
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_EINVAL;
static ERL_NIF_TERM ATOM_CREATE_IF_MISSING;
static ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;
static ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
static ERL_NIF_TERM ATOM_MAX_OPEN_FILES;
static ERL_NIF_TERM ATOM_BLOCK_SIZE;                    /* DEPRECATED */
static ERL_NIF_TERM ATOM_SST_BLOCK_SIZE;
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
static ERL_NIF_TERM ATOM_ERROR_DB_DESTROY;
static ERL_NIF_TERM ATOM_KEYS_ONLY;
static ERL_NIF_TERM ATOM_COMPRESSION;
static ERL_NIF_TERM ATOM_ERROR_DB_REPAIR;
static ERL_NIF_TERM ATOM_USE_BLOOMFILTER;

static ErlNifFunc nif_funcs[] =
{
    {"open", 2, eleveldb_open},
    {"close", 1, eleveldb_close},
    {"get", 3, eleveldb_get},
    {"write", 3, eleveldb_write},
    {"iterator", 2, eleveldb_iterator},
    {"iterator", 3, eleveldb_iterator},
    {"iterator_move", 2, eleveldb_iterator_move},
    {"iterator_close", 1, eleveldb_iterator_close},
    {"status", 2, eleveldb_status},
    {"destroy", 2, eleveldb_destroy},
    {"repair", 2, eleveldb_repair},
    {"is_empty", 1, eleveldb_is_empty},
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
            unsigned long write_buffer_sz;
            if (enif_get_ulong(env, option[1], &write_buffer_sz))
                opts.write_buffer_size = write_buffer_sz;
        }
        else if (option[0] == ATOM_BLOCK_SIZE) 
        { 
            /* DEPRECATED: the old block_size atom was actually ignored. */
            unsigned long block_sz;
            enif_get_ulong(env, option[1], &block_sz); // ignore
        }
        else if (option[0] == ATOM_SST_BLOCK_SIZE)
        {
            unsigned long sst_block_sz(0);
            if (enif_get_ulong(env, option[1], &sst_block_sz))
             opts.block_size = sst_block_sz; // Note: We just set the "old" block_size option. 
        }
        else if (option[0] == ATOM_BLOCK_RESTART_INTERVAL) 
        { 
            int block_restart_interval;
            if (enif_get_int(env, option[1], &block_restart_interval))
                opts.block_restart_interval = block_restart_interval;
        }
        else if (option[0] == ATOM_CACHE_SIZE) 
        {
            unsigned long cache_sz;
            if (enif_get_ulong(env, option[1], &cache_sz)) 
                if (cache_sz != 0) 
                    opts.block_cache = leveldb::NewLRUCache(cache_sz);
        }
        else if (option[0] == ATOM_COMPRESSION)
        {
            if (option[1] == ATOM_TRUE)
            {
                opts.compression = leveldb::kSnappyCompression;
            }
            else
            {
                opts.compression = leveldb::kNoCompression;
            }
        }
        else if (option[0] == ATOM_USE_BLOOMFILTER)
        {
            // By default, we want to use a 10-bit-per-key bloom filter on a
            // per-table basis. We only disable it if explicitly asked. Alternatively,
            // one can provide a value for # of bits-per-key.
            unsigned long bfsize = 10;
            if (option[1] == ATOM_TRUE || enif_get_ulong(env, option[1], &bfsize))
            {
                opts.filter_policy = leveldb::NewBloomFilterPolicy(bfsize);
            }
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

// Free dynamic elements of iterator - acquire lock before calling
static void free_itr(eleveldb_itr_handle* itr_handle)
{
    if (itr_handle->itr)
    {
        delete itr_handle->itr;
        itr_handle->itr = 0;
        itr_handle->db_handle->db->ReleaseSnapshot(itr_handle->snapshot);
    }
}

// Free dynamic elements of database - acquire lock before calling
static void free_db(eleveldb_db_handle* db_handle)
{
    if (db_handle->db)
    {
        // shutdown all the iterators - grab the lock as
        // another thread could still be in eleveldb:fold
        // which will get {error, einval} returned next time
        for (std::set<eleveldb_itr_handle*>::iterator iters_it = db_handle->iters->begin();
             iters_it != db_handle->iters->end();
             ++iters_it)
        {
            eleveldb_itr_handle* itr_handle = *iters_it;
            enif_mutex_lock(itr_handle->itr_lock);
            free_itr(*iters_it);
            enif_mutex_unlock(itr_handle->itr_lock);
        }

        // close the db 
        delete db_handle->db;
        db_handle->db = NULL;
        
        // delete the iters
        delete db_handle->iters;
        db_handle->iters = NULL;

        // Release any cache we explicitly allocated when setting up options
        if (db_handle->options.block_cache)
        {
            delete db_handle->options.block_cache;
        }
        
        // Clean up any filter policies
        if (db_handle->options.filter_policy)
        {
            delete db_handle->options.filter_policy;
        }
    }
}


ERL_NIF_TERM error_einval(ErlNifEnv* env)
{
    return enif_make_tuple2(env, ATOM_ERROR, ATOM_EINVAL);
}

ERL_NIF_TERM error_tuple(ErlNifEnv* env, ERL_NIF_TERM error, leveldb::Status& status)
{
    ERL_NIF_TERM reason = enif_make_string(env, status.ToString().c_str(),
                                           ERL_NIF_LATIN1);
    return enif_make_tuple2(env, ATOM_ERROR,
                            enif_make_tuple2(env, error, reason));
}

ERL_NIF_TERM eleveldb_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
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
        eleveldb_db_handle* handle =
            (eleveldb_db_handle*) enif_alloc_resource(eleveldb_db_RESOURCE,
                                                       sizeof(eleveldb_db_handle));
        memset(handle, '\0', sizeof(eleveldb_db_handle));
        handle->db = db;
        handle->db_lock = enif_mutex_create((char*)"eleveldb_db_lock");
        handle->options = opts;
        handle->iters = new std::set<struct eleveldb_itr_handle*>();

        ERL_NIF_TERM result = enif_make_resource(env, handle);
        enif_release_resource(handle);

        return enif_make_tuple2(env, ATOM_OK, result);
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM eleveldb_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_db_handle* db_handle;
    if (enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&db_handle))
    {
        ERL_NIF_TERM result;

        enif_mutex_lock(db_handle->db_lock);
        if (db_handle->db)
        {
            free_db(db_handle);
            result = ATOM_OK;
        }
        else
        {
            result = error_einval(env);
        }
        enif_mutex_unlock(db_handle->db_lock);
        return result;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM eleveldb_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_db_handle* handle;
    ErlNifBinary key;
    if (enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&handle) &&
        enif_inspect_binary(env, argv[1], &key) &&
        enif_is_list(env, argv[2]))
    {
        enif_mutex_lock(handle->db_lock);
        if (handle->db == NULL)
        {
            enif_mutex_unlock(handle->db_lock);
            return error_einval(env);
        }

        leveldb::DB* db = handle->db;
        leveldb::Slice key_slice((const char*)key.data, key.size);

        // Parse out the read options
        leveldb::ReadOptions opts;
        fold(env, argv[2], parse_read_option, opts);

        std::string sval;
        leveldb::Status status = db->Get(opts, key_slice, &sval);
        if (status.ok())
        {
            const size_t size = sval.size();
            ERL_NIF_TERM value_bin;
            unsigned char* value = enif_make_new_binary(env, size, &value_bin);
            memcpy(value, sval.data(), size);
            enif_mutex_unlock(handle->db_lock);
            return enif_make_tuple2(env, ATOM_OK, value_bin);
        }
        else
        {
            enif_mutex_unlock(handle->db_lock);
            return ATOM_NOT_FOUND;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM eleveldb_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    using std::make_pair;

    eleveldb_db_handle* handle;

    if (enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&handle) &&
        enif_is_list(env, argv[1]) && // Actions
        enif_is_list(env, argv[2]))   // Opts
    {
        if (handle->db == NULL)
         return error_einval(env);

        // Traverse actions and build a write batch
        leveldb::WriteBatch batch;
        ERL_NIF_TERM result = fold(env, argv[1], write_batch_item, batch);
        if (result == ATOM_OK)
        {
            // Was able to fold across all items cleanly -- apply the batch

            // Parse out the write options
            leveldb::WriteOptions opts;
            fold(env, argv[2], parse_write_option, opts);

            // Increment the refcount on the database handle so it doesn't vanish:
            driver_binary_inc_refc(reinterpret_cast<ErlDrvBinary*>(handle));

            // Build and submit a job entry:
            eleveldb_priv_data *priv = static_cast<eleveldb_priv_data *>(enif_priv_data(env));
            eleveldb_thread_pool_handle::work_item_t work_item(make_pair(handle, make_pair(batch, opts)));
            priv->thread_pool->work_queue->push(work_item);
            enif_cond_signal(priv->thread_pool->work_queue_pending);

            return ATOM_OK;
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

ERL_NIF_TERM eleveldb_iterator(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_db_handle* db_handle;
    if (enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&db_handle) &&
        enif_is_list(env, argv[1])) // Options
    {
        enif_mutex_lock(db_handle->db_lock);
        if (db_handle->db == NULL)
        {
            enif_mutex_unlock(db_handle->db_lock);
            return error_einval(env);
        }

        // Increment references to db_handle for duration of the iterator
        enif_keep_resource(db_handle);

        // Parse out the read options
        leveldb::ReadOptions opts;
        fold(env, argv[1], parse_read_option, opts);

        // Setup handle
        eleveldb_itr_handle* itr_handle =
            (eleveldb_itr_handle*) enif_alloc_resource(eleveldb_itr_RESOURCE,
                                                        sizeof(eleveldb_itr_handle));
        memset(itr_handle, '\0', sizeof(eleveldb_itr_handle));

        // Initialize itr handle
        // TODO: Should it be possible to iterate WITHOUT a snapshot?
        itr_handle->itr_lock = enif_mutex_create((char*)"eleveldb_itr_lock");
        itr_handle->db_handle = db_handle;
        itr_handle->snapshot = db_handle->db->GetSnapshot();
        opts.snapshot = itr_handle->snapshot;
        itr_handle->itr = db_handle->db->NewIterator(opts);

        // Check for keys_only iterator flag
        itr_handle->keys_only = ((argc == 3) && (argv[2] == ATOM_KEYS_ONLY));

        ERL_NIF_TERM result = enif_make_resource(env, itr_handle);
        enif_release_resource(itr_handle);

        db_handle->iters->insert(itr_handle);
        enif_mutex_unlock(db_handle->db_lock);
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

ERL_NIF_TERM eleveldb_iterator_move(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_itr_handle* itr_handle;
    if (enif_get_resource(env, argv[0], eleveldb_itr_RESOURCE, (void**)&itr_handle))
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
            if (itr_handle->keys_only)
            {
                result = enif_make_tuple2(env, ATOM_OK,
                                          slice_to_binary(env, itr->key()));
            }
            else
            {
                result = enif_make_tuple3(env, ATOM_OK,
                                          slice_to_binary(env, itr->key()),
                                          slice_to_binary(env, itr->value()));
            }
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


ERL_NIF_TERM eleveldb_iterator_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_itr_handle* itr_handle;
    if (enif_get_resource(env, argv[0], eleveldb_itr_RESOURCE, (void**)&itr_handle))
    {
        // Make sure locks are acquired in the same order to close/free_db
        // to avoid a deadlock.
        enif_mutex_lock(itr_handle->db_handle->db_lock);
        enif_mutex_lock(itr_handle->itr_lock);

        if (itr_handle->db_handle->iters)
        {
            // db may have been closed before the iter (the unit test
            // does an evil close-inside-fold)
            itr_handle->db_handle->iters->erase(itr_handle);
        }
        free_itr(itr_handle);

        enif_mutex_unlock(itr_handle->itr_lock);
        enif_mutex_unlock(itr_handle->db_handle->db_lock);

        enif_release_resource(itr_handle->db_handle); // matches keep in eleveldb_iterator()

        return ATOM_OK;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM eleveldb_status(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_db_handle* db_handle;
    ErlNifBinary name_bin;
    if (enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&db_handle) &&
        enif_inspect_binary(env, argv[1], &name_bin))
    {
        enif_mutex_lock(db_handle->db_lock);
        if (db_handle->db == NULL)
        {
            enif_mutex_unlock(db_handle->db_lock);
            return error_einval(env);
        }

        leveldb::Slice name((const char*)name_bin.data, name_bin.size);
        std::string value;
        if (db_handle->db->GetProperty(name, &value))
        {
            ERL_NIF_TERM result;
            unsigned char* result_buf = enif_make_new_binary(env, value.size(), &result);
            memcpy(result_buf, value.c_str(), value.size());
            enif_mutex_unlock(db_handle->db_lock);
            return enif_make_tuple2(env, ATOM_OK, result);
        }
        else
        {
            enif_mutex_unlock(db_handle->db_lock);
            return ATOM_ERROR;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM eleveldb_repair(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name[4096];
    if (enif_get_string(env, argv[0], name, sizeof(name), ERL_NIF_LATIN1))
    {
        // Parse out the options
        leveldb::Options opts;

        leveldb::Status status = leveldb::RepairDB(name, opts);
        if (!status.ok())
        {
            return error_tuple(env, ATOM_ERROR_DB_REPAIR, status);
        }
        else
        {
            return ATOM_OK;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM eleveldb_destroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char name[4096];
    if (enif_get_string(env, argv[0], name, sizeof(name), ERL_NIF_LATIN1) &&
        enif_is_list(env, argv[1]))
    {
        // Parse out the options
        leveldb::Options opts;
        fold(env, argv[1], parse_open_option, opts);

        leveldb::Status status = leveldb::DestroyDB(name, opts);
        if (!status.ok())
        {
            return error_tuple(env, ATOM_ERROR_DB_DESTROY, status);
        }
        else
        {
            return ATOM_OK;
        }
    }
    else
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM eleveldb_is_empty(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_db_handle* db_handle;
    if (enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&db_handle))
    {
        enif_mutex_lock(db_handle->db_lock);
        if (db_handle->db == NULL)
        {
            enif_mutex_unlock(db_handle->db_lock);
            return error_einval(env);
        }

        leveldb::ReadOptions opts;
        leveldb::Iterator* itr = db_handle->db->NewIterator(opts);
        itr->SeekToFirst();
        ERL_NIF_TERM result;
        if (itr->Valid())
        {
            result = ATOM_FALSE;
        }
        else
        {
            result = ATOM_TRUE;
        }
        delete itr;
        enif_mutex_unlock(db_handle->db_lock);
        return result;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

static void eleveldb_db_resource_cleanup(ErlNifEnv* env, void* arg)
{
    // Delete any dynamically allocated memory stored in eleveldb_db_handle
    eleveldb_db_handle* handle = (eleveldb_db_handle*)arg;

    free_db(handle);

    enif_mutex_destroy(handle->db_lock);
}

static void eleveldb_itr_resource_cleanup(ErlNifEnv* env, void* arg)
{
    // Delete any dynamically allocated memory stored in eleveldb_itr_handle
    eleveldb_itr_handle* itr_handle = (eleveldb_itr_handle*)arg;

    // No need to lock iter - it's the last reference
    if (itr_handle->itr != 0)
    {
        enif_mutex_lock(itr_handle->db_handle->db_lock);

        if (itr_handle->db_handle->iters)
        {
            itr_handle->db_handle->iters->erase(itr_handle);
        }
        free_itr(itr_handle);

        enif_mutex_unlock(itr_handle->db_handle->db_lock);
        enif_release_resource(itr_handle->db_handle);  // matches keep in eleveldb_iterator()
    }

    enif_mutex_destroy(itr_handle->itr_lock);
}

static void eleveldb_thread_pool_resource_cleanup(ErlNifEnv* env, void* arg)
{
   eleveldb_thread_pool_handle* thread_pool_handle = (eleveldb_thread_pool_handle*)arg;
   thread_pool_handle->finalize();
}

#define ATOM(Id, Value) { Id = enif_make_atom(env, Value); }

// JFW: TODO: what's the last parameter for? A result code?
static void on_unload(ErlNifEnv *env, void *JFW_mysterious)
{
 eleveldb_priv_data *p = static_cast<eleveldb_priv_data *>(enif_priv_data(env));

 p->finalize();

 enif_free(p);
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    eleveldb_db_RESOURCE = enif_open_resource_type(env, NULL, "eleveldb_db_resource",
                                                    &eleveldb_db_resource_cleanup,
                                                    flags, NULL);
    eleveldb_itr_RESOURCE = enif_open_resource_type(env, NULL, "eleveldb_itr_resource",
                                                     &eleveldb_itr_resource_cleanup,
                                                     flags, NULL);

    eleveldb_thread_pool_RESOURCE = enif_open_resource_type(env, NULL, "elevedb_thread_pool_resource",
                                                             &eleveldb_thread_pool_resource_cleanup,
                                                             flags, NULL); 
    if(0 == eleveldb_thread_pool_RESOURCE)
     ; // JFW: handle failure

    /* Establish private data: */
    eleveldb_priv_data *priv = eleveldb_priv_data::construct();
    if(0 == priv)
     ; // JFW: handle failure

    if(false == priv->initialize(env))
     ; // JFW handle failure

    *priv_data = priv;

    // Initialize common atoms
    ATOM(ATOM_OK, "ok");
    ATOM(ATOM_ERROR, "error");
    ATOM(ATOM_EINVAL, "einval");
    ATOM(ATOM_TRUE, "true");
    ATOM(ATOM_FALSE, "false");
    ATOM(ATOM_CREATE_IF_MISSING, "create_if_missing");
    ATOM(ATOM_ERROR_IF_EXISTS, "error_if_exists");
    ATOM(ATOM_WRITE_BUFFER_SIZE, "write_buffer_size");
    ATOM(ATOM_MAX_OPEN_FILES, "max_open_files");
    ATOM(ATOM_BLOCK_SIZE, "block_size");
    ATOM(ATOM_SST_BLOCK_SIZE, "sst_block_size");
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
    ATOM(ATOM_ERROR_DB_DESTROY, "error_db_destroy");
    ATOM(ATOM_ERROR_DB_REPAIR, "error_db_repair");
    ATOM(ATOM_KEYS_ONLY, "keys_only");
    ATOM(ATOM_COMPRESSION, "compression");
    ATOM(ATOM_USE_BLOOMFILTER, "use_bloomfilter");

    return 0;
}

extern "C" {
    ERL_NIF_INIT(eleveldb, nif_funcs, &on_load, NULL, NULL, &on_unload);
}
