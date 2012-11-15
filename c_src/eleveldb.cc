// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
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

#include <new>
#include <set>
#include <stack>
#include <queue>
#include <sstream>
#include <algorithm>

#include "eleveldb.h"

#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/write_batch.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"

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
    {"submit_job", 4, eleveldb_submit_job},
    {"iterator", 2, eleveldb_iterator},
    {"iterator", 3, eleveldb_iterator},
    {"iterator_move", 2, eleveldb_iterator_move},
    {"iterator_close", 1, eleveldb_iterator_close},
    {"status", 2, eleveldb_status},
    {"destroy", 2, eleveldb_destroy},
    {"repair", 2, eleveldb_repair},
    {"is_empty", 1, eleveldb_is_empty},
};

using std::copy;
using std::nothrow;

static ErlNifResourceType* eleveldb_db_RESOURCE;
static ErlNifResourceType* eleveldb_itr_RESOURCE;

struct eleveldb_db_handle;
struct eleveldb_itr_handle;

class eleveldb_thread_pool;
class eleveldb_priv_data;

/* Some primitive-yet-useful NIF helpers: */
namespace {

template <class T>
void *placement_alloc()
{
 void *placement = enif_alloc(sizeof(T));
 if(0 == placement)
  throw;

 return placement;
}

template <class T>
T *placement_ctor()
{
 return new(placement_alloc<T>()) T;
}

template <class T, 
          class P0>
T *placement_ctor(P0 p0)
{
 return new(placement_alloc<T>()) T(p0);
}

template <class T, 
          class P0, class P1>
T *placement_ctor(P0 p0, P1 p1)
{
 return new(placement_alloc<T>()) T(p0, p1);
}

template <class T, 
          class P0, class P1, class P2>
T *placement_ctor(P0 p0, P1 p1, P2 p2)
{
 return new(placement_alloc<T>()) T(p0, p1, p2);
}

template <class T, 
          class P0, class P1, class P2, class P3>
T *placement_ctor(P0 p0, P1 p1, P2 p2, P3 p3)
{
 return new(placement_alloc<T>()) T(p0, p1, p2, p3);
}

template <class T, 
          class P0, class P1, class P2, class P3, class P4>
T *placement_ctor(P0 p0, P1 p1, P2 p2, P3 p3, P4 p4)
{
 return new(placement_alloc<T>()) T(p0, p1, p2, p3, p4);
}

template <class T, 
          class P0, class P1, class P2, class P3, class P4, class P5>
T *placement_ctor(P0 p0, P1 p1, P2 p2, P3 p3, P4 p4, P5 p5)
{
 return new(placement_alloc<T>()) T(p0, p1, p2, p3, p4, p5);
}

template <class T>
void placement_dtor(T *& x)
{
 if(0 == x)
  return;

 x->~T();
 enif_free(x);
}

// Scoped lock that is not ownership-aware:
class simple_scoped_lock
{
 ErlNifMutex* lock;

 private:
 simple_scoped_lock();                                        // nodefault
 simple_scoped_lock(const simple_scoped_lock&);               // nocopy
 simple_scoped_lock& operator=(const simple_scoped_lock&);    // nocopyassign

 public:
 simple_scoped_lock(ErlNifMutex* _lock)
  : lock(_lock)
 {
    enif_mutex_lock(lock);
 }

 ~simple_scoped_lock()
 {
    enif_mutex_unlock(lock);
 }
};

} // namespace

struct eleveldb_db_handle
{
    leveldb::DB* db;
    ErlNifMutex* db_lock;                                       // protects access to db

    leveldb::Options *options;

    std::set<struct eleveldb_itr_handle*>* iters;

    private:
    eleveldb_db_handle();                                       // nodefault
    eleveldb_db_handle(const eleveldb_db_handle&);              // nocopy
    eleveldb_db_handle& operator=(const eleveldb_db_handle&);   // nocopyassign
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

class eleveldb_thread_pool
{
 friend void *eleveldb_write_thread_worker(void *args);

 private:
 eleveldb_thread_pool(const eleveldb_thread_pool&);             // nocopy
 eleveldb_thread_pool& operator=(const eleveldb_thread_pool&);  // nocopyassign

 public:
 struct work_item_t
 {
    private:
    work_item_t(const work_item_t&);            // nocopy
    work_item_t& operator=(const work_item_t&); // nocopyassign

    public:
    ErlNifEnv*                      local_env;

    ERL_NIF_TERM                    caller_ref, 
                                    pid_term;

    mutable eleveldb_db_handle*     db_handle;

    mutable leveldb::WriteBatch*    batch; 
    leveldb::WriteOptions*          options;

    work_item_t(ErlNifEnv* _local_env,
                ERL_NIF_TERM& _caller_ref, ERL_NIF_TERM _pid_term, 
                eleveldb_db_handle* _db_handle,
                leveldb::WriteBatch* _batch,
                leveldb::WriteOptions* _options)
     : local_env(_local_env),
       caller_ref(_caller_ref), pid_term(_pid_term), 
       db_handle(_db_handle), 
       batch(_batch),
       options(_options)
    {}

    ~work_item_t()
    {
        placement_dtor(batch);
        placement_dtor(options);

        enif_free_env(local_env);
    }
 };

 private:
 typedef std::queue<work_item_t*> work_queue_t; 
 typedef std::stack<ErlNifTid *>  thread_pool_t;

 private:
 thread_pool_t  threads;
 ErlNifMutex*   threads_lock;       // protect resizing of the thread pool

 work_queue_t   work_queue;
 ErlNifCond*    work_queue_pending; // flags job present in the work queue
 ErlNifMutex*   work_queue_lock;    // protects access to work_queue

 bool shutdown;                     // should we stop threads and shut down?

 public:
 eleveldb_thread_pool(const size_t thread_pool_size);
 ~eleveldb_thread_pool();

 public:
 void lock()                    { enif_mutex_lock(work_queue_lock); }
 void unlock()                  { enif_mutex_unlock(work_queue_lock); }

 void submit(work_item_t* item) 
 { 
    lock(), work_queue.push(item), unlock(); 
    enif_cond_signal(work_queue_pending);
 }

 bool resize_thread_pool(const size_t n)
 {
    if(0 == n)
     return false;

    if(threads.size() == n)
     return true; // nothing to do

    // Strictly expanding is less expensive:
    if(threads.size() < n)
     return grow_thread_pool(n - threads.size());

    if(false == drain_thread_pool())
     return false;

    return grow_thread_pool(n);
 }

 size_t work_queue_size() const { return work_queue.size(); } 
 bool shutdown_pending() const  { return shutdown; }

 private:

 bool grow_thread_pool(const size_t nthreads);
 bool drain_thread_pool();

 static bool notify_caller(const work_item_t& work_item, const bool job_result);
};

eleveldb_thread_pool::eleveldb_thread_pool(const size_t thread_pool_size)
  : threads_lock(0),
    work_queue_pending(0), work_queue_lock(0), 
    shutdown(false)
{
 threads_lock = enif_mutex_create(const_cast<char *>("threads_lock"));
 if(0 == threads_lock)
  throw;

 work_queue_pending = enif_cond_create(const_cast<char *>("work_queue_pending"));
 if(0 == work_queue_pending)
  throw;

 work_queue_lock = enif_mutex_create(const_cast<char *>("work_queue_lock"));
 if(0 == work_queue_lock)
  throw;

 grow_thread_pool(thread_pool_size);
}

eleveldb_thread_pool::~eleveldb_thread_pool()
{
 drain_thread_pool();   // all kids out of the pool

 enif_mutex_destroy(work_queue_lock);
 enif_cond_destroy(work_queue_pending);

 enif_mutex_destroy(threads_lock);
}

// Grow the thread pool by nthreads threads:
bool eleveldb_thread_pool::grow_thread_pool(const size_t nthreads)
{
 simple_scoped_lock l(threads_lock);

 if(0 >= nthreads)
  return true;  // nothing to do, but also not failure

 // At least one thread means that we don't shut threads down:
 shutdown = false;

 for(size_t i = nthreads; i; --i)
  {
    std::ostringstream thread_name;
    thread_name << "eleveldb_write_thread_" << threads.size() + 1;

    ErlNifTid *thread_id = static_cast<ErlNifTid *>(enif_alloc(sizeof(ErlNifTid)));

    if(0 == thread_id)
     return false;

    const int result = enif_thread_create(const_cast<char *>(thread_name.str().c_str()), thread_id, 
                                          eleveldb_write_thread_worker, 
                                          static_cast<void *>(this),
                                          0);

    if(0 != result)
     return false;

    threads.push(thread_id);
  }

 return true;
}

// Shut down and destroy all threads in the thread pool:
bool eleveldb_thread_pool::drain_thread_pool()
{
 struct release_thread
 {
    bool state;

    release_thread()
     : state(true)
    {}

    void operator()(ErlNifTid*& tid)
    {
        if(0 != enif_thread_join(*tid, 0)) 
         state = false; 

        enif_free(tid);
    }

    bool operator()() const { return state; }
 } rt;

 // Signal shutdown and raise all threads:
 shutdown = true;
 enif_cond_broadcast(work_queue_pending);

 simple_scoped_lock l(threads_lock);
 while(!threads.empty())
  {
    rt(threads.top());
    threads.pop();    
  }

 return rt();
}

bool eleveldb_thread_pool::notify_caller(const work_item_t& work_item, const bool job_result)
{
 ErlNifPid pid;

 if(0 == enif_get_local_pid(work_item.local_env, work_item.pid_term, &pid))
  return false;

 ERL_NIF_TERM result_tuple = 
                enif_make_tuple2(work_item.local_env, 
                                 (job_result ? ATOM_OK : ATOM_ERROR),
                                 work_item.caller_ref); 
 
 return (0 != enif_send(0, &pid, work_item.local_env, result_tuple));
}

/* Module-level private data: */
class eleveldb_priv_data
{
 eleveldb_priv_data(const eleveldb_priv_data&);             // nocopy
 eleveldb_priv_data& operator=(const eleveldb_priv_data&);  // nocopyassign

 public:
 eleveldb_thread_pool thread_pool;

 eleveldb_priv_data(const size_t n_write_threads)
  : thread_pool(n_write_threads)
 {}
};

/* Poll the work queue, submit jobs to leveldb: */
void *eleveldb_write_thread_worker(void *args)
{
 eleveldb_thread_pool& h = *reinterpret_cast<eleveldb_thread_pool*>(args);

 for(;;)
  {
    h.lock();

    while(h.work_queue.empty() && not h.shutdown)
     enif_cond_wait(h.work_queue_pending, h.work_queue_lock);

    if(h.shutdown)
     {
        h.unlock();
        break;
     }

    // Take a job from and release the queue:
    eleveldb_thread_pool::work_item_t* submission = h.work_queue.front(); 

    h.work_queue.pop();
    h.unlock();

    // Submit the job to leveldb:
    eleveldb_db_handle* dbh = submission->db_handle;

    enif_mutex_lock(dbh->db_lock);
    leveldb::Status status = dbh->db->Write(*(submission->options), submission->batch);
    enif_mutex_unlock(dbh->db_lock);

    enif_release_resource(dbh);         // decrement the refcount of the leveldb handle

    // Ping the caller back in Erlang-land:
    if(false == eleveldb_thread_pool::notify_caller(*submission, status.ok() ? true : false))
     ; // There isn't much to be done if this has failed. We have no supervisor process.

    placement_dtor(submission);
  }

 return 0; 
}

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
                 {
                    opts.block_cache = leveldb::NewLRUCache(cache_sz);
                 }
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
static bool free_db(eleveldb_db_handle* db_handle)
{
    if (0 == db_handle)
     return false;

    if (db_handle->db_lock)
     enif_mutex_lock(db_handle->db_lock);

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
    }

    if (db_handle->options)
    {
        // Release any cache we explicitly allocated when setting up options
        if (db_handle->options->block_cache)
         delete db_handle->options->block_cache, db_handle->options->block_cache = 0;
        
        // Clean up any filter policies
        if (db_handle->options->filter_policy)
         delete db_handle->options->filter_policy, db_handle->options->filter_policy = 0;

        placement_dtor(db_handle->options), db_handle->options = 0;
     }

    if (db_handle->db_lock)
     {
        enif_mutex_unlock(db_handle->db_lock);
        enif_mutex_destroy(db_handle->db_lock), db_handle->db_lock = 0;
     }

    return true;
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
        leveldb::Options *opts = placement_ctor<leveldb::Options>();;
        fold(env, argv[1], parse_open_option, *opts);

        // Open the database
        leveldb::DB* db;

        leveldb::Status status = leveldb::DB::Open(*opts, name, &db);
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

    if (!enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&db_handle))
     return enif_make_badarg(env);

    return free_db(db_handle) ? ATOM_OK : error_einval(env);
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

ERL_NIF_TERM eleveldb_submit_job(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_db_handle* handle;

    ERL_NIF_TERM caller_ref = argv[0];

    if (enif_get_resource(env, argv[1], eleveldb_db_RESOURCE, (void**)&handle) &&
        enif_is_list(env, argv[2]) && // Actions
        enif_is_list(env, argv[3]))   // Opts
    {
        if (handle->db == NULL)
         return error_einval(env);

        eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

        // Stop taking requests if we've been asked to shut down:
        if(priv.thread_pool.shutdown_pending())
         return enif_make_tuple2(env, ATOM_ERROR, caller_ref);

        // Construct a write batch:
        leveldb::WriteBatch* batch = placement_ctor<leveldb::WriteBatch>();

        // Seed the batch's data:
        ERL_NIF_TERM result = fold(env, argv[2], write_batch_item, *batch);
        if (result == ATOM_OK)
        {
            // Was able to fold across all items cleanly -- apply the batch

            // Parse out the write options
            leveldb::WriteOptions* opts = placement_ctor<leveldb::WriteOptions>();;
            fold(env, argv[3], parse_write_option, *opts);

            // Increment the refcount on the database handle so it doesn't vanish:
            enif_keep_resource(handle);

            // Build a job entry and submit it into the queue:

            // Construct a local environment to store terms and messages:
            ErlNifEnv* local_env = enif_alloc_env();
            if(0 == local_env)
             return enif_make_tuple2(env, ATOM_ERROR, caller_ref);

            ErlNifPid local_pid;
            enif_self(env, &local_pid);

            // Enqueue the job:
            eleveldb_thread_pool::work_item_t* work_item = placement_ctor<eleveldb_thread_pool::work_item_t>(
                                                            local_env,
                                                            enif_make_copy(local_env, caller_ref),
                                                            enif_make_pid(local_env, &local_pid),
                                                            handle, batch, opts
                                                           );

            priv.thread_pool.submit(work_item);

            return ATOM_OK;
        }
        else
        {
            // Failed to parse out batch commands; bad item was returned from fold.
            return enif_make_tuple3(env, ATOM_ERROR, caller_ref,
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
 free_db(reinterpret_cast<eleveldb_db_handle *>(arg));
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

#define ATOM(Id, Value) { Id = enif_make_atom(env, Value); }

static void on_unload(ErlNifEnv *env, void *priv_data)
{
 eleveldb_priv_data *p = static_cast<eleveldb_priv_data *>(priv_data);
 placement_dtor(p);
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
try
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    eleveldb_db_RESOURCE = enif_open_resource_type(env, NULL, "eleveldb_db_resource",
                                                    &eleveldb_db_resource_cleanup,
                                                    flags, NULL);
    eleveldb_itr_RESOURCE = enif_open_resource_type(env, NULL, "eleveldb_itr_resource",
                                                     &eleveldb_itr_resource_cleanup,
                                                     flags, NULL);

    /* Gather local initialization data: */
    struct _local
    {
        int n_threads;

        _local()
         : n_threads(0)
        {}
    } local;

    /* Seed our private data with appropriate values: */
    if(!enif_is_list(env, load_info))
     return enif_make_badarg(env);

    ERL_NIF_TERM load_info_head;

    while(0 != enif_get_list_cell(env, load_info, &load_info_head, &load_info))
     {
        int arity = 0;
        ERL_NIF_TERM *tuple_data;

        // Pick out "{write_threads, N}":
        if(enif_get_tuple(env, load_info_head, &arity, const_cast<const ERL_NIF_TERM **>(&tuple_data)))
         {
            if(2 != arity)
             continue;

            unsigned int atom_len;
            if(0 == enif_get_atom_length(env, tuple_data[0], &atom_len, ERL_NIF_LATIN1))
             continue;

            const unsigned int atom_max = 128;
            char atom[atom_max];
            if((atom_len + 1) != static_cast<unsigned int>(enif_get_atom(env, tuple_data[0], atom, atom_max, ERL_NIF_LATIN1)))
             continue;

            if(0 != strncmp(atom, "write_threads", atom_max))
             continue;

            // We have a setting, now peek at the parameter: 
            if(0 == enif_get_int(env, tuple_data[1], &local.n_threads))
             return enif_make_badarg(env);

            if(0 >= local.n_threads)
             return enif_make_badarg(env);
         } 
     }

    /* Spin up the thread pool, set up all private data: */
    eleveldb_priv_data *priv = placement_ctor<eleveldb_priv_data>(local.n_threads);

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
catch(...)
{
    return 1; // refuse to load the NIF module
}

extern "C" {
    ERL_NIF_INIT(eleveldb, nif_funcs, &on_load, NULL, NULL, &on_unload);
}
