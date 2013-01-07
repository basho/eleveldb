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
    {"close", 1, eleveldb_close},
    {"iterator_close", 1, eleveldb_iterator_close},
    {"status", 2, eleveldb_status},
    {"destroy", 2, eleveldb_destroy},
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
ERL_NIF_TERM ATOM_CREATE_IF_MISSING;
ERL_NIF_TERM ATOM_ERROR_IF_EXISTS;
ERL_NIF_TERM ATOM_WRITE_BUFFER_SIZE;
ERL_NIF_TERM ATOM_MAX_OPEN_FILES;
ERL_NIF_TERM ATOM_BLOCK_SIZE;                    /* DEPRECATED */
ERL_NIF_TERM ATOM_SST_BLOCK_SIZE;
ERL_NIF_TERM ATOM_BLOCK_RESTART_INTERVAL;
ERL_NIF_TERM ATOM_ERROR_DB_OPEN;
ERL_NIF_TERM ATOM_ERROR_DB_PUT;
ERL_NIF_TERM ATOM_NOT_FOUND;
ERL_NIF_TERM ATOM_VERIFY_CHECKSUMS;
ERL_NIF_TERM ATOM_FILL_CACHE;
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
ERL_NIF_TERM ATOM_INVALID_ITERATOR;
ERL_NIF_TERM ATOM_CACHE_SIZE;
ERL_NIF_TERM ATOM_PARANOID_CHECKS;
ERL_NIF_TERM ATOM_ERROR_DB_DESTROY;
ERL_NIF_TERM ATOM_KEYS_ONLY;
ERL_NIF_TERM ATOM_COMPRESSION;
ERL_NIF_TERM ATOM_ERROR_DB_REPAIR;
ERL_NIF_TERM ATOM_USE_BLOOMFILTER;

}   // namespace eleveldb





using std::nothrow;

struct eleveldb_itr_handle;

class eleveldb_thread_pool;
class eleveldb_priv_data;

namespace {
const size_t N_THREADS_MAX = 32767;
}


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



void *eleveldb_write_thread_worker(void *args);


/**
 * Meta / managment data related to a worker thread.
 */
struct ThreadData
{
    ErlNifTid * m_ErlTid;                //!< erlang handle for this thread
    volatile uint32_t m_Available;       //!< 1 if thread waiting, using standard type for atomic operation
    class eleveldb_thread_pool & m_Pool; //!< parent pool object
    volatile eleveldb::WorkTask * m_DirectWork; //!< work passed direct to thread

    pthread_mutex_t m_Mutex;             //!< mutex for condition variable
    pthread_cond_t m_Condition;          //!< condition for thread waiting


    ThreadData(class eleveldb_thread_pool & Pool)
    : m_ErlTid(NULL), m_Available(0), m_Pool(Pool), m_DirectWork(NULL)
    {
        pthread_mutex_init(&m_Mutex, NULL);
        pthread_cond_init(&m_Condition, NULL);

        return;
    }   // ThreadData

private:
    ThreadData();

};  // class ThreadData


class eleveldb_thread_pool
{
 friend void *eleveldb_write_thread_worker(void *args);

 private:
 eleveldb_thread_pool(const eleveldb_thread_pool&);             // nocopy
 eleveldb_thread_pool& operator=(const eleveldb_thread_pool&);  // nocopyassign

protected:

 typedef std::deque<eleveldb::WorkTask*> work_queue_t;
    // typedef std::stack<ErlNifTid *>            thread_pool_t;
 typedef std::vector<ThreadData *>   thread_pool_t;

private:
 thread_pool_t  threads;
 eleveldb::Mutex threads_lock;       // protect resizing of the thread pool
 eleveldb::Mutex thread_resize_pool_mutex;

 work_queue_t   work_queue;
 ErlNifCond*    work_queue_pending; // flags job present in the work queue
 ErlNifMutex*   work_queue_lock;    // protects access to work_queue
 volatile size_t work_queue_atomic;   //!< atomic size to parallel work_queue.size().

 volatile bool  shutdown;           // should we stop threads and shut down?

 public:
 eleveldb_thread_pool(const size_t thread_pool_size);
 ~eleveldb_thread_pool();

 public:
 void lock()                    { enif_mutex_lock(work_queue_lock); }
 void unlock()                  { enif_mutex_unlock(work_queue_lock); }


 bool FindWaitingThread(eleveldb::WorkTask * work)
 {
     bool ret_flag;
     size_t start, index, pool_size;

     ret_flag=false;

     // pick "random" place in thread list.  hopefully
     //  list size is prime number.
     pool_size=threads.size();
     start=(size_t)pthread_self() % pool_size;
     index=start;

     do
     {
         if (0!=threads[index]->m_Available)
         {
             ret_flag = eleveldb::detail::compare_and_swap(&threads[index]->m_Available, 1, 0);

             if (ret_flag)
             {

                 // man page says mutex lock optional, experience in
                 //  this code says it is not.  using broadcast instead
                 //  of signal to cover one other race condition
                 //  that should never happen with single thread waiting.
                 pthread_mutex_lock(&threads[index]->m_Mutex);
                 threads[index]->m_DirectWork=work;
                 pthread_cond_broadcast(&threads[index]->m_Condition);
                 pthread_mutex_unlock(&threads[index]->m_Mutex);
             }   // if
         }   // if

         index=(index+1)%pool_size;

     } while(index!=start && !ret_flag);

     return(ret_flag);

 }   // FindWaitingThread


 bool submit(eleveldb::WorkTask* item)
 {
     bool ret_flag(false);

     if (NULL!=item)
     {
         item->RefInc();

         if(shutdown_pending())
         {
             item->RefDec();
             ret_flag=false;
         }   // if

         // try to give work to a waiting thread first
         else if (!FindWaitingThread(item))
         {
             // no waiting threads, put on backlog queue
             lock();
             __sync_add_and_fetch(&work_queue_atomic, 1);
             work_queue.push_back(item);
             unlock();

             // to address race condition, thread might be waiting now
             FindWaitingThread(NULL);

             perf()->Inc(leveldb::ePerfElevelQueued);
             ret_flag=true;
         }   // if
         else
         {
             perf()->Inc(leveldb::ePerfElevelDirect);
             ret_flag=true;
         }   // else
     }   // if

     return(ret_flag);

 }   // submit


 bool resize_thread_pool(const size_t n)
 {
     eleveldb::MutexLock l(thread_resize_pool_mutex);

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
 leveldb::PerformanceCounters * perf() const {return(leveldb::gPerfCounters);};


 private:

 bool grow_thread_pool(const size_t nthreads);
 bool drain_thread_pool();

 static bool notify_caller(eleveldb::WorkTask& work_item);
};

eleveldb_thread_pool::eleveldb_thread_pool(const size_t thread_pool_size)
  : work_queue_pending(0), work_queue_lock(0),
    work_queue_atomic(0),
    shutdown(false)
{

 work_queue_pending = enif_cond_create(const_cast<char *>("work_queue_pending"));
 if(0 == work_queue_pending)
  throw std::runtime_error("cannot create condition work_queue_pending");

 work_queue_lock = enif_mutex_create(const_cast<char *>("work_queue_lock"));
 if(0 == work_queue_lock)
  throw std::runtime_error("cannot create work_queue_lock");

 if(false == grow_thread_pool(thread_pool_size))
  throw std::runtime_error("cannot resize thread pool");
}

eleveldb_thread_pool::~eleveldb_thread_pool()
{
 drain_thread_pool();   // all kids out of the pool

 enif_mutex_destroy(work_queue_lock);
 enif_cond_destroy(work_queue_pending);

}

// Grow the thread pool by nthreads threads:
bool eleveldb_thread_pool::grow_thread_pool(const size_t nthreads)
{
 eleveldb::MutexLock l(threads_lock);
 ThreadData * new_thread;

 if(0 >= nthreads)
  return true;  // nothing to do, but also not failure

 if(N_THREADS_MAX < nthreads + threads.size())
  return false;

 // At least one thread means that we don't shut threads down:
 shutdown = false;

 threads.reserve(nthreads);

 for(size_t i = nthreads; i; --i)
  {
    std::ostringstream thread_name;
    thread_name << "eleveldb_write_thread_" << threads.size() + 1;

    ErlNifTid *thread_id = static_cast<ErlNifTid *>(enif_alloc(sizeof(ErlNifTid)));

    if(0 == thread_id)
     return false;

    new_thread=new ThreadData(*this);

    const int result = enif_thread_create(const_cast<char *>(thread_name.str().c_str()), thread_id,
                                          eleveldb_write_thread_worker,
                                          static_cast<void *>(new_thread),
                                          0);

    new_thread->m_ErlTid=thread_id;

    if(0 != result)
     return false;


    threads.push_back(new_thread);
  }

 return true;
}

#if 0
namespace {

// Utility predicate: true if db_handle in write task matches given db handle:
struct db_matches
{
    const eleveldb_db_handle* dbh;

    db_matches(const eleveldb_db_handle* _dbh)
     : dbh(_dbh)
    {}

    bool operator()(eleveldb::WorkTask*& rhs)
    {
        // We're only concerned with elements tied to our database handle:
        eleveldb::write_task_t *rhs_item = dynamic_cast<eleveldb::write_task_t *>(rhs);

        if(0 == rhs_item)
         return false;

        return dbh == rhs_item->db_handle;
    }
};

} // namespace
#endif

#if 0
bool eleveldb_thread_pool::complete_jobs_for(eleveldb_db_handle* dbh)
{
 if(0 == dbh)
  return true;  // nothing to do, but not a failure

 /* Our strategy for completion here is that we move any jobs pending for the handle we
 want to close to the front of the queue (in the same relative order), effectively giving
 them priority. When the next handle in the queue does not match our db handle, or there are
 no more jobs, we're done: */

 db_matches m(dbh);

 // We don't want more than one close operation to reshuffle:
 simple_scoped_mutex_handle complete_jobs_mutex("complete_jobs_mutex");

 {
 MutexLock complete_jobs_lock(complete_jobs_mutex);

 // Stop new jobs coming in during shuffle; after that, appending jobs is fine:
 {
 MutexLock l(work_queue_lock);
 std::stable_partition(work_queue.begin(), work_queue.end(), m);
 }

 // Signal all threads and drain our work first:
 enif_cond_broadcast(work_queue_pending);
 while(not work_queue.empty() and m(work_queue.front()))
  ;
 }

 return true;
}
#endif

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

 eleveldb::MutexLock l(threads_lock);
#if 0
 while(!threads.empty())
  {
    // Rebroadcast on each invocation (workers might not see the signal otherwise):
    enif_cond_broadcast(work_queue_pending);

    rt(threads.top());
    threads.pop();
  }
#endif

 return rt();
}

bool eleveldb_thread_pool::notify_caller(eleveldb::WorkTask& work_item)
{
 ErlNifPid pid;
 bool ret_flag(true);


 // Call the work function:
 basho::async_nif::work_result result = work_item();

 if (result.is_set())
 {
     if(0 != enif_get_local_pid(work_item.local_env(), work_item.pid(), &pid))
     {
         /* Assemble a notification of the following form:
            { PID CallerHandle, ERL_NIF_TERM result } */
         ERL_NIF_TERM result_tuple = enif_make_tuple2(work_item.local_env(),
                                                      work_item.caller_ref(), result.result());

         ret_flag=(0 != enif_send(0, &pid, work_item.local_env(), result_tuple));
     }   // if
     else
     {
         ret_flag=false;
     }   // else
 }   // if

 return(ret_flag);
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
    ThreadData &tdata = *(ThreadData *)args;
    eleveldb_thread_pool& h = tdata.m_Pool;
    eleveldb::WorkTask * submission;

    submission=NULL;

    while(!h.shutdown)
    {
        // cast away volatile
        if (NULL==submission)
        {
            // test non-blocking size for hint (much faster)
            if (0!=h.work_queue_atomic)
            {
                // retest with locking
                h.lock();
                if (!h.work_queue.empty())
                {
                    submission=h.work_queue.front();
                    h.work_queue.pop_front();
                    __sync_sub_and_fetch(&h.work_queue_atomic, 1);
                    h.perf()->Inc(leveldb::ePerfElevelDequeued);
                }   // if

                h.unlock();
            }   // if
        }   // if
        else
        {
//            tdata.m_DirectWork=NULL;
        }   // else

        if (NULL!=submission)
        {
            eleveldb_thread_pool::notify_caller(*submission);
            if (submission->resubmit())
            {
                submission->recycle();
                h.submit(submission);
            }   // if

            // resubmit will increment reference again, so
            //  always dec even in reuse case
            submission->RefDec();

            submission=NULL;
        }   // if
        else
        {
            pthread_mutex_lock(&tdata.m_Mutex);
            tdata.m_DirectWork=NULL;
            tdata.m_Available=1;
            pthread_cond_wait(&tdata.m_Condition, &tdata.m_Mutex);
            tdata.m_Available=0;    // safety
            submission=(eleveldb::WorkTask *)tdata.m_DirectWork;
            pthread_mutex_unlock(&tdata.m_Mutex);
        }   // else
    }   // while

    return 0;

}   // eleveldb_write_thread_worker


ERL_NIF_TERM parse_open_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::Options& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
    {
        if (option[0] == eleveldb::ATOM_CREATE_IF_MISSING)
            opts.create_if_missing = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_ERROR_IF_EXISTS)
            opts.error_if_exists = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_PARANOID_CHECKS)
            opts.paranoid_checks = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_MAX_OPEN_FILES)
        {
            int max_open_files;
            if (enif_get_int(env, option[1], &max_open_files))
                opts.max_open_files = max_open_files;
        }
        else if (option[0] == eleveldb::ATOM_WRITE_BUFFER_SIZE)
        {
            unsigned long write_buffer_sz;
            if (enif_get_ulong(env, option[1], &write_buffer_sz))
                opts.write_buffer_size = write_buffer_sz;
        }
        else if (option[0] == eleveldb::ATOM_BLOCK_SIZE)
        {
            /* DEPRECATED: the old block_size atom was actually ignored. */
            unsigned long block_sz;
            enif_get_ulong(env, option[1], &block_sz); // ignore
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
        else if (option[0] == eleveldb::ATOM_CACHE_SIZE)
        {
            unsigned long cache_sz;
            if (enif_get_ulong(env, option[1], &cache_sz))
                if (cache_sz != 0)
                 {
                    opts.block_cache = leveldb::NewLRUCache(cache_sz);
                 }
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
    }

    return eleveldb::ATOM_OK;
}

ERL_NIF_TERM parse_read_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::ReadOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
    {
        if (option[0] == eleveldb::ATOM_VERIFY_CHECKSUMS)
            opts.verify_checksums = (option[1] == eleveldb::ATOM_TRUE);
        else if (option[0] == eleveldb::ATOM_FILL_CACHE)
            opts.fill_cache = (option[1] == eleveldb::ATOM_TRUE);
    }

    return eleveldb::ATOM_OK;
}

ERL_NIF_TERM parse_write_option(ErlNifEnv* env, ERL_NIF_TERM item, leveldb::WriteOptions& opts)
{
    int arity;
    const ERL_NIF_TERM* option;
    if (enif_get_tuple(env, item, &arity, &option))
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

#if 0
// Free dynamic elements of iterator - acquire lock before calling
static void free_itr(eleveldb_itr_handle* itr_handle)
{
    if (itr_handle->itr)
    {
        delete itr_handle->itr;
        itr_handle->itr = 0;
        itr_handle->db_handle->db->ReleaseSnapshot(itr_handle->snapshot);
    }   // if

    enif_free_env(itr_handle->itr_ref_env);

    if (NULL!=itr_handle->reuse_move)
    {
        itr_handle->reuse_move->RefDec();
        itr_handle->reuse_move=NULL;
    }   // if
}

// Free dynamic elements of database - acquire lock before calling
static bool free_db(ErlNifEnv* env, eleveldb_db_handle* db_handle)
{
    if (0 == db_handle)
     return false;

    bool result = true;

    // Tidy up any pending jobs:
    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    if (false == priv.thread_pool.complete_jobs_for(db_handle))
     result = false;

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
            MutexLock l(itr_handle->itr_lock);
            free_itr(*iters_it);
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

        delete db_handle->options;
        db_handle->options = NULL;
     }

    if (db_handle->db_lock)
     {
        enif_mutex_unlock(db_handle->db_lock);
        enif_mutex_destroy(db_handle->db_lock), db_handle->db_lock = 0;
     }

    return result;
}
#endif


namespace eleveldb {

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

    eleveldb::WorkTask *work_item = new eleveldb::OpenTask(env, caller_ref,
                                                              db_name, opts);

    if(false == priv.thread_pool.submit(work_item))
    {
        delete work_item;
        return enif_make_tuple2(env, eleveldb::ATOM_ERROR, caller_ref);
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
     return error_einval(env);

    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    // Construct a write batch:
    leveldb::WriteBatch* batch = new leveldb::WriteBatch;

    // Seed the batch's data:
    ERL_NIF_TERM result = fold(env, argv[2], write_batch_item, *batch);
    if(eleveldb::ATOM_OK != result)
    {
        return enif_make_tuple3(env, eleveldb::ATOM_ERROR, caller_ref,
                                enif_make_tuple2(env, eleveldb::ATOM_BAD_WRITE_ACTION,
                                                 result));
    }   // if

    leveldb::WriteOptions* opts = new leveldb::WriteOptions;
    fold(env, argv[3], parse_write_option, *opts);

    eleveldb::WorkTask* work_item = new eleveldb::WriteTask(env, caller_ref,
                                                            db_ptr.get(), batch, opts);

    if(false == priv.thread_pool.submit(work_item))
        return enif_make_tuple2(env, eleveldb::ATOM_ERROR, caller_ref);

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
        return error_einval(env);

    leveldb::ReadOptions *opts = new leveldb::ReadOptions();
    fold(env, opts_ref, parse_read_option, *opts);

    eleveldb::WorkTask *work_item = new eleveldb::GetTask(env, caller_ref,
                                                          db_ptr.get(), key_ref, opts);

    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    if(false == priv.thread_pool.submit(work_item))
        return enif_make_tuple2(env, eleveldb::ATOM_ERROR, caller_ref);

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

    if(NULL==db_ptr.get()
       || !enif_is_list(env, options_ref))
     {
        return enif_make_badarg(env);
     }

    // likely useless
    if(NULL == db_ptr->m_Db)
        return error_einval(env);

    // Parse out the read options
    leveldb::ReadOptions *opts = new leveldb::ReadOptions;
    fold(env, options_ref, parse_read_option, *opts);

    eleveldb::WorkTask *work_item = new eleveldb::IterTask(env, caller_ref,
                                                           db_ptr.get(), keys_only, opts);

    // Now-boilerplate setup (we'll consolidate this pattern soon, I hope):
    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    if(false == priv.thread_pool.submit(work_item))
        return enif_make_tuple2(env, ATOM_ERROR, caller_ref);

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

    bool submit_new_request(true);

    ReferencePtr<ItrObject> itr_ptr;

    itr_ptr.assign(ItrObject::RetrieveItrObject(env, itr_handle_ref));

    if(NULL==itr_ptr.get())
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
    }   // if

    // before we launch a background job for "next iteration", see if there is a
    //  prefetch waiting for us (SEEK always "wins" and that condition is used)
    // if (eleveldb::detail::compare_and_swap(&itr_ptr->m_handoff_atomic, 0, 1))
    if (__sync_bool_compare_and_swap(&itr_ptr->m_handoff_atomic, 0, 1)
        || eleveldb::MoveTask::NEXT != action)
    {
        // nope
        ret_term = enif_make_copy(env, itr_ptr->itr_ref);

        leveldb::gPerfCounters->Inc(leveldb::ePerfDebug1);
        submit_new_request=(eleveldb::MoveTask::NEXT != action);
    }
    else
    {
        // why yes there is.  copy the key/value info into a return tuple before
        //  we launch the iterator for "next" again
        if(!itr_ptr->itr->Valid())
            ret_term=enif_make_tuple2(env, ATOM_ERROR, ATOM_INVALID_ITERATOR);

        else if (itr_ptr->keys_only)
            ret_term=enif_make_tuple2(env, ATOM_OK, slice_to_binary(env, itr_ptr->itr->key()));
        else
            ret_term=enif_make_tuple3(env, ATOM_OK,
                                      slice_to_binary(env, itr_ptr->itr->key()),
                                      slice_to_binary(env, itr_ptr->itr->value()));

        // reset for next race
        itr_ptr->m_handoff_atomic=0;
        submit_new_request=true;
    }   // else


    // only build request if actually need to submit it
    if (submit_new_request)
    {
        eleveldb::MoveTask * move_item;

        move_item=itr_ptr->reuse_move;

        // old item could still be exiting the thread, cannot
        //  reuse
        if (NULL!=move_item)
        {
            move_item->RefDec();
            move_item=NULL;
        }   // if

        move_item = new eleveldb::MoveTask(env, caller_ref,
                                           itr_ptr.get(), action);

        // prevent deletes during worker loop
        move_item->RefInc();
        itr_ptr->reuse_move=move_item;

        move_item->action=action;

        if (eleveldb::MoveTask::SEEK == action)
        {
            ErlNifBinary key;

            if(!enif_inspect_binary(env, action_or_target, &key))
            {
                move_item->RefDec();
                itr_ptr->reuse_move=NULL;
                return enif_make_tuple2(env, ATOM_EINVAL, caller_ref);
            }   // if

            move_item->seek_target.assign((const char *)key.data, key.size);
        }   // else

        eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

        if(false == priv.thread_pool.submit(move_item))
        {
            move_item->RefDec();
            itr_ptr->reuse_move=NULL;
            return enif_make_tuple2(env, ATOM_ERROR, caller_ref);
        }   // if
    }   // if

    return ret_term;

}   // async_iter_move


} // namespace eleveldb


ERL_NIF_TERM
eleveldb_close(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    eleveldb::DbObject * db_ptr;
    ERL_NIF_TERM ret_term;

    ret_term=eleveldb::ATOM_OK;

    db_ptr=eleveldb::DbObject::RetrieveDbObject(env, argv[0]);

    if (NULL!=db_ptr)
    {
        // set closing flag ... atomic likely unnecessary (but safer)
        __sync_bool_compare_and_swap(&db_ptr->m_CloseRequested, 0, 1);

        // remove reference count from the RetrieveDbObject call above
        db_ptr->RefDec();

        // remove "open database" from reference count,
        //  but it does NOT have a matching erlang ref.
        // ... db_ptr no longer known valid after call
        db_ptr->RefDec(false);
        db_ptr=NULL;

        ret_term=eleveldb::ATOM_OK;
    }   // if
    else
    {
        ret_term=enif_make_badarg(env);
    }   // else

    return(ret_term);

}  // eleveldb_close


ERL_NIF_TERM
eleveldb_iterator_close(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    eleveldb::ItrObject * itr_ptr;
    ERL_NIF_TERM ret_term;

    ret_term=eleveldb::ATOM_OK;

    itr_ptr=eleveldb::ItrObject::RetrieveItrObject(env, argv[0]);

    if (NULL!=itr_ptr)
    {
        // set closing flag ... atomic likely unnecessary (but safer)
        __sync_bool_compare_and_swap(&itr_ptr->m_CloseRequested, 0, 1);

        // if there is an active move object, set it up to delete
        //  (reuse_move holds a counter to this object, which will
        //   release when move object destructs)
        if (NULL!=itr_ptr->reuse_move)
        {
            itr_ptr->reuse_move->RefDec();
            itr_ptr->reuse_move=NULL;
        }   // if

        // remove reference count from RetrieveItrObject call above
        itr_ptr->RefDec();

        // remove "open iterator" from reference count,
        //  but it does NOT have a matching erlang ref.
        // ... db_ptr no longer known valid after call
        itr_ptr->RefDec(false);
        itr_ptr=NULL;

        ret_term=eleveldb::ATOM_OK;
    }   // if
    else
    {
        ret_term=enif_make_badarg(env);
    }   // else

    return(ret_term);

}   // elveldb_iterator_close


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
eleveldb_destroy(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
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
            return error_tuple(env, eleveldb::ATOM_ERROR_DB_DESTROY, status);
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

}   // eleveldb_destroy


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


#if 0
static void eleveldb_db_resource_cleanup(ErlNifEnv* env, void* arg)
{
// free_db(env, reinterpret_cast<eleveldb_db_handle *>(arg));
}


static void eleveldb_itr_resource_cleanup(ErlNifEnv* env, void* arg)
{
#if 0
    // Delete any dynamically allocated memory stored in eleveldb_itr_handle
    eleveldb_itr_handle* itr_handle = (eleveldb_itr_handle*)arg;

    // No need to lock iter - it's the last reference
    if (itr_handle->itr != 0)
    {
    MutexLock l(itr_handle->db_handle->db_lock);

        if (itr_handle->db_handle->iters)
        {
            itr_handle->db_handle->iters->erase(itr_handle);
        }
        free_itr(itr_handle);

        enif_release_resource(itr_handle->db_handle);  // matches keep in eleveldb_iterator()
    }

    enif_mutex_destroy(itr_handle->itr_lock);
#endif
}
#endif

static void on_unload(ErlNifEnv *env, void *priv_data)
{
    eleveldb_priv_data *p = static_cast<eleveldb_priv_data *>(priv_data);
    delete p;
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
try
{
    *priv_data = 0;

    // inform erlang of our two resource types
    eleveldb::DbObject::CreateDbObjectType(env);
    eleveldb::ItrObject::CreateItrObjectType(env);

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
        throw std::invalid_argument("on_load::load_info");

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
             throw std::invalid_argument("on_load::n_threads");

            if(0 >= local.n_threads || N_THREADS_MAX < static_cast<size_t>(local.n_threads))
             throw std::range_error("on_load::n_threads");
         }
     }

    /* Spin up the thread pool, set up all private data: */
    eleveldb_priv_data *priv = new eleveldb_priv_data(local.n_threads);

    *priv_data = priv;

    // Initialize common atoms

#define ATOM(Id, Value) { Id = enif_make_atom(env, Value); }
    ATOM(eleveldb::ATOM_OK, "ok");
    ATOM(eleveldb::ATOM_ERROR, "error");
    ATOM(eleveldb::ATOM_EINVAL, "einval");
    ATOM(eleveldb::ATOM_TRUE, "true");
    ATOM(eleveldb::ATOM_FALSE, "false");
    ATOM(eleveldb::ATOM_CREATE_IF_MISSING, "create_if_missing");
    ATOM(eleveldb::ATOM_ERROR_IF_EXISTS, "error_if_exists");
    ATOM(eleveldb::ATOM_WRITE_BUFFER_SIZE, "write_buffer_size");
    ATOM(eleveldb::ATOM_MAX_OPEN_FILES, "max_open_files");
    ATOM(eleveldb::ATOM_BLOCK_SIZE, "block_size");
    ATOM(eleveldb::ATOM_SST_BLOCK_SIZE, "sst_block_size");
    ATOM(eleveldb::ATOM_BLOCK_RESTART_INTERVAL, "block_restart_interval");
    ATOM(eleveldb::ATOM_ERROR_DB_OPEN,"db_open");
    ATOM(eleveldb::ATOM_ERROR_DB_PUT, "db_put");
    ATOM(eleveldb::ATOM_NOT_FOUND, "not_found");
    ATOM(eleveldb::ATOM_VERIFY_CHECKSUMS, "verify_checksums");
    ATOM(eleveldb::ATOM_FILL_CACHE,"fill_cache");
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
    ATOM(eleveldb::ATOM_INVALID_ITERATOR, "invalid_iterator");
    ATOM(eleveldb::ATOM_CACHE_SIZE, "cache_size");
    ATOM(eleveldb::ATOM_PARANOID_CHECKS, "paranoid_checks");
    ATOM(eleveldb::ATOM_ERROR_DB_DESTROY, "error_db_destroy");
    ATOM(eleveldb::ATOM_ERROR_DB_REPAIR, "error_db_repair");
    ATOM(eleveldb::ATOM_KEYS_ONLY, "keys_only");
    ATOM(eleveldb::ATOM_COMPRESSION, "compression");
    ATOM(eleveldb::ATOM_USE_BLOOMFILTER, "use_bloomfilter");

#undef ATOM

    return 0;
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
