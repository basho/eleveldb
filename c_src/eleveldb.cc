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

#ifdef OS_SOLARIS
#  include <atomic.h>
#endif

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

using std::copy;
using std::nothrow;
using std::make_pair;

static ErlNifResourceType* eleveldb_db_RESOURCE;
static ErlNifResourceType* eleveldb_itr_RESOURCE;

struct eleveldb_db_handle;
struct eleveldb_itr_handle;

class eleveldb_thread_pool;
class eleveldb_priv_data;

namespace {
const size_t N_THREADS_MAX = 32767;
}

/* Some primitive-yet-useful NIF helpers: */
namespace {

template <class T>
void *placement_alloc()
{
 void *placement = malloc(sizeof(T));
 if(0 == placement)
  throw std::bad_alloc();

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
 free(x);
}

// A relatively unsafe (no ownership semantics) mutex handle; allocates and releases with lifetime:
class simple_scoped_mutex_handle
{
 friend class simple_scoped_lock;

 ErlNifMutex* mutex;

 public:
 simple_scoped_mutex_handle(const std::string name = "simple_scoped_mutex_handle")
  : mutex(enif_mutex_create(const_cast<char *>(name.c_str())))
 {
    if(0 == mutex)
     throw std::exception();
 }

 ~simple_scoped_mutex_handle()
 {
    enif_mutex_destroy(mutex);
 }
};

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

 simple_scoped_lock(simple_scoped_mutex_handle& _lock)
  : lock(_lock.mutex)
 {
    enif_mutex_lock(lock);
 }

 ~simple_scoped_lock()
 {
    enif_mutex_unlock(lock);
 }
};

/* Increment refcount on construction, decrement on destruction:
 Note: I should generalize these "simple scoped X" classes at some point... I see a pattern. Of course, more robust smart
       pointers would be even better. We'll see on the next pass. An example of what this would make easier is that, among
       other things, we could have handles that were effectively ignorant of things like refcount preservation-- and a lot
       safer.
*/
template <class TargetT>
class simple_scoped_refcount
{
 TargetT *t;    // assumes this won't vanish!

 public:
 simple_scoped_refcount(TargetT *_t)
  : t(_t)
 {
    if(0 == t)
     throw std::exception();

    enif_keep_resource(t);
 }

 ~simple_scoped_refcount()
 {
    enif_release_resource(t);
 }
};

} // namespace

// Erlang helpers:
namespace {
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

static ERL_NIF_TERM slice_to_binary(ErlNifEnv* env, leveldb::Slice s)
{
    ERL_NIF_TERM result;
    unsigned char* value = enif_make_new_binary(env, s.size(), &result);
    memcpy(value, s.data(), s.size());
    return result;
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

void *eleveldb_write_thread_worker(void *args);

/* This is all a shade hacky, we are in a time crunch: */
namespace eleveldb {

/* Type returned from a work task: */
typedef std::pair<bool, ERL_NIF_TERM>   work_result_t;

/* Virtual base class ("interface") for async NIF work items: */
class work_task_t
{
 protected:
 ErlNifEnv      *local_env_;

 ERL_NIF_TERM   caller_ref_term,
                caller_pid_term;

 private:
 ErlNifPid local_pid;   // maintain for task lifetime (JFW)

 public:
 work_task_t(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref)
 {
    local_env_ = enif_alloc_env();

    if(0 == local_env_)
     throw std::invalid_argument("work_task_t::local_env_");

    caller_ref_term = enif_make_copy(local_env_, caller_ref);

    caller_pid_term = enif_make_pid(local_env_, enif_self(caller_env, &local_pid));
 }

 virtual ~work_task_t()
 {
    enif_free_env(local_env_);
 }

 ErlNifEnv *local_env() const           { return local_env_; }

 const ERL_NIF_TERM& caller_ref() const { return caller_ref_term; }
 const ERL_NIF_TERM& pid() const        { return caller_pid_term; }

 virtual work_result_t operator()()     = 0;
};

struct open_task_t : public work_task_t
{
 std::string         db_name;
 leveldb::Options   *open_options;  // associated with db handle, we don't free it

 open_task_t(ErlNifEnv* caller_env, ERL_NIF_TERM& _caller_ref,
             const std::string& db_name_, leveldb::Options *open_options_)
  : work_task_t(caller_env, _caller_ref),
    db_name(db_name_), open_options(open_options_)
 {}

 work_result_t operator()()
 {
    leveldb::DB *db(0);

    leveldb::Status status = leveldb::DB::Open(*open_options, db_name, &db);

    if(!status.ok())
     return make_pair(false, error_tuple(local_env(), ATOM_ERROR_DB_OPEN, status));

    eleveldb_db_handle* handle = (eleveldb_db_handle*)
     enif_alloc_resource(eleveldb_db_RESOURCE, sizeof(eleveldb_db_handle));
    memset(handle, '\0', sizeof(eleveldb_db_handle));
    handle->db = db;
    handle->db_lock = enif_mutex_create((char*)"eleveldb_db_lock");
    handle->options = open_options;
    handle->iters = new std::set<struct eleveldb_itr_handle*>();

    ERL_NIF_TERM result = enif_make_resource(local_env(), handle);

    enif_release_resource(handle);

    return make_pair(true, result);
 }
};

struct iter_task_t : public work_task_t
{
 eleveldb_db_handle *db_handle;
 const bool keys_only;
 leveldb::ReadOptions *options;

 iter_task_t(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
             eleveldb_db_handle *_db_handle, const bool _keys_only, leveldb::ReadOptions *_options)
  : work_task_t(_caller_env, _caller_ref),
    db_handle(_db_handle), keys_only(_keys_only), options(_options)
 {}

 ~iter_task_t()
 {
    placement_dtor(options);
 }

 work_result_t operator()()
 {
    eleveldb_itr_handle* itr_handle =
            (eleveldb_itr_handle*) enif_alloc_resource(eleveldb_itr_RESOURCE,
                                                       sizeof(eleveldb_itr_handle));
    memset(itr_handle, '\0', sizeof(eleveldb_itr_handle));

    // Initialize itr handle
    itr_handle->itr_lock = enif_mutex_create((char*)"eleveldb_itr_lock");
    itr_handle->db_handle = db_handle;

    itr_handle->snapshot = db_handle->db->GetSnapshot();
    options->snapshot = itr_handle->snapshot;

    itr_handle->itr = db_handle->db->NewIterator(*options);
    itr_handle->keys_only = keys_only;

    ERL_NIF_TERM result = enif_make_resource(local_env(), itr_handle);

    enif_mutex_lock(db_handle->db_lock);
    db_handle->iters->insert(itr_handle);
    enif_mutex_unlock(db_handle->db_lock);

    enif_release_resource(itr_handle);

    return make_pair(true, result);
 }
};

struct iter_move_task_t : public work_task_t
{
 typedef enum { FIRST, LAST, NEXT, PREV, SEEK } action_t;

 simple_scoped_refcount<eleveldb_itr_handle>    itr_handle_refcount;
 mutable eleveldb_itr_handle*                   itr_handle;

 action_t                                       action;

 ERL_NIF_TERM                                   seek_target;

 // No seek target:
 iter_move_task_t(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
                  eleveldb_itr_handle *_itr_handle,
                  action_t& _action)
 : work_task_t(_caller_env, _caller_ref),
   itr_handle_refcount(_itr_handle),
   itr_handle(_itr_handle),
   action(_action),
   seek_target(ATOM_ERROR)
 {}

 // With seek target:
 iter_move_task_t(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
                  eleveldb_itr_handle *_itr_handle,
                  action_t& _action,
                  ERL_NIF_TERM _seek_target)
 : work_task_t(_caller_env, _caller_ref),
   itr_handle_refcount(_itr_handle),
   itr_handle(_itr_handle),
   action(_action),
   seek_target(enif_make_copy(local_env_, _seek_target))
 {}

 work_result_t operator()()
 {
    simple_scoped_lock l(itr_handle->itr_lock);

    ErlNifBinary key;

    leveldb::Iterator* itr = itr_handle->itr;

    if(0 == itr)
     return make_pair(false, ATOM_ITERATOR_CLOSED);

    switch(action)
     {
        default:
                    return make_pair(false, ATOM_ERROR);
                    break;

        case FIRST:
                    itr->SeekToFirst();
                    break;

        case LAST:
                    itr->SeekToLast();
                    break;

        case NEXT:
                    if(!itr->Valid())
                     return make_pair(false, ATOM_ERROR);

                    itr->Next();
                    break;

        case PREV:
                    if(!itr->Valid())
                     return make_pair(false, ATOM_ERROR);

                    itr->Prev();
                    break;

        case SEEK:
                    if(!enif_inspect_binary(local_env(), seek_target, &key))
                     return make_pair(false, ATOM_ERROR);

                    leveldb::Slice key_slice(reinterpret_cast<char *>(key.data), key.size);

                    itr->Seek(key_slice);
                    break;
     }

    if(!itr->Valid())
     return make_pair(false, ATOM_INVALID_ITERATOR);

    if(itr_handle->keys_only)
     return make_pair(true, slice_to_binary(local_env(), itr->key()));

    return make_pair(true, enif_make_tuple2(local_env(),
                                            slice_to_binary(local_env(), itr->key()),
                                            slice_to_binary(local_env(), itr->value())));
 }
};

struct get_task_t : public work_task_t
{
 mutable eleveldb_db_handle*        db_handle;

 ERL_NIF_TERM                       key_term;
 leveldb::ReadOptions*              options;

 get_task_t(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
            eleveldb_db_handle *_db_handle,
            ERL_NIF_TERM _key_term,
            leveldb::ReadOptions *_options)
  : work_task_t(_caller_env, _caller_ref),
    db_handle(_db_handle),
    key_term(enif_make_copy(local_env_, _key_term)),
    options(_options)
 {}

 ~get_task_t()
 {
    placement_dtor(options);
 }

 work_result_t operator()()
 {
    ErlNifBinary key;

    if(!enif_inspect_binary(local_env(), key_term, &key))
     return make_pair(false, error_einval(local_env()));

    leveldb::Slice key_slice((const char*)key.data, key.size);

    simple_scoped_lock(db_handle->db_lock);

    std::string value;

    leveldb::Status status = db_handle->db->Get(*options, key_slice, &value);

    if(!status.ok())
     return make_pair(true, ATOM_NOT_FOUND);

    ERL_NIF_TERM value_bin;

    // The documentation does not say if this can fail:
    unsigned char *result = enif_make_new_binary(local_env(), value.size(), &value_bin);

    copy(value.data(), value.data() + value.size(), result);

    return make_pair(true, value_bin);
 }
};

struct write_task_t : public work_task_t
{
    mutable eleveldb_db_handle*     db_handle;
    mutable leveldb::WriteBatch*    batch;

    leveldb::WriteOptions*          options;

    write_task_t(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                eleveldb_db_handle* _db_handle,
                leveldb::WriteBatch* _batch,
                leveldb::WriteOptions* _options)
     : work_task_t(_owner_env, _caller_ref),
       db_handle(_db_handle),
       batch(_batch),
       options(_options)
    {}

    ~write_task_t()
    {
        placement_dtor(batch);
        placement_dtor(options);
    }

    work_result_t operator()()
    {
        simple_scoped_lock(db_handle->db_lock);

        leveldb::Status status = db_handle->db->Write(*options, batch);

        enif_release_resource(db_handle);   // decrement refcount of leveldb handle

        return std::make_pair(status.ok(), status.ok() ? ATOM_OK : ATOM_ERROR);
    }
 };

} // namespace eleveldb


/**
 * Meta / managment data related to a worker thread.
 */
struct ThreadData
{
    ErlNifTid * m_ErlTid;                //!< erlang handle for this thread
    volatile uint32_t m_Available;       //!< 1 if thread waiting, using standard type for atomic operation
    class eleveldb_thread_pool & m_Pool; //!< parent pool object
    volatile eleveldb::work_task_t * m_DirectWork; //!< work passed direct to thread

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

 typedef std::deque<eleveldb::work_task_t*> work_queue_t;
    // typedef std::stack<ErlNifTid *>            thread_pool_t;
 typedef std::vector<ThreadData *>   thread_pool_t;

private:
 thread_pool_t  threads;
 ErlNifMutex*   threads_lock;       // protect resizing of the thread pool
 simple_scoped_mutex_handle thread_resize_pool_mutex;

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


 bool FindWaitingThread(eleveldb::work_task_t * work)
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
             ret_flag=__sync_bool_compare_and_swap(&threads[index]->m_Available, 1, 0);
             if (ret_flag)
             {
                 threads[index]->m_DirectWork=work;
                 pthread_mutex_lock(&threads[index]->m_Mutex);
                 pthread_cond_signal(&threads[index]->m_Condition);
                 pthread_mutex_unlock(&threads[index]->m_Mutex);
             }   // if
         }   // if

         index=(index+1)%pool_size;

     } while(index!=start && !ret_flag);

     return(ret_flag);

 }   // FindWaitingThread


 bool submit(eleveldb::work_task_t* item)
 {
    if(shutdown_pending())
     {
        placement_dtor(item);

        return false;
     }

    // try to give work to a waiting thread first
    if (!FindWaitingThread(item))
    {
        // no waiting threads, put on backlog queue
        lock();
#ifdef OS_SOLARIS
        atomic_add_64(&work_queue_atomic, 1);
#else
        __sync_add_and_fetch(&work_queue_atomic, 1);
#endif
        work_queue.push_back(item);
        unlock();

        // race condition, thread might be waiting now
        FindWaitingThread(NULL);

        perf()->Inc(leveldb::ePerfElevelQueued);
    }   // if
    else
    {
        perf()->Inc(leveldb::ePerfElevelDirect);
    }   // else

    return true;

 }


 bool resize_thread_pool(const size_t n)
 {
    simple_scoped_lock l(thread_resize_pool_mutex);

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

 bool complete_jobs_for(eleveldb_db_handle* dbh);

 size_t work_queue_size() const { return work_queue.size(); }
 bool shutdown_pending() const  { return shutdown; }
 leveldb::PerformanceCounters * perf() const {return(leveldb::gPerfCounters);};


 private:

 bool grow_thread_pool(const size_t nthreads);
 bool drain_thread_pool();

 static bool notify_caller(eleveldb::work_task_t& work_item);
};

eleveldb_thread_pool::eleveldb_thread_pool(const size_t thread_pool_size)
  : threads_lock(0),
    work_queue_pending(0), work_queue_lock(0),
    shutdown(false)
{
 threads_lock = enif_mutex_create(const_cast<char *>("threads_lock"));
 if(0 == threads_lock)
  throw std::runtime_error("cannot create threads_lock");

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

 enif_mutex_destroy(threads_lock);
}

// Grow the thread pool by nthreads threads:
bool eleveldb_thread_pool::grow_thread_pool(const size_t nthreads)
{
 simple_scoped_lock l(threads_lock);
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

namespace {

// Utility predicate: true if db_handle in write task matches given db handle:
struct db_matches
{
    const eleveldb_db_handle* dbh;

    db_matches(const eleveldb_db_handle* _dbh)
     : dbh(_dbh)
    {}

    bool operator()(eleveldb::work_task_t*& rhs)
    {
        // We're only concerned with elements tied to our database handle:
        eleveldb::write_task_t *rhs_item = dynamic_cast<eleveldb::write_task_t *>(rhs);

        if(0 == rhs_item)
         return false;

        return dbh == rhs_item->db_handle;
    }
};

} // namespace

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
 simple_scoped_lock complete_jobs_lock(complete_jobs_mutex);

 // Stop new jobs coming in during shuffle; after that, appending jobs is fine:
 {
 simple_scoped_lock l(work_queue_lock);
 std::stable_partition(work_queue.begin(), work_queue.end(), m);
 }

 // Signal all threads and drain our work first:
 enif_cond_broadcast(work_queue_pending);
 while(not work_queue.empty() and m(work_queue.front()))
  ;
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

bool eleveldb_thread_pool::notify_caller(eleveldb::work_task_t& work_item)
{
 ErlNifPid pid;

 if(0 == enif_get_local_pid(work_item.local_env(), work_item.pid(), &pid))
  return false;

 // Call the work function:
 eleveldb::work_result_t result = work_item();

 /* Assemble a notification of the following form:
        { ATOM Status, PID CallerHandle, ERL_NIF_TERM result } */
 ERL_NIF_TERM result_tuple =
                enif_make_tuple3(work_item.local_env(),
                                 work_item.caller_ref(),
                                 (result.first ? ATOM_OK : ATOM_ERROR),
                                 result.second);

 return (0 != enif_send(0, &pid, work_item.local_env(), result_tuple));
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
    eleveldb::work_task_t * submission;

    while(!h.shutdown)
    {
        // cast away volatile
        submission=(eleveldb::work_task_t *)tdata.m_DirectWork;
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
#ifdef OS_SOLARIS
                    atomic_sub_64(&h.work_queue_atomic, 1);
#else
                    __sync_sub_and_fetch(&h.work_queue_atomic, 1);
#endif
                    h.perf()->Inc(leveldb::ePerfElevelDequeued);
                }   // if

                h.unlock();
            }   // if
        }   // if

        if (NULL!=submission)
        {
            eleveldb_thread_pool::notify_caller(*submission);
            placement_dtor(submission);
            submission=NULL;
            tdata.m_DirectWork=NULL;
        }   // if
        else
        {
            pthread_mutex_lock(&tdata.m_Mutex);
            tdata.m_Available=1;
            pthread_cond_wait(&tdata.m_Condition, &tdata.m_Mutex);
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
            // By default, we want to use a 16-bit-per-key bloom filter on a
            // per-table basis. We only disable it if explicitly asked. Alternatively,
            // one can provide a value for # of bits-per-key.
            unsigned long bfsize = 16;
            if (option[1] == ATOM_TRUE || enif_get_ulong(env, option[1], &bfsize))
            {
                opts.filter_policy = leveldb::NewBloomFilterPolicy2(bfsize);
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
            simple_scoped_lock l(itr_handle->itr_lock);
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

        placement_dtor(db_handle->options), db_handle->options = 0;
     }

    if (db_handle->db_lock)
     {
        enif_mutex_unlock(db_handle->db_lock);
        enif_mutex_destroy(db_handle->db_lock), db_handle->db_lock = 0;
     }

    return result;
}

namespace eleveldb {

ERL_NIF_TERM async_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
 char db_name[4096];

 if(!enif_get_string(env, argv[1], db_name, sizeof(db_name), ERL_NIF_LATIN1) ||
    !enif_is_list(env, argv[2]))
  {
    return enif_make_badarg(env);
  }

 ERL_NIF_TERM caller_ref = argv[0];

 eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

 leveldb::Options *opts = placement_ctor<leveldb::Options>();
 fold(env, argv[2], parse_open_option, *opts);

 eleveldb::work_task_t *work_item = placement_ctor<eleveldb::open_task_t>(
                                        env, caller_ref,
                                        db_name, opts
                                       );

 if(false == priv.thread_pool.submit(work_item))
  {
    placement_dtor(work_item);
    return enif_make_tuple2(env, ATOM_ERROR, caller_ref);
  }

 return ATOM_OK;
}

ERL_NIF_TERM async_get(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
 eleveldb_db_handle *db_handle = 0;

 const ERL_NIF_TERM& caller_ref = argv[0];
 const ERL_NIF_TERM& dbh_ref    = argv[1];
 const ERL_NIF_TERM& key_ref    = argv[2];
 const ERL_NIF_TERM& opts_ref   = argv[3];

 if(!enif_get_resource(env, dbh_ref, eleveldb_db_RESOURCE, (void **)&db_handle) ||
    !enif_is_list(env, opts_ref))
  {
    return enif_make_badarg(env);
  }

 if(0 == db_handle->db)
  return error_einval(env);

 eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

 leveldb::ReadOptions *opts = placement_ctor<leveldb::ReadOptions>();
 fold(env, opts_ref, parse_read_option, *opts);

 eleveldb::work_task_t *work_item = placement_ctor<eleveldb::get_task_t>(
                                        env, caller_ref,
                                        db_handle, key_ref, opts
                                       );

 if(false == priv.thread_pool.submit(work_item))
  return enif_make_tuple2(env, ATOM_ERROR, caller_ref);

 return ATOM_OK;
}

ERL_NIF_TERM async_iterator(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& caller_ref  = argv[0];
    const ERL_NIF_TERM& dbh_ref     = argv[1];
    const ERL_NIF_TERM& options_ref = argv[2];

#if 0
    const bool keys_only = (3 == argc && ATOM_KEYS_ONLY == options_ref) ? true : false;
#else
    const bool keys_only = ((argc == 4) && (argv[3] == ATOM_KEYS_ONLY));
#endif

    eleveldb_db_handle* db_handle;

    if (!enif_get_resource(env, dbh_ref, eleveldb_db_RESOURCE, (void**)&db_handle) &&
        !enif_is_list(env, options_ref))
     {
        return enif_make_badarg(env);
     }

    // simple_scoped_lock(db_handle->db_lock);

    if(0 == db_handle->db)
     return error_einval(env);

    // Increment references to db_handle for duration of the iterator
    enif_keep_resource(db_handle);

    // Parse out the read options
    leveldb::ReadOptions *opts = placement_ctor<leveldb::ReadOptions>();
    fold(env, options_ref, parse_read_option, *opts);

    // Now-boilerplate setup (we'll consolidate this pattern soon, I hope):
    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    eleveldb::work_task_t *work_item = placement_ctor<eleveldb::iter_task_t>(
                                            env, caller_ref,
                                            db_handle, keys_only, opts);

    if(false == priv.thread_pool.submit(work_item))
     return enif_make_tuple2(env, ATOM_ERROR, caller_ref);

    return ATOM_OK;
}

ERL_NIF_TERM async_iterator_move(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
 const ERL_NIF_TERM& caller_ref       = argv[0];
 const ERL_NIF_TERM& itr_handle_ref   = argv[1];
 const ERL_NIF_TERM& action_or_target = argv[2];

 eleveldb_itr_handle *itr_handle = 0;

 if(!enif_get_resource(env, itr_handle_ref, eleveldb_itr_RESOURCE, (void **)&itr_handle))
  return enif_make_badarg(env);

 // Now that we have our resource, lock it while we submit the job and increment the refcount:
 // simple_scoped_lock l(itr_handle->itr_lock);

 eleveldb::work_task_t *work_item = 0;

 /* We can be invoked with two different arities from Erlang. If our "action_atom" parameter is not
 in fact an atom, then it is actually a seek target. Let's find out which we are: */

 eleveldb::iter_move_task_t::action_t action = eleveldb::iter_move_task_t::SEEK;

 // If we have an atom, it's one of these (action_or_target's value is ignored):
 if(enif_is_atom(env, action_or_target))
  {
    if(ATOM_FIRST == action_or_target)  action = eleveldb::iter_move_task_t::FIRST;
    if(ATOM_LAST == action_or_target)   action = eleveldb::iter_move_task_t::LAST;
    if(ATOM_NEXT == action_or_target)   action = eleveldb::iter_move_task_t::NEXT;
    if(ATOM_PREV == action_or_target)   action = eleveldb::iter_move_task_t::PREV;

    work_item = placement_ctor<eleveldb::iter_move_task_t>(
                 env, caller_ref,
                 itr_handle, action
                );
  }
 else
  {
    work_item = placement_ctor<eleveldb::iter_move_task_t>(
                 env, caller_ref,
                 itr_handle, action, action_or_target
                );
  }

 eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

 if(false == priv.thread_pool.submit(work_item))
  return enif_make_tuple2(env, ATOM_ERROR, caller_ref);

 return ATOM_OK;
}

ERL_NIF_TERM async_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    const ERL_NIF_TERM& caller_ref = argv[0];
    const ERL_NIF_TERM& handle_ref = argv[1];
    const ERL_NIF_TERM& action_ref = argv[2];
    const ERL_NIF_TERM& opts_ref   = argv[3];

    eleveldb_db_handle* handle(0);

    if(!enif_get_resource(env, handle_ref, eleveldb_db_RESOURCE, (void**)&handle) ||
       !enif_is_list(env, action_ref) ||
       !enif_is_list(env, opts_ref))
    {
        return enif_make_badarg(env);
    }

    if(0 == handle->db)
     return error_einval(env);

    eleveldb_priv_data& priv = *static_cast<eleveldb_priv_data *>(enif_priv_data(env));

    // Construct a write batch:
    leveldb::WriteBatch* batch = placement_ctor<leveldb::WriteBatch>();

    // Seed the batch's data:
    ERL_NIF_TERM result = fold(env, argv[2], write_batch_item, *batch);
    if(ATOM_OK != result)
     {
        return enif_make_tuple3(env, ATOM_ERROR, caller_ref,
                                     enif_make_tuple2(env, ATOM_BAD_WRITE_ACTION,
                                                      result));
     }

    leveldb::WriteOptions* opts = placement_ctor<leveldb::WriteOptions>();
    fold(env, argv[3], parse_write_option, *opts);

    // Increment the refcount on the database handle so it doesn't vanish:
    enif_keep_resource(handle);

    eleveldb::work_task_t* work_item = placement_ctor<eleveldb::write_task_t>(
                                        env, caller_ref,
                                        handle, batch, opts
                                       );

    if(false == priv.thread_pool.submit(work_item))
     return enif_make_tuple2(env, ATOM_ERROR, caller_ref);

    return ATOM_OK;
}

} // namespace eleveldb

ERL_NIF_TERM eleveldb_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    eleveldb_db_handle* db_handle;

    if (!enif_get_resource(env, argv[0], eleveldb_db_RESOURCE, (void**)&db_handle))
     return enif_make_badarg(env);

    return free_db(env, db_handle) ? ATOM_OK : error_einval(env);
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
        simple_scoped_lock(db_handle->db_lock);

        if (db_handle->db == NULL)
        {
            return error_einval(env);
        }

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
        simple_scoped_lock(db_handle->db_lock);

        if (db_handle->db == NULL)
        {
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

        return result;
    }
    else
    {
        return enif_make_badarg(env);
    }
}

static void eleveldb_db_resource_cleanup(ErlNifEnv* env, void* arg)
{
 free_db(env, reinterpret_cast<eleveldb_db_handle *>(arg));
}

static void eleveldb_itr_resource_cleanup(ErlNifEnv* env, void* arg)
{
    // Delete any dynamically allocated memory stored in eleveldb_itr_handle
    eleveldb_itr_handle* itr_handle = (eleveldb_itr_handle*)arg;

    // No need to lock iter - it's the last reference
    if (itr_handle->itr != 0)
    {
    simple_scoped_lock l(itr_handle->db_handle->db_lock);

        if (itr_handle->db_handle->iters)
        {
            itr_handle->db_handle->iters->erase(itr_handle);
        }
        free_itr(itr_handle);

        enif_release_resource(itr_handle->db_handle);  // matches keep in eleveldb_iterator()
    }

    enif_mutex_destroy(itr_handle->itr_lock);
}

static void on_unload(ErlNifEnv *env, void *priv_data)
{
 eleveldb_priv_data *p = static_cast<eleveldb_priv_data *>(priv_data);
 placement_dtor(p);
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
try
{
    *priv_data = 0;

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
    eleveldb_priv_data *priv = placement_ctor<eleveldb_priv_data>(local.n_threads);

    *priv_data = priv;

    // Initialize common atoms

#define ATOM(Id, Value) { Id = enif_make_atom(env, Value); }
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
