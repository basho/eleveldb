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
#ifndef INCL_THREADING_H
#define INCL_THREADING_H

#include <deque>
#include <vector>
#include "leveldb/perf_count.h"

#ifndef INCL_MUTEX_H
    #include "mutex.h"
#endif

#ifndef INCL_ELEVELDB_H
    #include "eleveldb.h"
#endif

namespace eleveldb {

// constant
const size_t N_THREADS_MAX = 32767;

// forward declare
struct ThreadData;
class WorkTask;


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

    bool FindWaitingThread(eleveldb::WorkTask * work);

    bool submit(eleveldb::WorkTask* item);

    bool resize_thread_pool(const size_t n);

    size_t work_queue_size() const { return work_queue.size(); }
    bool shutdown_pending() const  { return shutdown; }
    leveldb::PerformanceCounters * perf() const {return(leveldb::gPerfCounters);};


private:
    bool grow_thread_pool(const size_t nthreads);
    bool drain_thread_pool();

    static bool notify_caller(eleveldb::WorkTask& work_item);

};  // class eleveldb_thread_pool

} // namespace eleveldb


#endif  // INCL_THREADING_H
