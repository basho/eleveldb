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

#include <sstream>
#include <stdexcept>

#ifndef INCL_THREADING_H
    #include "threading.h"
#endif

#ifndef INCL_WORKITEMS_H
    #include "workitems.h"
#endif

#ifndef __ELEVELDB_DETAIL_HPP
    #include "detail.hpp"
#endif

namespace eleveldb {

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


bool                           // returns true if available worker thread found and claimed
eleveldb_thread_pool::FindWaitingThread(
    eleveldb::WorkTask * work) // non-NULL to pass current work directly to a thread,
                               // NULL to potentially nudge an available worker toward backlog queue
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
         // perform quick test to see thread available
         if (0!=threads[index]->m_Available)
         {
             // perform expensive compare and swap to potentially
             //  claim worker thread (this is an exclusive claim to the worker)
             ret_flag = eleveldb::compare_and_swap(&threads[index]->m_Available, 1, 0);

             // the compare/swap only succeeds if worker thread is sitting on
             //  pthread_cond_wait ... or is about to be there but is holding
             //  the mutex already
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


 bool eleveldb_thread_pool::submit(eleveldb::WorkTask* item)
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
             eleveldb::inc_and_fetch(&work_queue_atomic);
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

  // not clear that this works or is testable
 bool eleveldb_thread_pool::resize_thread_pool(const size_t n)
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
//  may not work at this time ...
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


// Shut down and destroy all threads in the thread pool:
//   does not currently work
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

/**
 * Worker threads:  worker threads have 3 states:
 *  A. doing nothing, available to be claimed: m_Available=1
 *  B. processing work passed by Erlang thread: m_Available=0, m_DirectWork=<non-null>
 *  C. processing backlog queue of work: m_Available=0, m_DirectWork=NULL
 */
void *eleveldb_write_thread_worker(void *args)
{
    ThreadData &tdata = *(ThreadData *)args;
    eleveldb_thread_pool& h = tdata.m_Pool;
    eleveldb::WorkTask * submission;

    submission=NULL;

    while(!h.shutdown)
    {
        // is work assigned yet?
        //  check backlog work queue if not
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
                    eleveldb::dec_and_fetch(&h.work_queue_atomic);
                    h.perf()->Inc(leveldb::ePerfElevelDequeued);
                }   // if

                h.unlock();
            }   // if
        }   // if


        // a work item identified (direct or queue), work it!
        //  then loop to test queue again
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

        // no work found, attempt to go into wait state
        //  (but retest queue before sleep due to race condition)
        else
        {
            pthread_mutex_lock(&tdata.m_Mutex);
            tdata.m_DirectWork=NULL; // safety

            // only wait if we are really sure no work pending
            if (0==h.work_queue_atomic)
	    {
                // yes, thread going to wait. set available now.
	        tdata.m_Available=1;
                pthread_cond_wait(&tdata.m_Condition, &tdata.m_Mutex);
	    }    // if

            tdata.m_Available=0;    // safety
            submission=(eleveldb::WorkTask *)tdata.m_DirectWork; // NULL is valid
            tdata.m_DirectWork=NULL;// safety

            pthread_mutex_unlock(&tdata.m_Mutex);
        }   // else
    }   // while

    return 0;

}   // eleveldb_write_thread_worker

};  // namespace eleveldb
