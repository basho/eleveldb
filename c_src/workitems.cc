// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2015 Basho Technologies, Inc. All Rights Reserved.
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

#ifndef INCL_WORKITEMS_H
    #include "workitems.h"
#endif

#include "leveldb/atomics.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/perf_count.h"

// error_tuple duplicated in workitems.cc and eleveldb.cc ... how to fix?
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


namespace eleveldb {


/**
 * WorkTask functions
 */


WorkTask::WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref)
    : terms_set(false)
{
    if (NULL!=caller_env)
    {
        local_env_ = enif_alloc_env();
        caller_ref_term = enif_make_copy(local_env_, caller_ref);
        caller_pid_term = enif_make_pid(local_env_, enif_self(caller_env, &local_pid));
        terms_set=true;
    }   // if
    else
    {
        local_env_=NULL;
        terms_set=false;
    }   // else

    return;

}   // WorkTask::WorkTask


WorkTask::WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref, DbObject * DbPtr)
    : m_DbPtr(DbPtr), terms_set(false)
{
    if (NULL!=caller_env)
    {
        local_env_ = enif_alloc_env();
        caller_ref_term = enif_make_copy(local_env_, caller_ref);
        caller_pid_term = enif_make_pid(local_env_, enif_self(caller_env, &local_pid));
        terms_set=true;
    }   // if
    else
    {
        local_env_=NULL;
        terms_set=false;
    }   // else

    return;

}   // WorkTask::WorkTask


WorkTask::~WorkTask()
{
    ErlNifEnv * env_ptr;

    // this is likely overkill in the present code, but seemed
    //  important at one time and leaving for safety
    env_ptr=local_env_;
    if (leveldb::compare_and_swap(&local_env_, env_ptr, (ErlNifEnv *)NULL)
        && NULL!=env_ptr)
    {
        enif_free_env(env_ptr);
    }   // if

    return;

}   // WorkTask::~WorkTask


void
WorkTask::operator()()
{
    // call the DoWork() method defined by the subclass
    basho::async_nif::work_result result = DoWork();
    if (result.is_set())
    {
        ErlNifPid pid;
        if(0 != enif_get_local_pid(this->local_env(), this->pid(), &pid))
        {
            /* Assemble a notification of the following form:
               { PID CallerHandle, ERL_NIF_TERM result } */
            ERL_NIF_TERM result_tuple = enif_make_tuple2(this->local_env(),
                                                         this->caller_ref(),
                                                         result.result());

            enif_send(0, &pid, this->local_env(), result_tuple);
        }
    }
}


/**
 * OpenTask functions
 */

OpenTask::OpenTask(
    ErlNifEnv* caller_env,
    ERL_NIF_TERM& _caller_ref,
    const std::string& db_name_,
    leveldb::Options *open_options_)
    : WorkTask(caller_env, _caller_ref),
    db_name(db_name_), open_options(open_options_)
{
}   // OpenTask::OpenTask


work_result
OpenTask::DoWork()
{
    void * db_ptr_ptr;
    leveldb::DB *db(0);

    leveldb::Status status = leveldb::DB::Open(*open_options, db_name, &db);

    if(!status.ok())
        return error_tuple(local_env(), ATOM_ERROR_DB_OPEN, status);

    db_ptr_ptr=DbObject::CreateDbObject(db, open_options);

    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(local_env(), db_ptr_ptr);

    // clear the automatic reference from enif_alloc_resource in CreateDbObject
    enif_release_resource(db_ptr_ptr);

    return work_result(local_env(), ATOM_OK, result);

}   // OpenTask::DoWork()



/**
 * MoveTask functions
 */

work_result
MoveTask::DoWork()
{
    leveldb::Iterator* itr;

    itr=m_ItrWrap->get();

    ++m_ItrWrap->m_MoveCount;
//
// race condition of prefetch clearing db iterator while
//  async_iterator_move looking at it.
//

    // iterator_refresh operation
    if (m_ItrWrap->m_Options.iterator_refresh && m_ItrWrap->m_StillUse)
    {
        struct timeval tv;

        gettimeofday(&tv, NULL);

        if (m_ItrWrap->m_IteratorStale < tv.tv_sec || NULL==itr)
        {
            m_ItrWrap->RebuildIterator();
            itr=m_ItrWrap->get();

            // recover position
            if (NULL!=itr && 0!=m_ItrWrap->m_RecentKey.size())
            {
                leveldb::Slice key_slice(m_ItrWrap->m_RecentKey);

                itr->Seek(key_slice);
                m_ItrWrap->m_StillUse=itr->Valid();
                if (!m_ItrWrap->m_StillUse)
                {
                    itr=NULL;
                    m_ItrWrap->PurgeIterator();
                }   // if
            }   // if
        }   // if
    }   // if

    // hung iterator debug
    {
        struct timeval tv;

        gettimeofday(&tv, NULL);

        // 14400 is 4 hours in seconds ... 60*60*4
        if ((m_ItrWrap->m_LastLogReport + 14400) < tv.tv_sec && NULL!=m_ItrWrap->get())
        {
            m_ItrWrap->LogIterator();
            m_ItrWrap->m_LastLogReport=tv.tv_sec;
        }   // if
    }

    // back to normal operation
    if(NULL == itr)
        return work_result(local_env(), ATOM_ERROR, ATOM_ITERATOR_CLOSED);

    switch(action)
    {
        case FIRST: itr->SeekToFirst(); break;

        case LAST:  itr->SeekToLast();  break;

        case PREFETCH:
        case PREFETCH_STOP:
        case NEXT:  if(itr->Valid()) itr->Next(); break;

        case PREV:  if(itr->Valid()) itr->Prev(); break;

        case SEEK:
        {
            leveldb::Slice key_slice(seek_target);

            itr->Seek(key_slice);
            break;
        }   // case

        default:
            // JFW: note: *not* { ERROR, badarg() } here-- we want the exception:
            // JDB: note: We can't send an exception as a message. It crashes Erlang.
            //            Changing to be {error, badarg}.
            return work_result(local_env(), ATOM_ERROR, ATOM_BADARG);
            break;

    }   // switch

    // set state for Erlang side to read
    m_ItrWrap->SetValid(itr->Valid());

    // Post processing before telling the world the results
    //  (while only one thread might be looking at objects)
    if (m_ItrWrap->m_Options.iterator_refresh)
    {
        if (itr->Valid())
        {
            m_ItrWrap->m_RecentKey.assign(itr->key().data(), itr->key().size());
        }   // if
        else if (PREFETCH_STOP!=action)
        {
            // release iterator now, not later
            m_ItrWrap->m_StillUse=false;
            m_ItrWrap->PurgeIterator();
            itr=NULL;
        }   // else
    }   // if

    // debug syslog(LOG_ERR, "                     MoveItem::DoWork() %d, %d, %d",
    //              action, m_ItrWrap->m_StillUse, m_ItrWrap->m_HandoffAtomic);

    // who got back first, us or the erlang loop
    if (leveldb::compare_and_swap(&m_ItrWrap->m_HandoffAtomic, 0, 1))
    {
        // this is prefetch of next iteration.  It returned faster than actual
        //  request to retrieve it.  Stop and wait for erlang to catch up.
        //  (even if this result is an Invalid() )
    }   // if
    else
    {
        // setup next race for the response
        m_ItrWrap->m_HandoffAtomic=0;

        if(NULL!=itr && itr->Valid())
        {
            if (PREFETCH==action && m_ItrWrap->m_PrefetchStarted)
                m_ResubmitWork=true;

            // erlang is waiting, send message
            if(m_ItrWrap->m_KeysOnly)
                return work_result(local_env(), ATOM_OK, slice_to_binary(local_env(), itr->key()));

            return work_result(local_env(), ATOM_OK,
                               slice_to_binary(local_env(), itr->key()),
                               slice_to_binary(local_env(), itr->value()));
        }   // if
        else
        {
            // using compare_and_swap as a hardware locking "set to false"
            //  (a little heavy handed, but not executed often)
            leveldb::compare_and_swap(&m_ItrWrap->m_PrefetchStarted, (int)true, (int)false);
            return work_result(local_env(), ATOM_ERROR, ATOM_INVALID_ITERATOR);
        }   // else

    }   // else

    return(work_result());
}


ErlNifEnv *
MoveTask::local_env()
{
    if (NULL==local_env_)
        local_env_ = enif_alloc_env();

    if (!terms_set)
    {
        caller_ref_term = enif_make_copy(local_env_, m_ItrWrap->itr_ref);
        caller_pid_term = enif_make_pid(local_env_, &local_pid);
        terms_set=true;
    }   // if

    return(local_env_);

}   // MoveTask::local_env


void
MoveTask::recycle()
{
    // test for race condition of simultaneous delete & recycle
    if (1<RefInc())
    {
        if (NULL!=local_env_)
            enif_clear_env(local_env_);

        terms_set=false;
        m_ResubmitWork=false;

        // only do this in non-race condition
        RefDec();
    }   // if
    else
    {
        // touch NOTHING
    }   // else

}   // MoveTask::recycle

/**
 * DestroyTask functions
 */

DestroyTask::DestroyTask(
    ErlNifEnv* caller_env,
    ERL_NIF_TERM& _caller_ref,
    const std::string& db_name_,
    leveldb::Options *open_options_)
    : WorkTask(caller_env, _caller_ref),
    db_name(db_name_), open_options(open_options_)
{
}   // DestroyTask::DestroyTask


work_result
DestroyTask::DoWork()
{
    leveldb::Status status = leveldb::DestroyDB(db_name, *open_options);

    if(!status.ok())
        return error_tuple(local_env(), ATOM_ERROR_DB_DESTROY, status);

    return work_result(ATOM_OK);

}   // DestroyTask::DoWork()


} // namespace eleveldb


