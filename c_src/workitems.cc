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
#ifndef __ELEVELDB_DETAIL_HPP
    #include "detail.hpp"
#endif

#ifndef INCL_WORKITEMS_H
    #include "workitems.h"
#endif

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
 * DbObject Functions
 */

ErlNifResourceType * DbObject::m_Db_RESOURCE(NULL);


void
DbObject::CreateDbObjectType(
    ErlNifEnv * Env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_Db_RESOURCE = enif_open_resource_type(Env, NULL, "eleveldb_DbObject",
                                            &DbObject::DbObjectResourceCleanup,
                                            flags, NULL);

    return;

}   // DbObject::CreateDbObjectType


DbObject *
DbObject::CreateDbObject(
    leveldb::DB * Db,
    leveldb::Options * DbOptions)
{
    DbObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_Db_RESOURCE, sizeof(DbObject));

    ret_ptr=new (alloc_ptr) DbObject(Db, DbOptions);

    // manual reference increase to keep active until "close" called
    //  only inc local counter, leave erl ref count alone ... will force
    //  erlang to call us if process holding ref dies
    ret_ptr->RefInc(false);

    // see OpenTask::operator() for release of reference count

    return(ret_ptr);

}   // DbObject::CreateDbObject


DbObject *
DbObject::RetrieveDbObject(
    ErlNifEnv * Env,
    const ERL_NIF_TERM & DbTerm)
{
    DbObject * ret_ptr;

    ret_ptr=NULL;

    if (enif_get_resource(Env, DbTerm, m_Db_RESOURCE, (void **)&ret_ptr))
    {
        // increment erlang normal GC count, then our "fast" count
        ret_ptr->RefInc();

        // has close been requested?
        if (ret_ptr->m_CloseRequested)
        {
            // object already closing
            ret_ptr->RefDec();
            ret_ptr=NULL;
        }   // else
    }   // if

    return(ret_ptr);

}   // DbObject::RetrieveDbObject


void
DbObject::DbObjectResourceCleanup(
    ErlNifEnv * Env,
    void * Arg)
{
    DbObject * db_ptr;

    db_ptr=(DbObject *)Arg;

    if (compare_and_swap(&db_ptr->m_CloseRequested, 0, 1))
    {
        // clear C++ only reference
        db_ptr->RefDec(false);

    }   // if

    // has object completed activities AND destructor called
    if (3!=db_ptr->m_CloseRequested)
    {
        pthread_mutex_lock(&db_ptr->m_CloseMutex);
        if (3!=db_ptr->m_CloseRequested)
        {
            pthread_cond_wait(&db_ptr->m_CloseCond, &db_ptr->m_CloseMutex);
        }   // if
        pthread_mutex_unlock(&db_ptr->m_CloseMutex);
    }   // if

    // need code for if process kills db (db references) while other threads still running
    //  extra reference for erlang, extra user count for db ... have code here stop on condition
    //  delete condition here
    pthread_mutex_destroy(&db_ptr->m_CloseMutex);
    pthread_cond_destroy(&db_ptr->m_CloseCond);

    return;

}   // DbObject::DbObjectResourceCleanup


DbObject::DbObject(
    leveldb::DB * DbPtr,
    leveldb::Options * Options)
    : m_Db(DbPtr), m_DbOptions(Options),
      m_CloseRequested(0), m_ActiveCount(0)
{
    pthread_mutexattr_t attr;

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&m_CloseMutex, &attr);
    pthread_cond_init(&m_CloseCond, NULL);
    pthread_mutexattr_destroy(&attr);

}   // DbObject::DbObject


// iterators should already be cleared since they hold a reference
DbObject::~DbObject()
{
    // close the db
    delete m_Db;
    m_Db=NULL;

    if (NULL!=m_DbOptions)
    {
        // Release any cache we explicitly allocated when setting up options
        delete m_DbOptions->block_cache;
        m_DbOptions->block_cache = NULL;

        // Clean up any filter policies
        delete m_DbOptions->filter_policy;
        m_DbOptions->filter_policy = NULL;

        delete m_DbOptions;
        m_DbOptions = NULL;
    }   // if

    pthread_mutex_lock(&m_CloseMutex);
    m_CloseRequested=3;
    pthread_cond_broadcast(&m_CloseCond);
    pthread_mutex_unlock(&m_CloseMutex);

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // DbObject::~DbObject


void
DbObject::RefInc(bool ErlRefToo)
{
//    if (ErlRefToo)
//        enif_keep_resource(this);
    eleveldb::add_and_fetch(&m_ActiveCount, 1);

}   // DbObject::RefInc


void
DbObject::RefDec(bool ErlRefToo)
{
    uint32_t cur_count;

    cur_count=eleveldb::sub_and_fetch(&m_ActiveCount, 1);

    // this the last active after close requested?
    //  (atomic swap should be unnecessary ... but going for safety)
    if (0==cur_count && compare_and_swap(&m_CloseRequested, 1, 2))
    {
        // deconstruct, but let erlang deallocate memory later
        this->~DbObject();
    }   // if

    // do this second so no race on destructor versus GC
//    if (ErlRefToo)
//        enif_release_resource(this);

    return;

}   // DbObject::RefDec



/**
 * Iterator management object
 */

ErlNifResourceType * ItrObject::m_Itr_RESOURCE(NULL);


void
ItrObject::CreateItrObjectType(
    ErlNifEnv * Env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    m_Itr_RESOURCE = enif_open_resource_type(Env, NULL, "eleveldb_ItrObject",
                                             &ItrObject::ItrObjectResourceCleanup,
                                             flags, NULL);

    return;

}   // ItrObject::CreateItrObjectType


ItrObject *
ItrObject::CreateItrObject(
    DbObject * DbPtr,
    bool KeysOnly)
{
    ItrObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_Itr_RESOURCE, sizeof(ItrObject));

    ret_ptr=new (alloc_ptr) ItrObject(DbPtr, KeysOnly);

    // manual reference increase to keep active until "close" called
    //  only inc local counter, leave erl ref count alone ... will force
    //  erlang to call us if process holding ref dies
    ret_ptr->RefInc(false);

    // see IterTask::operator() for release of reference count

    return(ret_ptr);

}   // ItrObject::CreateItrObject


ItrObject *
ItrObject::RetrieveItrObject(
    ErlNifEnv * Env,
    const ERL_NIF_TERM & ItrTerm, bool ItrClosing)
{
    ItrObject * ret_ptr;

    ret_ptr=NULL;

    if (enif_get_resource(Env, ItrTerm, m_Itr_RESOURCE, (void **)&ret_ptr))
    {
        // increment erlang normal GC count, then our "fast" count
        ret_ptr->RefInc();

        // has close been requested?
        if (ret_ptr->m_CloseRequested
            || (!ItrClosing && ret_ptr->m_DbPtr->m_CloseRequested))
        {
            // object already closing
            ret_ptr->RefDec();
            ret_ptr=NULL;
        }   // else
    }   // if

    return(ret_ptr);

}   // ItrObject::RetrieveItrObject


void
ItrObject::ItrObjectResourceCleanup(
    ErlNifEnv * Env,
    void * Arg)
{
    ItrObject * itr_ptr;

    itr_ptr=(ItrObject *)Arg;

    leveldb::gPerfCounters->Inc(leveldb::ePerfDebug2);

    if (compare_and_swap(&itr_ptr->m_CloseRequested, 0, 1))
    {
        leveldb::gPerfCounters->Inc(leveldb::ePerfDebug3);


        // if there is an active move object, set it up to delete
        //  (reuse_move holds a counter to this object, which will
        //   release when move object destructs)
        itr_ptr->ReleaseReuseMove();

        // clear C++ only reference
        itr_ptr->RefDec(false);

    }   // if

    // quick test if any work pending
    if (3!=itr_ptr->m_CloseRequested)
    {
        pthread_mutex_lock(&itr_ptr->m_CloseMutex);

        // retest after mutex helc
        if (3!=itr_ptr->m_CloseRequested)
        {
            pthread_cond_wait(&itr_ptr->m_CloseCond, &itr_ptr->m_CloseMutex);
        }   // if
        pthread_mutex_unlock(&itr_ptr->m_CloseMutex);
    }   // if

    pthread_mutex_destroy(&itr_ptr->m_CloseMutex);
    pthread_cond_destroy(&itr_ptr->m_CloseCond);

    leveldb::gPerfCounters->Inc(leveldb::ePerfDebug4);

    return;

}   // ItrObject::ItrObjectResourceCleanup


ItrObject::ItrObject(
    DbObject * DbPtr,
    bool KeysOnly)
    : itr(NULL), snapshot(NULL), keys_only(KeysOnly),
      m_handoff_atomic(0), itr_ref_env(NULL), reuse_move(NULL),
      m_DbPtr(DbPtr), m_CloseRequested(0), m_ActiveCount(0)
{
    pthread_mutexattr_t attr;

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&m_CloseMutex, &attr);
    pthread_cond_init(&m_CloseCond, NULL);
    pthread_mutexattr_destroy(&attr);

}   // ItrObject::ItrObject


ItrObject::~ItrObject()
{
    delete itr;
    itr=NULL;

    if (NULL!=snapshot)
        m_DbPtr->m_Db->ReleaseSnapshot(snapshot);

    if (NULL!=itr_ref_env)
        enif_free_env(itr_ref_env);

    // not likely to have active reuse item since it would
    //  block destruction
    ReleaseReuseMove();

    pthread_mutex_lock(&m_CloseMutex);
    m_CloseRequested=3;
    pthread_cond_broadcast(&m_CloseCond);
    pthread_mutex_unlock(&m_CloseMutex);

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // ItrObject::~ItrObject


void
ItrObject::RefInc(bool ErlRefToo)
{
//    if (ErlRefToo)
//        enif_keep_resource(this);
    eleveldb::add_and_fetch(&m_ActiveCount, 1);

}   // ItrObject::RefInc


void
ItrObject::RefDec(bool ErlRefToo)
{
    uint32_t cur_count;

    cur_count=eleveldb::sub_and_fetch(&m_ActiveCount, 1);

    // this the last active after close requested?
    //  (atomic swap should be unnecessary ... but going for safety)
    if (0==cur_count && compare_and_swap(&m_CloseRequested, 1, 2))
    {
        // deconstruct, but let erlang deallocate memory later
        this->~ItrObject();
    }   // if

    // do this second so no race on destructor versus GC
//    if (ErlRefToo)
//        enif_release_resource(this);

    return;

}   // ItrObject::RefDec


void
ItrObject::ReleaseReuseMove()
{
    MoveTask * ptr;

    // move pointer off ItrObject first, then decrement ...
    //  otherwise there is potential for infinite loop
    ptr=(MoveTask *)reuse_move;
    if (compare_and_swap(&reuse_move, ptr, (MoveTask *)NULL)
        && NULL!=ptr)
    {
        ptr->RefDec();
    }   // if

    return;

}   // ItrObject::ReleaseReuseMove()


/**
 * WorkTask functions
 */


WorkTask::WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref)
    : ref_count(0), terms_set(false), resubmit_work(false), m_DeleteCount(0)
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
    : m_DbPtr(DbPtr), ref_count(0), terms_set(false), resubmit_work(false), m_DeleteCount(0)
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

    ++m_DeleteCount;
    env_ptr=local_env_;
    if (compare_and_swap(&local_env_, env_ptr, (ErlNifEnv *)NULL)
        && NULL!=env_ptr)
    {
        enif_free_env(env_ptr);
    }   // if

    return;

}   // WorkTask::~WorkTask


void
WorkTask::prepare_recycle()
{
    // does not work by default
    resubmit_work=false;
}  // WorkTask::prepare_recycle


void
WorkTask::recycle()
{
    // does not work by default
}   // WorkTask::recycle


uint32_t
WorkTask::RefInc() {return(eleveldb::add_and_fetch(&ref_count, 1));};


void
WorkTask::RefDec()
{
    volatile uint32_t current_refs;

    current_refs=eleveldb::sub_and_fetch(&ref_count, 1);
    if (0==current_refs)
        delete this;

}   // WorkTask::RefDec



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
OpenTask::operator()()
{
    DbObject * db_ptr;
    leveldb::DB *db(0);

    leveldb::Status status = leveldb::DB::Open(*open_options, db_name, &db);

    if(!status.ok())
        return error_tuple(local_env(), ATOM_ERROR_DB_OPEN, status);

    db_ptr=DbObject::CreateDbObject(db, open_options);

    // create a resource reference to send erlang
    ERL_NIF_TERM result = enif_make_resource(local_env(), db_ptr);

    // clear the automatic reference from enif_alloc_resource in CreateDbObject
    enif_release_resource(db_ptr);

    return work_result(local_env(), ATOM_OK, result);

}   // OpenTask::operator()



/**
 * MoveTask functions
 */

work_result
MoveTask::operator()()
{

    leveldb::Iterator* itr = m_ItrPtr->itr;

    if(NULL == itr)
        return work_result(local_env(), ATOM_ERROR, ATOM_ITERATOR_CLOSED);


    switch(action)
    {
        case FIRST:
            itr->SeekToFirst();
            break;

        case LAST:
            itr->SeekToLast();
            break;

        case NEXT:
            if(!itr->Valid())
                return work_result(local_env(), ATOM_ERROR, ATOM_INVALID_ITERATOR);

            itr->Next();
            break;

        case PREV:
            if(!itr->Valid())
                return work_result(local_env(), ATOM_ERROR, ATOM_INVALID_ITERATOR);

            itr->Prev();
            break;

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

    if(!itr->Valid())
        return work_result(local_env(), ATOM_ERROR, ATOM_INVALID_ITERATOR);

    // who got back first, us or the erlang loop
//    if (eleveldb::detail::compare_and_swap(&m_ItrPtr->m_handoff_atomic, 0, 1))
    if (compare_and_swap(&m_ItrPtr->m_handoff_atomic, 0, 1))
    {
        // this is prefetch of next iteration.  It returned faster than actual
        //  request to retrieve it.  Stop and wait for erlang to catch up.
        leveldb::gPerfCounters->Inc(leveldb::ePerfDebug0);
    }   // if
    else
    {
        // setup next race for the response
        m_ItrPtr->m_handoff_atomic=0;

        if (NEXT==action || SEEK==action || FIRST==action)
        {
            prepare_recycle();
            action=NEXT;
        }   // if

        // erlang is waiting, send message
        if(m_ItrPtr->keys_only)
            return work_result(local_env(), ATOM_OK, slice_to_binary(local_env(), itr->key()));

        return work_result(local_env(), ATOM_OK,
                           slice_to_binary(local_env(), itr->key()),
                           slice_to_binary(local_env(), itr->value()));
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
        caller_ref_term = enif_make_copy(local_env_, m_ItrPtr->itr_ref);
        caller_pid_term = enif_make_pid(local_env_, &local_pid);
        terms_set=true;
    }   // if

    return(local_env_);

}   // MoveTask::local_env


void
MoveTask::prepare_recycle()
{

    resubmit_work=true;

 }  // MoveTask::prepare_recycle


void
MoveTask::recycle()
{
    // test for race condition of simultaneous delete & recycle
    if (1<RefInc())
    {
        if (NULL!=local_env_)
            enif_clear_env(local_env_);

        terms_set=false;
        resubmit_work=false;

        // only do this in non-race condition
        RefDec();
    }   // if
    else
    {
        // touch NOTHING
    }   // else

}   // MoveTask::recycle



} // namespace eleveldb


