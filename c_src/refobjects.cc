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

#ifndef INCL_REFOBJECTS_H
    #include "refobjects.h"
#endif

#ifndef INCL_WORKITEMS_H
    #include "workitems.h"
#endif

#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/perf_count.h"


namespace eleveldb {

/**
 * RefObject Functions
 */

RefObject::RefObject()
    : m_RefCount(0)
{
}   // RefObject::RefObject


RefObject::~RefObject()
{
}   // RefObject::~RefObject


uint32_t
RefObject::RefInc()
{

    return(eleveldb::inc_and_fetch(&m_RefCount));

}   // RefObject::RefInc


uint32_t
RefObject::RefDec()
{
    uint32_t current_refs;

    current_refs=eleveldb::dec_and_fetch(&m_RefCount);
    if (0==current_refs)
        delete this;

    return(current_refs);

}   // RefObject::RefDec


/**
 * Erlang reference object
 */

ErlRefObject::ErlRefObject()
    : m_CloseRequested(0)
{
    pthread_mutexattr_t attr;

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&m_CloseMutex, &attr);
    pthread_cond_init(&m_CloseCond, NULL);
    pthread_mutexattr_destroy(&attr);

    return;

}   // ErlRefObject::ErlRefObject


ErlRefObject::~ErlRefObject()
{

    // DO NOT DESTROY m_CloseMutex or m_CloseCond here

}   // ErlRefObject::~ErlRefObject


uint32_t
ErlRefObject::RefDec()
{
    uint32_t cur_count;

    cur_count=eleveldb::dec_and_fetch(&m_RefCount);

    // this the last active after close requested?
    //  (atomic swap should be unnecessary ... but going for safety)
    if (0==cur_count && compare_and_swap(&m_CloseRequested, 1, 2))
    {
        // deconstruct, but let erlang deallocate memory later
        this->~ErlRefObject();
    }   // if

    return(cur_count);

}   // DbObject::RefDec



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
    ret_ptr->RefInc();

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
        db_ptr->RefDec();
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
    : m_Db(DbPtr), m_DbOptions(Options)
{
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
    //  only inc local counter
    ret_ptr->RefInc();

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

        itr_ptr->RefDec();

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
      m_DbPtr(DbPtr)
{
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


} // namespace eleveldb


