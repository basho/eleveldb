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


namespace eleveldb {

/**
 * RefObject Functions
 */

RefObject::RefObject()
    : m_RefCount(0)
{
        leveldb::gPerfCounters->Inc(leveldb::ePerfElevelRefCreate);
}   // RefObject::RefObject


RefObject::~RefObject()
{
    leveldb::gPerfCounters->Inc(leveldb::ePerfElevelRefDelete);
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

    pthread_mutex_lock(&m_CloseMutex);
    m_CloseRequested=3;
    pthread_cond_broadcast(&m_CloseCond);
    pthread_mutex_unlock(&m_CloseMutex);

    // DO NOT DESTROY m_CloseMutex or m_CloseCond here

}   // ErlRefObject::~ErlRefObject


bool
ErlRefObject::InitiateCloseRequest(
    ErlRefObject * Object)
{
    bool ret_flag;

    ret_flag=false;

    // special handling since destructor may have been called
    if (NULL!=Object && 0==Object->m_CloseRequested)
        ret_flag=compare_and_swap(&Object->m_CloseRequested, 0, 1);

    // vtable is still good, this thread is initiating close
    //   ask object to clean-up
    if (ret_flag)
    {
        Object->Shutdown();
    }   // if

    return(ret_flag);

}   // ErlRefObject::InitiateCloseRequest


void
ErlRefObject::AwaitCloseAndDestructor(
    ErlRefObject * Object)
{
    // NOTE:  it is possible, actually likely, that this
    //        routine is called AFTER the destructor is called
    //        Don't panic.

    if (NULL!=Object)
    {
        // quick test if any work pending
        if (3!=Object->m_CloseRequested)
        {
            pthread_mutex_lock(&Object->m_CloseMutex);

            // retest after mutex helc
            while (3!=Object->m_CloseRequested)
            {
                pthread_cond_wait(&Object->m_CloseCond, &Object->m_CloseMutex);
            }   // while
            pthread_mutex_unlock(&Object->m_CloseMutex);
        }   // if

        pthread_mutex_destroy(&Object->m_CloseMutex);
        pthread_cond_destroy(&Object->m_CloseCond);
    }   // if

    return;

}   // ErlRefObject::AwaitCloseAndDestructor


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
        // has close been requested?
        if (ret_ptr->m_CloseRequested)
        {
            // object already closing
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

    // YES, the destructor may already have been called
    InitiateCloseRequest(db_ptr);

    // YES, the destructor may already have been called
    AwaitCloseAndDestructor(db_ptr);

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





    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // DbObject::~DbObject


void
DbObject::Shutdown()
{
#if 1
    bool again;
    ItrObject * itr_ptr;

    do
    {
        again=false;
        itr_ptr=NULL;

        // lock the ItrList
        {
            MutexLock lock(m_ItrMutex);

            if (!m_ItrList.empty())
            {
                again=true;
                itr_ptr=m_ItrList.front();
                m_ItrList.pop_front();
            }   // if
        }

        // must be outside lock so ItrObject can attempt
        //  RemoveReference
        if (again)
            ItrObject::InitiateCloseRequest(itr_ptr);

    } while(again);
#endif

    RefDec();

    return;

}   // DbObject::Shutdown


void
DbObject::AddReference(
    ItrObject * ItrPtr)
{
    MutexLock lock(m_ItrMutex);

    m_ItrList.push_back(ItrPtr);

    return;

}   // DbObject::AddReference


void
DbObject::RemoveReference(
    ItrObject * ItrPtr)
{
    MutexLock lock(m_ItrMutex);

    m_ItrList.remove(ItrPtr);

    return;

}   // DbObject::RemoveReference


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
    bool KeysOnly,
    leveldb::ReadOptions * Options)
{
    ItrObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_Itr_RESOURCE, sizeof(ItrObject));

    ret_ptr=new (alloc_ptr) ItrObject(DbPtr, KeysOnly, Options);

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
        // has close been requested?
        if (ret_ptr->m_CloseRequested
            || (!ItrClosing && ret_ptr->m_DbPtr->m_CloseRequested))
        {
            // object already closing
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

    // vtable for itr_ptr could be invalid if close already
    //  occurred
    InitiateCloseRequest(itr_ptr);

    // YES this can be called after itr_ptr destructor.  Don't panic.
    AwaitCloseAndDestructor(itr_ptr);

    return;

}   // ItrObject::ItrObjectResourceCleanup


ItrObject::ItrObject(
    DbObject * DbPtr,
    bool KeysOnly,
    leveldb::ReadOptions * Options)
    : keys_only(KeysOnly), m_ReadOptions(Options), reuse_move(NULL), m_DbPtr(DbPtr)
{
    if (NULL!=DbPtr)
        DbPtr->AddReference(this);

}   // ItrObject::ItrObject


ItrObject::~ItrObject()
{
    // not likely to have active reuse item since it would
    //  block destruction
    ReleaseReuseMove();

    delete m_ReadOptions;

    if (NULL!=m_DbPtr.get())
        m_DbPtr->RemoveReference(this);

    // do not clean up m_CloseMutex and m_CloseCond

    return;

}   // ItrObject::~ItrObject


void
ItrObject::Shutdown()
{
    // if there is an active move object, set it up to delete
    //  (reuse_move holds a counter to this object, which will
    //   release when move object destructs)
    ReleaseReuseMove();

    RefDec();

    return;

}   // ItrObject::CloseRequest


bool
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

    return(NULL!=ptr);

}   // ItrObject::ReleaseReuseMove()


} // namespace eleveldb


