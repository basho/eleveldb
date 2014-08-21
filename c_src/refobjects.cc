// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2014 Basho Technologies, Inc. All Rights Reserved.
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
    : m_ErlangThisPtr(NULL), m_CloseRequested(0)
{
    pthread_mutexattr_t attr;

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&m_CloseMutex, &attr);
    pthread_cond_init(&m_CloseCond, NULL);
    pthread_mutexattr_destroy(&attr);

}   // ErlRefObject::ErlRefObject


ErlRefObject::~ErlRefObject()
{
    pthread_cond_destroy(&m_CloseCond);
    pthread_mutex_destroy(&m_CloseMutex);


}   // ErlRefObject::~ErlRefObject


bool
ErlRefObject::ClaimCloseFromCThread()
{
    bool ret_flag;
    void * volatile * erlang_ptr;

    ret_flag=false;

    // first C thread claims contents of m_ErlangThisPtr and sets it to NULL
    //  This reduces number of times C code might look into Erlang heap memory
    //  that has garbage collected
    erlang_ptr=m_ErlangThisPtr;
    if (compare_and_swap((void**)&m_ErlangThisPtr, (void *)erlang_ptr, (void *)NULL)
        && NULL!=erlang_ptr)
    {
        // now test if this C thread preceded Erlang in claiming the close operation
        ret_flag=compare_and_swap((void **)erlang_ptr, (void *)this,(void *) NULL);
    }   // if

    return(ret_flag);

}   // ErlRefObject::ClaimCloseFromCThread


void
ErlRefObject::InitiateCloseRequest()
{
    m_CloseRequested=1;

    Shutdown();

    // WAIT for shutdown to complete
    pthread_mutex_lock(&m_CloseMutex);

    // one ref from construction, one ref from broadcast in RefDec below
    //  (only wait if RefDec has not signaled)
    if (1<m_RefCount && 1==m_CloseRequested)
    {
        pthread_cond_wait(&m_CloseCond, &m_CloseMutex);
    }   // while
    pthread_mutex_unlock(&m_CloseMutex);

    m_CloseRequested=3;
    RefDec();

    return;

}   // ErlRefObject::InitiateCloseRequest


uint32_t
ErlRefObject::RefDec()
{
    uint32_t cur_count;

    pthread_mutex_lock(&m_CloseMutex);
    cur_count=eleveldb::dec_and_fetch(&m_RefCount);

    if (cur_count<2 && 1==m_CloseRequested)
    {
        bool flag;

        // state 2 is sign that all secondary references have cleared
        m_CloseRequested=2;

        // is there really more than one ref count now?
        flag=(0<m_RefCount);
        if (flag)
        {
            RefObject::RefInc();
            pthread_cond_broadcast(&m_CloseCond);
        }   // if

        // this "flag" and ref count dance is to ensure
        //  that the mutex unlock is called on all threads
        //  before destruction.
        if (flag)
            RefObject::RefDec();
        else
            cur_count=0;
    }   // if
    pthread_mutex_unlock(&m_CloseMutex);

    if (0==cur_count)
    {
        assert(0!=m_CloseRequested);
        delete this;
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


void *
DbObject::CreateDbObject(
    leveldb::DB * Db,
    leveldb::Options * DbOptions)
{
    DbObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_Db_RESOURCE, sizeof(DbObject *));

    ret_ptr=new DbObject(Db, DbOptions);
    *(DbObject **)alloc_ptr=ret_ptr;

    // manual reference increase to keep active until "eleveldb_close" called
    ret_ptr->RefInc();
    ret_ptr->m_ErlangThisPtr=(void * volatile *)alloc_ptr;

    return(alloc_ptr);

}   // DbObject::CreateDbObject


DbObject *
DbObject::RetrieveDbObject(
    ErlNifEnv * Env,
    const ERL_NIF_TERM & DbTerm,
    bool * term_ok)
{
    DbObject ** db_ptr_ptr, * ret_ptr;

    ret_ptr=NULL;
    if (NULL!=term_ok)
    {
        *term_ok=false;
    }

    if (NULL!=term_ok)
        *term_ok=false;

    if (enif_get_resource(Env, DbTerm, m_Db_RESOURCE, (void **)&db_ptr_ptr))
    {
        if (NULL!=term_ok)
            *term_ok=true;

        ret_ptr=*db_ptr_ptr;

        if (NULL!=ret_ptr)
        {
            // has close been requested?
            if (0!=ret_ptr->m_CloseRequested)
            {
                // object already closing
                ret_ptr=NULL;
            }   // if
        }   // if
    }   // if

    return(ret_ptr);

}   // DbObject::RetrieveDbObject


void
DbObject::DbObjectResourceCleanup(
    ErlNifEnv * Env,
    void * Arg)
{
    DbObject * volatile * erl_ptr;
    DbObject * db_ptr;

    erl_ptr=(DbObject * volatile *)Arg;
    db_ptr=*erl_ptr;

    // is Erlang first to initiate close?
    if (compare_and_swap(erl_ptr, db_ptr, (DbObject *)NULL)
        && NULL!=db_ptr)
    {
        db_ptr->InitiateCloseRequest();
    }   // if

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

    return;

}   // DbObject::~DbObject


void
DbObject::Shutdown()
{
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
        {
            // follow protocol, only one thread calls Initiate
//            if (compare_and_swap(itr_ptr->m_ErlangThisPtr, itr_ptr, (ItrObject *)NULL))
            if (itr_ptr->ClaimCloseFromCThread())
                itr_ptr->ItrObject::InitiateCloseRequest();
        }   // if
    } while(again);

    return;

}   // DbObject::Shutdown


bool
DbObject::AddReference(
    ItrObject * ItrPtr)
{
    bool ret_flag;
    MutexLock lock(m_ItrMutex);

    ret_flag=(0==m_CloseRequested);

    if (ret_flag)
        m_ItrList.push_back(ItrPtr);

    return(ret_flag);

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
 * Regenerative iterator object (malloc memory)
 */

LevelIteratorWrapper::LevelIteratorWrapper(
    ItrObject * ItrPtr,
    bool KeysOnly,
    leveldb::ReadOptions & Options,
    ERL_NIF_TERM itr_ref)
    : m_DbPtr(ItrPtr->m_DbPtr.get()), m_ItrPtr(ItrPtr), m_Snapshot(NULL), m_Iterator(NULL),
      m_HandoffAtomic(0), m_KeysOnly(KeysOnly), m_PrefetchStarted(false),
      m_Options(Options), itr_ref(itr_ref),
      m_IteratorStale(0), m_StillUse(true)
{
    RebuildIterator();
};



/**
 * Iterator management object (Erlang memory)
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


void *
ItrObject::CreateItrObject(
    DbObject * DbPtr,
    bool KeysOnly,
    leveldb::ReadOptions & Options)
{
    ItrObject * ret_ptr;
    void * alloc_ptr;

    // the alloc call initializes the reference count to "one"
    alloc_ptr=enif_alloc_resource(m_Itr_RESOURCE, sizeof(ItrObject *));

    ret_ptr=new ItrObject(DbPtr, KeysOnly, Options);
    *(ItrObject **)alloc_ptr=ret_ptr;

    // manual reference increase to keep active until "eleveldb_iterator_close" called
    ret_ptr->RefInc();
    ret_ptr->m_ErlangThisPtr=(void * volatile *)alloc_ptr;

    return(alloc_ptr);

}   // ItrObject::CreateItrObject


ItrObject *
ItrObject::RetrieveItrObject(
    ErlNifEnv * Env,
    const ERL_NIF_TERM & ItrTerm, bool ItrClosing)
{
    ItrObject ** itr_ptr_ptr, * ret_ptr;

    ret_ptr=NULL;

    if (enif_get_resource(Env, ItrTerm, m_Itr_RESOURCE, (void **)&itr_ptr_ptr))
    {
        ret_ptr=*itr_ptr_ptr;

        if (NULL!=ret_ptr)
        {
            // has close been requested?
            if (ret_ptr->m_CloseRequested
                || (!ItrClosing && ret_ptr->m_DbPtr->m_CloseRequested))
            {
                // object already closing
                ret_ptr=NULL;
            }   // if
        }   // if
    }   // if

    return(ret_ptr);

}   // ItrObject::RetrieveItrObject


void
ItrObject::ItrObjectResourceCleanup(
    ErlNifEnv * Env,
    void * Arg)
{
    ItrObject * volatile * erl_ptr;
    ItrObject * itr_ptr;

    erl_ptr=(ItrObject * volatile *)Arg;
    itr_ptr=*erl_ptr;

    // is Erlang first to initiate close?
    if (compare_and_swap(erl_ptr, itr_ptr, (ItrObject *)NULL)
        && NULL!=itr_ptr)
    {
        itr_ptr->InitiateCloseRequest();
    }   // if

    return;

}   // ItrObject::ItrObjectResourceCleanup


ItrObject::ItrObject(
    DbObject * DbPtr,
    bool KeysOnly,
    leveldb::ReadOptions & Options)
    : keys_only(KeysOnly), m_ReadOptions(Options), reuse_move(NULL),
      m_DbPtr(DbPtr), itr_ref_env(NULL)
{
    if (NULL!=DbPtr)
        DbPtr->AddReference(this);

}   // ItrObject::ItrObject


ItrObject::~ItrObject()
{
    // not likely to have active reuse item since it would
    //  block destruction
    ReleaseReuseMove();

    if (NULL!=itr_ref_env)
    {
        enif_free_env(itr_ref_env);
        itr_ref_env=NULL;
    }   // if

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

    // ItrObject and m_Iter each hold pointers to other, release ours
    m_Iter.assign(NULL);

    return;

}   // ItrObject::Shutdown


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


