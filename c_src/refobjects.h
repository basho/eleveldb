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

#ifndef INCL_REFOBJECTS_H
#define INCL_REFOBJECTS_H

#include <stdint.h>
#include <list>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/perf_count.h"

#ifndef INCL_THREADING_H
    #include "threading.h"
#endif

#ifndef __WORK_RESULT_HPP
    #include "work_result.hpp"
#endif

#ifndef ATOMS_H
    #include "atoms.h"
#endif


namespace eleveldb {

/**
 * Base class for any object that offers RefInc / RefDec interface
 */

class RefObject
{
public:

protected:
    volatile uint32_t m_RefCount;     //!< simple count of reference, auto delete at zero

public:
    RefObject();

    virtual ~RefObject();

    virtual uint32_t RefInc();

    virtual uint32_t RefDec();

private:
    RefObject(const RefObject&);              // nocopy
    RefObject& operator=(const RefObject&);   // nocopyassign
};  // class RefObject


/**
 * Base class for any object that is managed as an Erlang reference
 */

class ErlRefObject : public RefObject
{
public:
    // these member objects are public to simplify
    //  access by statics and external APIs
    //  (yes, wrapper functions would be welcome)
    volatile uint32_t m_CloseRequested;  // 1 once api close called, 2 once thread starts destructor, 3 destructor done

    // DO NOT USE CONTAINER OBJECTS
    //  ... these must be live after destructor called
    pthread_mutex_t m_CloseMutex;        //!< for erlang forced close
    pthread_cond_t  m_CloseCond;         //!< for erlang forced close

protected:


public:
    ErlRefObject();

    virtual ~ErlRefObject();

    virtual uint32_t RefDec();

    // allows for secondary close actions IF InitiateCloseRequest returns true
    virtual void Shutdown()=0;

    // the following will sometimes be called AFTER the
    //  destructor ... in which case the vtable is not valid
    static bool InitiateCloseRequest(ErlRefObject * Object);

    static void AwaitCloseAndDestructor(ErlRefObject * Object);


private:
    ErlRefObject(const ErlRefObject&);              // nocopy
    ErlRefObject& operator=(const ErlRefObject&);   // nocopyassign
};  // class RefObject


/**
 * Class to manage access and counting of references
 * to a reference object.
 */

template <class TargetT>
class ReferencePtr
{
    TargetT * t;

public:
    ReferencePtr()
        : t(NULL)
    {};

    ReferencePtr(TargetT *_t)
        : t(_t)
    {
        if (NULL!=t)
            t->RefInc();
    }

    ReferencePtr(const ReferencePtr &rhs)
    {t=rhs.t; if (NULL!=t) t->RefInc();};

    ~ReferencePtr()
    {
        if (NULL!=t)
            t->RefDec();
    }

    void assign(TargetT * _t)
    {
        if (_t!=t)
        {
            if (NULL!=t)
                t->RefDec();
            t=_t;
            if (NULL!=t)
                t->RefInc();
        }   // if
    };

    TargetT * get() {return(t);};

    TargetT * operator->() {return(t);};

private:
 ReferencePtr & operator=(const ReferencePtr & rhs); // no assignment

};  // ReferencePtr


/**
 * Per database object.  Created as erlang reference.
 *
 * Extra reference count created upon initialization, released on close.
 */
class DbObject : public ErlRefObject
{
public:
    leveldb::DB* m_Db;                                   // NULL or leveldb database object

    leveldb::Options * m_DbOptions;

    Mutex m_ItrMutex;                         //!< mutex protecting m_ItrList
    std::list<class ItrObject *> m_ItrList;   //!< ItrObjects holding ref count to this

protected:
    static ErlNifResourceType* m_Db_RESOURCE;

public:
    DbObject(leveldb::DB * DbPtr, leveldb::Options * Options);

    virtual ~DbObject();

    virtual void Shutdown();

    // manual back link to ItrObjects holding reference to this
    void AddReference(class ItrObject *);

    void RemoveReference(class ItrObject *);

    static void CreateDbObjectType(ErlNifEnv * Env);

    static DbObject * CreateDbObject(leveldb::DB * Db, leveldb::Options * DbOptions);

    static DbObject * RetrieveDbObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void DbObjectResourceCleanup(ErlNifEnv *Env, void * Arg);

private:
    DbObject();
    DbObject(const DbObject&);              // nocopy
    DbObject& operator=(const DbObject&);   // nocopyassign
};  // class DbObject


/**
 * A self deleting wrapper to contain leveldb snapshot pointer.
 *   Needed because multiple LevelIteratorWrappers could be using
 *   it ... and finishing at different times.
 */

class LevelSnapshotWrapper : public RefObject
{
public:
    ReferencePtr<DbObject> m_DbPtr;  //!< need to keep db open for delete of this object
    const leveldb::Snapshot * m_Snapshot;

    // this is an odd place to put this info, but it
    //  happens to have the exact same lifespan
    ERL_NIF_TERM itr_ref;
    ErlNifEnv *itr_ref_env;

    LevelSnapshotWrapper(DbObject * DbPtr, const leveldb::Snapshot * Snapshot)
        : m_DbPtr(DbPtr), m_Snapshot(Snapshot), itr_ref_env(NULL)
    {
    };

    virtual ~LevelSnapshotWrapper()
    {
        if (NULL!=itr_ref_env)
            enif_free_env(itr_ref_env);

        if (NULL!=m_Snapshot)
        {
            // leveldb performs actual "delete" call on m_Shapshot's pointer
            m_DbPtr->m_Db->ReleaseSnapshot(m_Snapshot);
            m_Snapshot=NULL;
        }   // if
    }   // ~LevelSnapshotWrapper

    const leveldb::Snapshot * get() {return(m_Snapshot);};
    const leveldb::Snapshot * operator->() {return(m_Snapshot);};

private:
    LevelSnapshotWrapper(const LevelSnapshotWrapper &);            // no copy
    LevelSnapshotWrapper& operator=(const LevelSnapshotWrapper &); // no assignment

};  // LevelSnapshotWrapper



/**
 * A self deleting wrapper to contain leveldb iterator.
 *   Used when an ItrObject needs to skip around and might
 *   have a background MoveItem performing a prefetch on existing
 *   iterator.
 */

class LevelIteratorWrapper : public RefObject
{
public:
    ReferencePtr<DbObject> m_DbPtr;           //!< need to keep db open for delete of this object
    ReferencePtr<LevelSnapshotWrapper> m_Snap;//!< keep snapshot active while this object is
    leveldb::Iterator * m_Iterator;
    volatile uint32_t m_HandoffAtomic;        //!< matthew's atomic foreground/background prefetch flag.
    bool m_KeysOnly;                          //!< only return key values
    bool m_PrefetchStarted;                   //!< true after first prefetch command

    LevelIteratorWrapper(DbObject * DbPtr, LevelSnapshotWrapper * Snapshot,
                         leveldb::Iterator * Iterator, bool KeysOnly)
        : m_DbPtr(DbPtr), m_Snap(Snapshot), m_Iterator(Iterator),
        m_HandoffAtomic(0), m_KeysOnly(KeysOnly), m_PrefetchStarted(false)
    {
    };

    virtual ~LevelIteratorWrapper()
    {
        if (NULL!=m_Iterator)
        {
            delete m_Iterator;
            m_Iterator=NULL;
        }   // if
    }   // ~LevelIteratorWrapper

    leveldb::Iterator * get() {return(m_Iterator);};
    leveldb::Iterator * operator->() {return(m_Iterator);};

    bool Valid() {return(m_Iterator->Valid());};
    leveldb::Slice key() {return(m_Iterator->key());};
    leveldb::Slice value() {return(m_Iterator->value());};

private:
    LevelIteratorWrapper(const LevelIteratorWrapper &);            // no copy
    LevelIteratorWrapper& operator=(const LevelIteratorWrapper &); // no assignment


};  // LevelIteratorWrapper



/**
 * Per Iterator object.  Created as erlang reference.
 */
class ItrObject : public ErlRefObject
{
public:
    ReferencePtr<LevelIteratorWrapper> m_Iter;
    ReferencePtr<LevelSnapshotWrapper> m_Snapshot;

    bool keys_only;
    leveldb::ReadOptions * m_ReadOptions;

    volatile class MoveTask * reuse_move;//!< iterator work object that is reused instead of lots malloc/free

    ReferencePtr<DbObject> m_DbPtr;

protected:
    static ErlNifResourceType* m_Itr_RESOURCE;

public:
    ItrObject(DbObject *, bool, leveldb::ReadOptions *);

    virtual ~ItrObject(); // needs to perform free_itr

    virtual void Shutdown();

    static void CreateItrObjectType(ErlNifEnv * Env);

    static ItrObject * CreateItrObject(DbObject * Db, bool KeysOnly, leveldb::ReadOptions * Options);

    static ItrObject * RetrieveItrObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm,
                                         bool ItrClosing=false);

    static void ItrObjectResourceCleanup(ErlNifEnv *Env, void * Arg);

    bool ReleaseReuseMove();

private:
    ItrObject();
    ItrObject(const ItrObject &);            // no copy
    ItrObject & operator=(const ItrObject &); // no assignment
};  // class ItrObject

} // namespace eleveldb


#endif  // INCL_REFOBJECTS_H
