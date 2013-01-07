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

#ifndef INCL_WORKITEMS_H
#define INCL_WORKITEMS_H

#include <stdint.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"


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

/* Type returned from a work task: */
typedef basho::async_nif::work_result   work_result;

/**
 * Automatic lock and release of erlang reference
 */
template <class TargetT>
class ReferenceLock
{
    TargetT *t;    // assumes this won't vanish!

public:
    ReferenceLock(TargetT *_t)
    : t(_t)
    {
        if(NULL != t)
            enif_keep_resource(t);
    }

    ~ReferenceLock()
    {
        if (NULL!=t)
            enif_release_resource(t);
    }

private:
 ReferenceLock();                         // no default construction
 ReferenceLock(const ReferenceLock &rhs); // no copy
 ReferenceLock & operator=(const ReferenceLock & rhs); // no assignment

};  // ReferenceLock


/**
 * Class to manage access and counting of references
 * to an erlang based reference object.  Reference counts
 * in the case can mean erlang and c++ counts simultaneously.
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

    void assign(TargetT * _t) {t=_t;}; //  RefInc called by RetrieveXXObject

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
class DbObject
{
public:
    leveldb::DB* m_Db;                                   // NULL or leveldb database object

    leveldb::Options * m_DbOptions;

    volatile uint32_t m_CloseRequested;                      // 1 once api close called, 2 once thread starts destructor
    volatile uint32_t m_ActiveCount;                     // number of active api calls this moment

    // DO NOT USE CONTAINER OBJECTS
    //  ... these must be live after destructor called
    pthread_mutex_t m_CloseMutex;        //!< for erlang forced close
    pthread_cond_t  m_CloseCond;         //!< for erlang forced close

protected:
    static ErlNifResourceType* m_Db_RESOURCE;

public:
    DbObject(leveldb::DB * DbPtr, leveldb::Options * Options);

    virtual ~DbObject();  // needs to perform free_db

    static void CreateDbObjectType(ErlNifEnv * Env);

    static DbObject * CreateDbObject(leveldb::DB * Db, leveldb::Options * DbOptions);

    static DbObject * RetrieveDbObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm);

    static void DbObjectResourceCleanup(ErlNifEnv *Env, void * Arg);

    void RefInc(bool ErlRefToo=true);

    void RefDec(bool ErlRefToo=true);

private:
    DbObject();
    DbObject(const DbObject&);              // nocopy
    DbObject& operator=(const DbObject&);   // nocopyassign
};  // class DbObject


/**
 * Per Iterator object.  Created as erlang reference.
 */
class ItrObject
{
public:
    leveldb::Iterator*   itr;
    const leveldb::Snapshot*   snapshot;
    bool keys_only;


    volatile uint32_t m_handoff_atomic;    //!< matthew's atomic foreground/background prefetch flag.
    ERL_NIF_TERM itr_ref;
    ErlNifEnv *itr_ref_env;
    class MoveTask * reuse_move;           //!< iterator work object that is reused instead of lots malloc/free

    ReferencePtr<DbObject> m_DbPtr;

    volatile uint32_t m_CloseRequested;                      // 1 once api close called, 2 once thread starts destructor
    volatile uint32_t m_ActiveCount;                     // number of active api calls this moment

    // DO NOT USE CONTAINER OBJECTS
    //  ... these must be live after destructor called
    pthread_mutex_t m_CloseMutex;        //!< for erlang forced close
    pthread_cond_t  m_CloseCond;         //!< for erlang forced close

protected:
    static ErlNifResourceType* m_Itr_RESOURCE;

public:
    ItrObject(DbObject *, bool);

    virtual ~ItrObject(); // needs to perform free_itr

    static void CreateItrObjectType(ErlNifEnv * Env);

    static ItrObject * CreateItrObject(DbObject * Db, bool KeysOnly);

    static ItrObject * RetrieveItrObject(ErlNifEnv * Env, const ERL_NIF_TERM & DbTerm,
                                         bool ItrClosing=false);

    static void ItrObjectResourceCleanup(ErlNifEnv *Env, void * Arg);

    void RefInc(bool ErlRefToo=true);

    void RefDec(bool ErlRefToo=true);

private:
    ItrObject();
    ItrObject(const ItrObject &);            // no copy
    ItrObject & operator=(const ItrObject &); // no assignment
};  // class ItrObject


/**
 * Virtual base class for async NIF work items:
 */
class WorkTask
{
public:

protected:
    ReferencePtr<DbObject> m_DbPtr;             //!< access to database, and holds reference
    volatile uint32_t ref_count;                //!< atomic count of users for auto delete

    ErlNifEnv      *local_env_;
    ERL_NIF_TERM   caller_ref_term;
    ERL_NIF_TERM   caller_pid_term;
    bool           terms_set;

    bool resubmit_work;           //!< true if this work item is loaded for prefetch

    volatile uint32_t m_CloseRequested;                      // 1 once api close called, 2 once thread starts destructor
    ErlNifPid local_pid;   // maintain for task lifetime (JFW)

 public:

    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref);

    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref, DbObject * DbPtr);

    virtual ~WorkTask();

    virtual void prepare_recycle();
    virtual void recycle();

    uint32_t RefInc();

    void RefDec();

    virtual ErlNifEnv *local_env()         { return local_env_; }

    // call local_env() since the virtual creates the data in MoveTask
    const ERL_NIF_TERM& caller_ref()       { local_env(); return caller_ref_term; }
    const ERL_NIF_TERM& pid()              { local_env(); return caller_pid_term; }
    bool resubmit() const {return(resubmit_work);}

    virtual work_result operator()()     = 0;

private:
 WorkTask();
 WorkTask(const WorkTask &);
 WorkTask & operator=(const WorkTask &);

};  // class WorkTask


/**
 * Background object for async open of a leveldb instance
 */

class OpenTask : public WorkTask
{
protected:
    std::string         db_name;
    leveldb::Options   *open_options;  // associated with db handle, we don't free it

public:
    OpenTask(ErlNifEnv* caller_env, ERL_NIF_TERM& _caller_ref,
             const std::string& db_name_, leveldb::Options *open_options_);

    virtual ~OpenTask() {};

    virtual work_result operator()();

private:
    OpenTask();
    OpenTask(const OpenTask &);
    OpenTask & operator=(const OpenTask &);

};  // class OpenTask



/**
 * Background object for async write
 */

class WriteTask : public WorkTask
{
protected:
    leveldb::WriteBatch*    batch;
    leveldb::WriteOptions*          options;

public:

    WriteTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                DbObject * _db_handle,
                leveldb::WriteBatch* _batch,
                leveldb::WriteOptions* _options)
        : WorkTask(_owner_env, _caller_ref, _db_handle),
       batch(_batch),
       options(_options)
    {}

    virtual ~WriteTask()
    {
        delete batch;
        delete options;
    }

    virtual work_result operator()()
    {
        leveldb::Status status = m_DbPtr->m_Db->Write(*options, batch);

        return (status.ok() ? work_result(ATOM_OK) : work_result(local_env(), ATOM_ERROR_DB_WRITE, status));
    }

};  // class WriteTask



/**
 * Background object for async get
 */

class GetTask : public WorkTask
{
protected:
    std::string                        m_Key;
    leveldb::ReadOptions*              options;

public:
    GetTask(ErlNifEnv *_caller_env,
            ERL_NIF_TERM _caller_ref,
            DbObject *_db_handle,
            ERL_NIF_TERM _key_term,
            leveldb::ReadOptions *_options)
        : WorkTask(_caller_env, _caller_ref, _db_handle),
        options(_options)
        {
            ErlNifBinary key;

            enif_inspect_binary(_caller_env, _key_term, &key);
            m_Key.assign((const char *)key.data, key.size);
        }

    virtual ~GetTask()
    {
        delete options;
    }

    virtual work_result operator()()
    {
        std::string value;
        leveldb::Slice key_slice(m_Key);

        leveldb::Status status = m_DbPtr->m_Db->Get(*options, key_slice, &value);

        if(!status.ok())
            return work_result(ATOM_NOT_FOUND);

        ERL_NIF_TERM value_bin;

        // The documentation does not say if this can fail:
        unsigned char *result = enif_make_new_binary(local_env(), value.size(), &value_bin);

        memcpy(result, value.data(), value.size());

        return work_result(local_env(), ATOM_OK, value_bin);
    }

};  // class GetTask



/**
 * Background object to open/start an iteration
 */

class IterTask : public WorkTask
{
protected:

    const bool keys_only;
    leveldb::ReadOptions *options;

public:
    IterTask(ErlNifEnv *_caller_env,
             ERL_NIF_TERM _caller_ref,
             DbObject *_db_handle,
             const bool _keys_only,
             leveldb::ReadOptions *_options)
        : WorkTask(_caller_env, _caller_ref, _db_handle),
        keys_only(_keys_only), options(_options)
    {}

    virtual ~IterTask()
    {
        delete options;
    }

    virtual work_result operator()()
    {
        ItrObject * itr_ptr;

        itr_ptr=ItrObject::CreateItrObject(m_DbPtr.get(), keys_only);

        itr_ptr->snapshot = m_DbPtr->m_Db->GetSnapshot();
        options->snapshot = itr_ptr->snapshot;

        itr_ptr->itr = m_DbPtr->m_Db->NewIterator(*options);

        // Copy caller_ref to reuse in future iterator_move calls
        itr_ptr->itr_ref_env = enif_alloc_env();
        itr_ptr->itr_ref = enif_make_copy(itr_ptr->itr_ref_env, caller_ref());

        ERL_NIF_TERM result = enif_make_resource(local_env(), itr_ptr);

        // release reference created during CreateItrObject()
        enif_release_resource(itr_ptr);

        return work_result(local_env(), ATOM_OK, result);
    }   // operator()

};  // class IterTask


class MoveTask : public WorkTask
{
public:
    typedef enum { FIRST, LAST, NEXT, PREV, SEEK } action_t;

protected:
    ReferencePtr<ItrObject> m_ItrPtr;             //!< access to database, and holds reference

public:
    action_t                                       action;
    std::string                                 seek_target;

public:
    // No seek target:
    MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
             ItrObject *_itr_handle, action_t& _action)
        : WorkTask(NULL, _caller_ref),
        m_ItrPtr(_itr_handle), action(_action)
    {
        // special case construction
        local_env_=NULL;
        enif_self(_caller_env, &local_pid);
    }

    // With seek target:
    MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
             ItrObject *_itr_handle, action_t& _action,
             std::string& _seek_target)
        : WorkTask(NULL, _caller_ref),
        m_ItrPtr(_itr_handle), action(_action),
        seek_target(_seek_target)
        {
            // special case construction
            local_env_=NULL;
            enif_self(_caller_env, &local_pid);
        }

    virtual ~MoveTask() {};

    virtual work_result operator()();

    virtual ErlNifEnv *local_env();

    virtual void prepare_recycle();
    virtual void recycle();

};  // class MoveTask

} // namespace eleveldb


#endif  // INCL_WORKITEMS_H
