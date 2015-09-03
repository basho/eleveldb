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

#ifndef INCL_WORKITEMS_H
#define INCL_WORKITEMS_H

#include <memory>
#include <string>
#include <stdint.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"


#ifndef INCL_MUTEX_H
    #include "mutex.h"
#endif

#ifndef __WORK_RESULT_HPP
    #include "work_result.hpp"
#endif

#ifndef ATOMS_H
    #include "atoms.h"
#endif

#ifndef INCL_REFOBJECTS_H
    #include "refobjects.h"
#endif


#include "filter.h"
#include "extractor.h"

namespace eleveldb {

/* Type returned from a work task: */
typedef basho::async_nif::work_result   work_result;



/**
 * Virtual base class for async NIF work items:
 */
class WorkTask : public RefObject
{
public:

protected:
    ReferencePtr<DbObject> m_DbPtr;             //!< access to database, and holds reference

    ErlNifEnv      *local_env_;
    ERL_NIF_TERM   caller_ref_term;
    ERL_NIF_TERM   caller_pid_term;
    bool           terms_set;

    bool resubmit_work;           //!< true if this work item is loaded for prefetch

    ErlNifPid local_pid;   // maintain for task lifetime (JFW)

 public:

    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref);

    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref, DbObject * DbPtr);

    virtual ~WorkTask();

    virtual void prepare_recycle();
    virtual void recycle();

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
 * Background object for async open of a leveldb instance
 */

class OpenFamilyTask : public WorkTask
{
protected:
    std::string         family_name_;
    leveldb::Options   *open_options_;  // associated with db handle, we don't free it

public:
    OpenFamilyTask(
        ErlNifEnv* caller_env,
        ERL_NIF_TERM caller_ref,
        DbObject * db_handle,
        const char* family_name,
        leveldb::Options *open_options);

    virtual work_result operator()();

    OpenFamilyTask(const OpenFamilyTask &) = delete;
    OpenFamilyTask & operator=(const OpenFamilyTask &) = delete;

};

class CloseFamilyTask : public WorkTask
{
protected:
    std::string         family_name_;

public:
    CloseFamilyTask(
        ErlNifEnv* caller_env,
        ERL_NIF_TERM caller_ref,
        DbObject * db_handle,
        const char* family_name);

    virtual work_result operator()();

    CloseFamilyTask(const CloseFamilyTask &) = delete;
    CloseFamilyTask & operator=(const CloseFamilyTask &) = delete;

};

/**
 * Background object for async write
 */

class WriteTask : public WorkTask
{
protected:
    std::unique_ptr<leveldb::WriteBatch>    batch;
    std::unique_ptr<leveldb::WriteOptions>  options;
    std::string family;

public:

    WriteTask(
        ErlNifEnv* _owner_env,
        ERL_NIF_TERM _caller_ref,
        DbObject * _db_handle,
        const char *family,
        leveldb::WriteBatch* _batch,
        leveldb::WriteOptions* _options);

    virtual work_result operator()();
};  // class WriteTask


/**
 * Alternate object for retrieving data out of leveldb.
 *  Reduces one memcpy operation.
 */
class BinaryValue : public leveldb::Value
{
private:
    ErlNifEnv* m_env;
    ERL_NIF_TERM& m_value_bin;

    BinaryValue(const BinaryValue&);
    void operator=(const BinaryValue&);

public:

    BinaryValue(ErlNifEnv* env, ERL_NIF_TERM& value_bin)
    : m_env(env), m_value_bin(value_bin)
    {};

    virtual ~BinaryValue() {};

    BinaryValue & assign(const char* data, size_t size)
    {
        unsigned char* v = enif_make_new_binary(m_env, size, &m_value_bin);
        memcpy(v, data, size);
        return *this;
    };

};


/**
 * Background object for async get,
 *  using new BinaryValue object
 */

class GetTask : public WorkTask
{
protected:
    std::string                        m_Key;
    leveldb::ReadOptions              options;
    std::string family;

public:
    GetTask(
        ErlNifEnv *_caller_env,
        ERL_NIF_TERM _caller_ref,
        DbObject *_db_handle,
        const char *family,
        ERL_NIF_TERM _key_term,
        leveldb::ReadOptions &_options);

    virtual work_result operator()();
};  // class GetTask



/**
 * Background object to open/start an iteration
 */

class IterTask : public WorkTask
{
protected:

    const bool keys_only;
    leveldb::ReadOptions options;

public:
    IterTask(ErlNifEnv *_caller_env,
             ERL_NIF_TERM _caller_ref,
             DbObject *_db_handle,
             const bool _keys_only,
             leveldb::ReadOptions &_options)
        : WorkTask(_caller_env, _caller_ref, _db_handle),
        keys_only(_keys_only), options(_options)
    {}

    virtual ~IterTask()
    {
    }

    virtual work_result operator()()
    {
        ItrObject * itr_ptr;
        void * itr_ptr_ptr;

        // NOTE: transfering ownership of options to ItrObject
        itr_ptr_ptr=ItrObject::CreateItrObject(m_DbPtr.get(), keys_only, options);

        // Copy caller_ref to reuse in future iterator_move calls
        itr_ptr=*(ItrObject**)itr_ptr_ptr;
        itr_ptr->itr_ref_env = enif_alloc_env();
        itr_ptr->itr_ref = enif_make_copy(itr_ptr->itr_ref_env, caller_ref());

        itr_ptr->m_Iter.assign(new LevelIteratorWrapper(itr_ptr, keys_only,
                                                        options, itr_ptr->itr_ref));

        ERL_NIF_TERM result = enif_make_resource(local_env(), itr_ptr_ptr);

        // release reference created during CreateItrObject()
        enif_release_resource(itr_ptr_ptr);

        return work_result(local_env(), ATOM_OK, result);
    }   // operator()

};  // class IterTask


class MoveTask : public WorkTask
{
public:
    typedef enum { FIRST, LAST, NEXT, PREV, SEEK, PREFETCH, PREFETCH_STOP } action_t;

protected:
    ReferencePtr<LevelIteratorWrapper> m_ItrWrap;             //!< access to database, and holds reference

public:
    action_t                                       action;
    std::string                                 seek_target;

public:

    // No seek target:
    MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
             LevelIteratorWrapper * IterWrap, action_t& _action)
        : WorkTask(NULL, _caller_ref, IterWrap->m_DbPtr.get()),
        m_ItrWrap(IterWrap), action(_action)
    {
        // special case construction
        local_env_=NULL;
        enif_self(_caller_env, &local_pid);
    }

    // With seek target:
    MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
             LevelIteratorWrapper * IterWrap, action_t& _action,
             std::string& _seek_target)
        : WorkTask(NULL, _caller_ref, IterWrap->m_DbPtr.get()),
        m_ItrWrap(IterWrap), action(_action),
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


/**
 * Background object for async databass close
 */

class CloseTask : public WorkTask
{
protected:

public:

    CloseTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
              DbObject * _db_handle)
        : WorkTask(_owner_env, _caller_ref, _db_handle)
    {}

    virtual ~CloseTask()
    {
    }

    virtual work_result operator()()
    {
        DbObject * db_ptr;

        // get db pointer then clear reference count to it
        db_ptr=m_DbPtr.get();
        m_DbPtr.assign(NULL);

        if (NULL!=db_ptr)
        {
            // set closing flag, this is blocking
            db_ptr->InitiateCloseRequest();

            // db_ptr no longer valid
            db_ptr=NULL;

            return(work_result(ATOM_OK));
        }   // if
        else
        {
            return work_result(local_env(), ATOM_ERROR, ATOM_BADARG);
        }   // else
    }

};  // class CloseTask


/**
 * Background object for async iterator close
 */
class ItrCloseTask : public WorkTask
{
protected:
    ReferencePtr<ItrObject> m_ItrPtr;

public:

    ItrCloseTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
              ItrObject * _itr_handle)
        : WorkTask(_owner_env, _caller_ref),
        m_ItrPtr(_itr_handle)
    {}

    virtual ~ItrCloseTask()
    {
    }

    virtual work_result operator()()
    {
        ItrObject * itr_ptr;

        // get iterator pointer then clear reference count to it
        itr_ptr=m_ItrPtr.get();
        m_ItrPtr.assign(NULL);

        if (NULL!=itr_ptr)
        {
            // set closing flag, this is blocking
            itr_ptr->InitiateCloseRequest();

            // itr_ptr no longer valid
            itr_ptr=NULL;

            return(work_result(ATOM_OK));
//            return(work_result());  // no message
        }   // if
        else
        {
            return work_result(local_env(), ATOM_ERROR, ATOM_BADARG);
//            return(work_result());  // no message
        }   // else
    }

};  // class ItrCloseTask

struct RangeScanOptions {

  // Byte-level controls for batching/ack

  size_t max_unacked_bytes;
  size_t low_bytes;
  size_t max_batch_bytes;

  // Max number of items to return. Zero means unlimited.

  size_t limit;

  // Include the start key in streaming iteration?

  bool start_inclusive;

  // Include the end key in streaming iteration?

  bool end_inclusive;

  // Read options

  bool fill_cache;
  bool verify_checksums;

  ExpressionNode<bool>* range_filter;
  Extractor* extractor;

  RangeScanOptions()
    : max_unacked_bytes(10 * 1024 * 1024), 
    low_bytes(2 * 1024 * 1024),
    max_batch_bytes(1 * 1024 * 1024),
    limit(0),
    start_inclusive(true), end_inclusive(false), 
    fill_cache(false), verify_checksums(true), range_filter(0), extractor(0)
  { }
};


class RangeScanTask : public WorkTask
{
public:
    // Used to coordinate production and consumption of batches of data.
    // Producers acknowledge each batch received. Consumers block when the
    // unacked limit has been reached and need to be woken up by the consumer.
    // When consumers die, the ref count is decremented and that will signal
    // the producer to go away too.
    class SyncObject : public RefObject {
        public:
            explicit SyncObject(const RangeScanOptions & opts);
            ~SyncObject();

            // True if only one side (producer or consumer) alive.
            inline bool SingleOwned() { return m_RefCount == 1; }

            // Adds number of bytes sent to count.
            // Will block if count exceeds max waiting for the other
            // side to ack some and take it under the limit or for the other
            // side to shut down.
            void AddBytes(uint32_t n);

            void AckBytes(uint32_t n);
            bool AckBytesRet(uint32_t n);

            // Should be called when the Erlang handle is garbage collected
            // so no process is there to consume the output.
            void MarkConsumerDead();

            bool IsConsumerDead() const;

        private:
            const uint32_t max_bytes_;
            const uint32_t low_bytes_;
            volatile uint32_t num_bytes_;
            bool producer_sleeping_;
            // Set if producer filled up but consumer acked before
            // producer went to sleep. Producer should abort going to 
            // sleep upon seeing this set.
            volatile bool pending_signal_;
            volatile bool consumer_dead_;
            volatile bool crossed_under_max_;
            ErlNifMutex * mutex_;
            ErlNifCond * cond_;
    };

    struct SyncHandle {
        SyncObject * sync_obj;
    };

    RangeScanTask(ErlNifEnv * caller_env,
                  ERL_NIF_TERM caller_ref,
                  DbObject * db_handle,
                  const std::string & start_key,
                  const std::string * end_key,
                  RangeScanOptions & options,
                  SyncObject * sync_obj);

    virtual ~RangeScanTask();
    virtual work_result operator()();

    static void CreateSyncHandleType(ErlNifEnv * env);
    static SyncHandle * CreateSyncHandle(const RangeScanOptions & options);
    static SyncHandle * RetrieveSyncHandle(ErlNifEnv * env, ERL_NIF_TERM term);
    static void SyncHandleResourceCleanup(ErlNifEnv * env, void * arg);

private:
    static ErlNifResourceType * sync_handle_resource_;
protected:
    RangeScanOptions options_;
    std::string start_key_;
    std::string end_key_;
    bool has_end_key_;
    SyncObject * sync_obj_;

};  // class RangeScanTask

class RangeScanTaskOld : public RangeScanTask {
 public:

    RangeScanTaskOld(ErlNifEnv * caller_env,
		     ERL_NIF_TERM caller_ref,
		     DbObject * db_handle,
		     const std::string & start_key,
		     const std::string * end_key,
		     RangeScanOptions & options,
		     SyncObject * sync_obj);
    
    virtual ~RangeScanTaskOld();
    virtual work_result operator()();

}; // class RangeScanTaskOld

/**
 * Background object for async open of a leveldb instance
 */

class DestroyTask : public WorkTask
{
protected:
    std::string         db_name;
    leveldb::Options   *open_options;  // associated with db handle, we don't free it

public:
    DestroyTask(ErlNifEnv* caller_env, ERL_NIF_TERM& _caller_ref,
             const std::string& db_name_, leveldb::Options *open_options_);

    virtual ~DestroyTask() {};

    virtual work_result operator()();

private:
    DestroyTask();
    DestroyTask(const DestroyTask &);
    DestroyTask & operator=(const DestroyTask &);

};  // class DestroyTask

} // namespace eleveldb


#endif  // INCL_WORKITEMS_H
