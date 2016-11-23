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

#ifndef INCL_WORKITEMS_H
#define INCL_WORKITEMS_H

#include <stdint.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#define LEVELDB_PLATFORM_POSIX
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/thread_tasks.h"

#ifndef __WORK_RESULT_HPP
    #include "work_result.hpp"
#endif

#ifndef ATOMS_H
    #include "atoms.h"
#endif

#ifndef INCL_REFOBJECTS_H
    #include "refobjects.h"
#endif

namespace eleveldb {

/* Type returned from a work task: */
typedef basho::async_nif::work_result   work_result;



/**
 * Virtual base class for async NIF work items:
 */
class WorkTask : public leveldb::ThreadTask
{
 protected:
    ReferencePtr<DbObject> m_DbPtr;             //!< access to database, and holds reference

    ErlNifEnv      *local_env_;
    ERL_NIF_TERM   caller_ref_term;
    ERL_NIF_TERM   caller_pid_term;
    bool           terms_set;

    ErlNifPid local_pid;   // maintain for task lifetime (JFW)

 public:
    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref);

    WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref, DbObjectPtr_t & DbPtr);

    virtual ~WorkTask();

    // this is the method called from the thread pool's worker thread; it
    // calls DoWork(), implemented in the subclass, and returns the result
    // of the work to the caller
    virtual void operator()();

    virtual ErlNifEnv *local_env()         { return local_env_; }

    // call local_env() since the virtual creates the data in MoveTask
    const ERL_NIF_TERM& caller_ref()       { local_env(); return caller_ref_term; }
    const ERL_NIF_TERM& pid()              { local_env(); return caller_pid_term; }

 protected:
    // this is the method that does the real work for this task
    virtual work_result DoWork() = 0;

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

protected:
    virtual work_result DoWork();

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
    leveldb::WriteOptions*  options;

public:
    WriteTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                DbObjectPtr_t & _db_handle,
                leveldb::WriteBatch* _batch,
              leveldb::WriteOptions* _options);

    virtual ~WriteTask();

protected:
    virtual work_result DoWork();

private:
    WriteTask();
    WriteTask(const WriteTask &);
    WriteTask & operator=(const WriteTask &);

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

public:
    GetTask(ErlNifEnv *_caller_env,
            ERL_NIF_TERM _caller_ref,
            DbObjectPtr_t & _db_handle,
            ERL_NIF_TERM _key_term,
            leveldb::ReadOptions &_options);

    virtual ~GetTask();

    virtual work_result DoWork();

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
             DbObjectPtr_t & _db_handle,
             const bool _keys_only,
             leveldb::ReadOptions &_options);

    virtual ~IterTask();

    virtual work_result DoWork();

};  // class IterTask


class MoveTask : public WorkTask
{
public:
    typedef enum { FIRST, LAST, NEXT, PREV, SEEK, PREFETCH, PREFETCH_STOP } action_t;

protected:
    ItrObjectPtr_t m_Itr;

public:
    action_t                                       action;
    std::string                                 seek_target;

public:

    // No seek target:
    MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
             ItrObjectPtr_t & Iter, action_t& _action);

    // With seek target:
    MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
             ItrObjectPtr_t & Iter, action_t& _action,
             std::string& _seek_target);

    virtual ~MoveTask();

    virtual ErlNifEnv *local_env();

    virtual void recycle();

protected:
    virtual work_result DoWork();

};  // class MoveTask


/**
 * Background object for async databass close
 */

class CloseTask : public WorkTask
{
protected:

public:

    CloseTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
              DbObjectPtr_t & _db_handle);

    virtual ~CloseTask();

    virtual work_result DoWork();

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
              ItrObjectPtr_t & _itr_handle);

    virtual ~ItrCloseTask();

    virtual work_result DoWork();

};  // class ItrCloseTask


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

protected:
    virtual work_result DoWork();

private:
    DestroyTask();
    DestroyTask(const DestroyTask &);
    DestroyTask & operator=(const DestroyTask &);

};  // class DestroyTask



} // namespace eleveldb


#endif  // INCL_WORKITEMS_H
