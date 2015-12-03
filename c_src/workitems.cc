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

#include <syslog.h>

#ifndef __ELEVELDB_DETAIL_HPP
    #include "detail.hpp"
#endif

#ifndef INCL_WORKITEMS_H
    #include "workitems.h"
#endif

#include "filter_parser.h"

#include "ErlUtil.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
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
    : terms_set(false), resubmit_work(false)
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
    : m_DbPtr(DbPtr), terms_set(false), resubmit_work(false)
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

}   // OpenTask::operator()

/**
 * WriteTask functions
 */

WriteTask::WriteTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                     DbObject * _db_handle,
                     leveldb::WriteBatch* _batch,
                     leveldb::WriteOptions* _options)
    : WorkTask(_owner_env, _caller_ref, _db_handle),
      batch(_batch),
      options(_options)
{}
    
WriteTask::~WriteTask()
{
    delete batch;
    delete options;
}
    
work_result 
WriteTask::operator()()
{
    leveldb::Status status = m_DbPtr->m_Db->Write(*options, batch);
    
    return (status.ok() ? work_result(ATOM_OK) : work_result(local_env(), ATOM_ERROR_DB_WRITE, status));
}

/**
 * GetTask functions
 */

GetTask::GetTask(ErlNifEnv *_caller_env,
                 ERL_NIF_TERM _caller_ref,
                 DbObject *_db_handle,
                 ERL_NIF_TERM _key_term,
                 leveldb::ReadOptions &_options)
    : WorkTask(_caller_env, _caller_ref, _db_handle),
      options(_options)
{
    ErlNifBinary key;
    
    enif_inspect_binary(_caller_env, _key_term, &key);
    m_Key.assign((const char *)key.data, key.size);
}

GetTask::~GetTask() {}

work_result 
GetTask::operator()()
{
    ERL_NIF_TERM value_bin;
    BinaryValue value(local_env(), value_bin);
    leveldb::Slice key_slice(m_Key);
    
    leveldb::Status status = m_DbPtr->m_Db->Get(options, key_slice, &value);
    
    if(!status.ok()){
        if ( status.IsNotFound() )
            return work_result(ATOM_NOT_FOUND);
        else
            return work_result(local_env(), ATOM_ERROR, status);
    }

    return work_result(local_env(), ATOM_OK, value_bin);
}

/**
 * IterTask functions
 */

IterTask::IterTask(ErlNifEnv *_caller_env,
                   ERL_NIF_TERM _caller_ref,
                   DbObject *_db_handle,
                   const bool _keys_only,
                   leveldb::ReadOptions &_options)
    : WorkTask(_caller_env, _caller_ref, _db_handle),
      keys_only(_keys_only), options(_options)
{}

IterTask::~IterTask() {}

work_result 
IterTask::operator()()
{
    ItrObject* itr_ptr=0;
    void*     itr_ptr_ptr=0;
    
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

/**
 * MoveTask functions
 */

// Constructor with no seek target:

MoveTask::MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
                   LevelIteratorWrapper * IterWrap, action_t& _action)
    : WorkTask(NULL, _caller_ref, IterWrap->m_DbPtr.get()),
      m_ItrWrap(IterWrap), action(_action)
{
    // special case construction
    local_env_=NULL;
    enif_self(_caller_env, &local_pid);
}

// Constructor with seek target:

MoveTask::MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
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

MoveTask::~MoveTask() {};

work_result
MoveTask::operator()()
{
    leveldb::Iterator* itr;

    itr=m_ItrWrap->get();


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

    // debug syslog(LOG_ERR, "                     MoveItem::operator() %d, %d, %d",
    //              action, m_ItrWrap->m_StillUse, m_ItrWrap->m_HandoffAtomic);

    // who got back first, us or the erlang loop
    if (compare_and_swap(&m_ItrWrap->m_HandoffAtomic, 0, 1))
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
                prepare_recycle();

            // erlang is waiting, send message
            if(m_ItrWrap->m_KeysOnly)
                return work_result(local_env(), ATOM_OK, slice_to_binary(local_env(), itr->key()));

            return work_result(local_env(), ATOM_OK,
                               slice_to_binary(local_env(), itr->key()),
                               slice_to_binary(local_env(), itr->value()));
        }   // if
        else
        {
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

/**
 * CloseTask functions
 */

CloseTask::CloseTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                     DbObject * _db_handle)
    : WorkTask(_owner_env, _caller_ref, _db_handle)
{}

CloseTask::~CloseTask()
{
}

work_result 
CloseTask::operator()()
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

/**
 * ItrCloseTask functions
 */

ItrCloseTask::ItrCloseTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                           ItrObject * _itr_handle)
    : WorkTask(_owner_env, _caller_ref),
      m_ItrPtr(_itr_handle)
{}

ItrCloseTask::~ItrCloseTask()
{
}

work_result 
ItrCloseTask::operator()()
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
DestroyTask::operator()()
{
    leveldb::Status status = leveldb::DestroyDB(db_name, *open_options);

    if(!status.ok())
        return error_tuple(local_env(), ATOM_ERROR_DB_DESTROY, status);

    return work_result(ATOM_OK);

}   // DestroyTask::operator()

/**
 * RangeScanOptions functions
 */
RangeScanOptions::RangeScanOptions() : 
    max_unacked_bytes(10 * 1024 * 1024), 
    low_bytes(2 * 1024 * 1024),
    max_batch_bytes(1 * 1024 * 1024),
    limit(0),
    start_inclusive(true), 
    end_inclusive(false), 
    fill_cache(false), 
    verify_checksums(true), 
    encodingType_(Encoding::NONE),
    env_(0), 
    useRangeFilter_(false) {};

RangeScanOptions::~RangeScanOptions() {};
    
//------------------------------------------------------------
// Sanity-check filter options
//------------------------------------------------------------

void RangeScanOptions::checkOptions() 
{
        
    //------------------------------------------------------------
    // If a filter was specified, we don't allocate the extractor
    // until we're done parsing the options.  This is because we
    // require that an a valid encoding is specified before we can
    // choose which extractor to use
    //------------------------------------------------------------
    
    if(useRangeFilter_) {
        switch (encodingType_) {
        case Encoding::MSGPACK:
            break;
        case Encoding::NONE:
            ThrowRuntimeError("No object encoding was specified");
            break;
        case Encoding::UNKNOWN:
            ThrowRuntimeError("An invalud object encoding was specified");
            break;
        default:
            ThrowRuntimeError("An unsupported object encoding was specified: " << encodingType_);
            break;
        }
    }
};

/**
 * RangeScanTask::SyncObject
 */

RangeScanTask::SyncObject::SyncObject(const RangeScanOptions & opts)
  : max_bytes_(opts.max_unacked_bytes), 
    low_bytes_(opts.low_bytes),
    num_bytes_(0),
    producer_sleeping_(false), pending_signal_(false), consumer_dead_(false),
    crossed_under_max_(false), mutex_(NULL), cond_(NULL)
{
    mutex_ = enif_mutex_create(0);
    cond_  = enif_cond_create(0);
}

RangeScanTask::SyncObject::~SyncObject()
{
    enif_mutex_destroy(mutex_);
    enif_cond_destroy(cond_);
}

void RangeScanTask::SyncObject::AddBytes(uint32_t n)
{
    uint32_t num_bytes = add_and_fetch(&num_bytes_, n);
    // Block if buffer full.
    if (num_bytes >= max_bytes_) {
        enif_mutex_lock(mutex_);
        if (!consumer_dead_ && !pending_signal_) {
            producer_sleeping_ = true;
            while (producer_sleeping_) {
                enif_cond_wait(cond_, mutex_);
            }
        }
        if (pending_signal_)
            pending_signal_ = false;
        enif_mutex_unlock(mutex_);
    }
}

bool RangeScanTask::SyncObject::AckBytesRet(uint32_t n)
{
    uint32_t num_bytes = sub_and_fetch(&num_bytes_, n);
    bool ret;
    
    const bool is_reack = n == 0;
    const bool is_under_max = num_bytes < max_bytes_;
    const bool was_over_max = num_bytes_ + n >= max_bytes_;
    const bool went_under_max = is_under_max && was_over_max;

    if (went_under_max || is_reack) {
        enif_mutex_lock(mutex_);
        if (producer_sleeping_) {
            producer_sleeping_ = false;
            enif_cond_signal(cond_);
            ret = false;
        } else {
            // Producer crossed the threshold, but we caught it before it 
            // blocked. Pending a cond signal to wake it when it does.
            pending_signal_ = true;
            ret = true;
        }
        enif_mutex_unlock(mutex_);
    } else 
        ret = false;

    return ret;
}

void RangeScanTask::SyncObject::AckBytes(uint32_t n)
{
    uint32_t num_bytes = sub_and_fetch(&num_bytes_, n);
    
    if (num_bytes < max_bytes_ && num_bytes_ + n >= max_bytes_)
        crossed_under_max_ = true;
    
    // Detect if at some point buffer was full, but now we have
    // acked enough bytes to go under the low watermark.
    if (crossed_under_max_ && num_bytes < low_bytes_) {
        crossed_under_max_ = false;
        enif_mutex_lock(mutex_);
        if (producer_sleeping_) {
            producer_sleeping_ = false;
            enif_cond_signal(cond_);
        } else {
            pending_signal_ = true;
        }
        enif_mutex_unlock(mutex_);
    }
}

void RangeScanTask::SyncObject::MarkConsumerDead() {
    enif_mutex_lock(mutex_);
    consumer_dead_ = true;
    if (producer_sleeping_) {
        producer_sleeping_ = false;
        enif_cond_signal(cond_);
    }
    enif_mutex_unlock(mutex_);
}

bool RangeScanTask::SyncObject::IsConsumerDead() const {
    return consumer_dead_;
}

/**
 * RangeScanTask
 */
RangeScanTask::RangeScanTask(ErlNifEnv * caller_env,
                             ERL_NIF_TERM caller_ref,
                             DbObject * db_handle,
                             const std::string & start_key,
                             const std::string * end_key,
                             RangeScanOptions & options,
                             SyncObject * sync_obj)
  : WorkTask(caller_env, caller_ref, db_handle),
    options_(options),
    start_key_(start_key),
    has_end_key_(bool(end_key)),
    sync_obj_(sync_obj),
    range_filter_(0),
    extractor_(0)
{
    //------------------------------------------------------------
    // Sanity checks
    //------------------------------------------------------------
    
    if(options_.useRangeFilter_) {
        if(options_.encodingType_ == Encoding::MSGPACK)
            extractor_ = new ExtractorMsgpack();
        else {
            ThrowRuntimeError("An invalid object encoding was specified");
        }

        //------------------------------------------------------------
        // Since the data types are now passed with the filter, we no
        // longer need to wait to decode some data to determine them,
        // so parse the filter up-front.  Note that to use the code
        // as-is, we need to set extractor_->typesParsed_ to true as
        // if we had parsed the datatypes
        //------------------------------------------------------------

        bool throwIfBadFilter = true;
        
        //------------------------------------------------------------
        // The code allows for individual filter conditions to be bad,
        // i.e., conditions that reference fields that don't exist, or
        // compare field values to constants of the wrong type.
        //
        // If throwIfBadFilter is set to true, these are treated as fatal
        // errors, halting execution of the loop.
        // 
        // If on the other hand, throwIfBadFilter is set to false, bad
        // filter conditions are ignored (always true), and the rest of
        // the filter will be evaluated anyway
        //------------------------------------------------------------
        
        extractor_->typesParsed_ = true;

        range_filter_ = parse_range_filter_opts(options_.env_, 
                                                options_.rangeFilterSpec_, 
                                                *(extractor_), throwIfBadFilter);
    }
    
    if(!sync_obj_) {
        ThrowRuntimeError("Constructor was called with NULL SyncObject pointer");
    }
    
    if (end_key) {
        end_key_ = *end_key;
    }
    
    sync_obj_->RefInc();
}

RangeScanTask::~RangeScanTask()
{
    if(range_filter_) {
        delete range_filter_;
        range_filter_ = 0;
    }
    
    if(extractor_) {
        delete extractor_;
        extractor_ = 0;
    }
    
    sync_obj_->RefDec();
}

void RangeScanTask::sendMsg(ErlNifEnv * msg_env, ERL_NIF_TERM atom, ErlNifPid pid)
{
    if(!sync_obj_->IsConsumerDead()) {
        ERL_NIF_TERM ref_copy = enif_make_copy(msg_env, caller_ref_term);
        ERL_NIF_TERM msg      = enif_make_tuple2(msg_env, atom, ref_copy);
        
        enif_send(NULL, &pid, msg_env, msg);
    }
}

void RangeScanTask::sendMsg(ErlNifEnv * msg_env, ERL_NIF_TERM atom, ErlNifPid pid, std::string msg)
{
    if(!sync_obj_->IsConsumerDead()) {
        ERL_NIF_TERM ref_copy = enif_make_copy(msg_env, caller_ref_term);
	ERL_NIF_TERM msg_str  = enif_make_string(msg_env, msg.c_str(), ERL_NIF_LATIN1);
        ERL_NIF_TERM msg      = enif_make_tuple3(msg_env, atom, ref_copy, msg_str);
        
        enif_send(NULL, &pid, msg_env, msg);
    }
}

void RangeScanTask::send_streaming_batch(ErlNifPid * pid, ErlNifEnv * msg_env, ERL_NIF_TERM ref_term,
                                         ErlNifBinary * bin) 
{
    // Binary now owned. No need to release it.

    ERL_NIF_TERM bin_term = enif_make_binary(msg_env, bin);
    ERL_NIF_TERM local_ref = enif_make_copy(msg_env, ref_term);
    ERL_NIF_TERM msg =
        enif_make_tuple3(msg_env, ATOM_STREAMING_BATCH, local_ref, bin_term);
    enif_send(NULL, pid, msg_env, msg);
    enif_clear_env(msg_env);
}

int RangeScanTask::VarintLength(uint64_t v) 
{
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

char* RangeScanTask::EncodeVarint64(char* dst, uint64_t v) 
{
    static const uint64_t B = 128;
    unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
    while (v >= B) {
        *(ptr++) = (v & (B-1)) | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<unsigned char>(v);
    return reinterpret_cast<char*>(ptr);
}

/**.......................................................................
 * Perform range-scan work, with optional filtering
 */
work_result RangeScanTask::operator()()
{
    ErlNifEnv* env     = local_env_;
    ErlNifEnv* msg_env = enif_alloc_env();

    leveldb::ReadOptions read_options;

    read_options.fill_cache = options_.fill_cache;
    read_options.verify_checksums = options_.verify_checksums;

    leveldb::Iterator* iter        = m_DbPtr->m_Db->NewIterator(read_options);
    const leveldb::Comparator* cmp = m_DbPtr->m_DbOptions->comparator;

    const leveldb::Slice skey_slice(start_key_);
    const leveldb::Slice ekey_slice(end_key_);

    iter->Seek(skey_slice);

    ErlNifPid pid;
    enif_get_local_pid(env, caller_pid_term, &pid);

    ErlNifBinary bin;
    const size_t initial_bin_size = size_t(options_.max_batch_bytes * 1.1);
    size_t out_offset = 0;
    size_t num_read   = 0;

    //------------------------------------------------------------
    // Skip if not including first key and first key exists
    //------------------------------------------------------------

    if (!options_.start_inclusive
        && iter->Valid()
        && cmp->Compare(iter->key(), skey_slice) == 0) {
        iter->Next();
    }

    while (!sync_obj_->IsConsumerDead()) {

        //------------------------------------------------------------
        // If reached end (iter invalid) or we've reached the
        // specified limit on number of items (options_.limit), or the
        // current key is past end key, send the batch and break out of the loop
        //------------------------------------------------------------
  
        if (!iter->Valid()
            || (options_.limit > 0 && num_read >= options_.limit)
            || (has_end_key_ &&
                (options_.end_inclusive ?
                 cmp->Compare(iter->key(), ekey_slice) > 0 :
                 cmp->Compare(iter->key(), ekey_slice) >= 0
                ))) {

	  // If data are present in the batch (ie, out_offset != 0),
	  // send the batch now

	    if (out_offset) {
            
	      // Shrink it to final size.

                if (out_offset != bin.size)
                    enif_realloc_binary(&bin, out_offset);

                send_streaming_batch(&pid, msg_env, caller_ref_term, &bin);
                out_offset = 0;
            }

            break;
        }

	//------------------------------------------------------------
	// Else keep going; shove the next entry in the batch, but
	// only if it passes any user-specified filter.  We default to
	// filter_passed = true, in case we are not using a filter,
	// which will cause all keys to be returned
	//------------------------------------------------------------

        leveldb::Slice key   = iter->key();
        leveldb::Slice value = iter->value();

        bool filter_passed = true;

	//------------------------------------------------------------
	// If we are using a filter, evaluate it here
	//------------------------------------------------------------

        if(options_.useRangeFilter_) {

            try {

                //------------------------------------------------------------
                // Now extract relevant fields of this object prior to
                // evaluating the filter. We check if the range filter is
                // non-NULL, because passing a filter specification that
                // refers to invalid fields can result in a NULL
                // expression tree.  In this case, we don't bother
                // extracting the data or evaluating the filter
                //------------------------------------------------------------

                if(range_filter_) {

                    //------------------------------------------------------------
                    // Also check if the key can be parsed.  If TS-encoded
                    // data and non-TS encoded data are interleaved, this
                    // causes us to ignore the non-TS-encoded data
                    //------------------------------------------------------------
                    
                    if(extractor_->riakObjectContentsCanBeParsed(value.data(), value.size())) {
                        
                        extractor_->extractRiakObject(value.data(), value.size(), range_filter_);
                        
                        //------------------------------------------------------------
                        // Now evaluate the filter, but only if we have a valid filter
                        //------------------------------------------------------------
                        
                        filter_passed = range_filter_->evaluate();

                    } else {
                        filter_passed = false;
                    }
                }

                //------------------------------------------------------------
                // Trap errors thrown during filter parsing or value
                // extraction, and propagate to erlang
                //------------------------------------------------------------
                
            } catch(std::runtime_error& err) {
                std::ostringstream os;
                os << err.what() << std::endl << "While processing key: " 
                   << ErlUtil::formatBinary((unsigned char*)key.data(), key.size());
                
                sendMsg(msg_env, ATOM_STREAMING_ERROR, pid, os.str());
                return work_result(local_env(), ATOM_ERROR, ATOM_STREAMING_ERROR);
            }
            
        }

        if (filter_passed) {
	  
            const size_t ksz = key.size();
	    const size_t vsz = value.size();

            const size_t ksz_sz = VarintLength(ksz);
            const size_t vsz_sz = VarintLength(vsz);

            const size_t esz = ksz + ksz_sz + vsz + vsz_sz;
            const size_t next_offset = out_offset + esz;

            // Allocate the output data array if this is the first data
	    // (i.e., if out_offset == 0)

            if(out_offset == 0)
                enif_alloc_binary(initial_bin_size, &bin);

	    //------------------------------------------------------------
            // If we need more space, allocate it exactly since that means we
            // reached the batch max anyway and will send it right away
	    //------------------------------------------------------------

            if(next_offset > bin.size)
                enif_realloc_binary(&bin, next_offset);

            char * const out = (char*)bin.data + out_offset;

            EncodeVarint64(out, ksz);
            memcpy(out + ksz_sz, key.data(), ksz);

            EncodeVarint64(out + ksz_sz + ksz, vsz);
            memcpy(out + ksz_sz + ksz + vsz_sz, value.data(), vsz);

            out_offset = next_offset;
	    
	    //------------------------------------------------------------
	    // If we've reached the maximum number of bytes to include in
	    // the batch, possibly shrink the binary and send it
	    //------------------------------------------------------------

            if(out_offset >= options_.max_batch_bytes) {

                if(out_offset != bin.size)
                    enif_realloc_binary(&bin, out_offset);

                send_streaming_batch(&pid, msg_env, caller_ref_term, &bin);

                // Maybe block if max reached.

                sync_obj_->AddBytes(out_offset);

                out_offset = 0;
	    }

	    //------------------------------------------------------------
	    // Increment the number of keys read and step to the next
	    // key
	    //------------------------------------------------------------

	    ++num_read;

	} else {
            //	  COUT("Filter DIDN'T pass");
	}

        iter->Next();
    }

    //------------------------------------------------------------
    // If exiting the work loop, send a streaming_end message to any
    // waiting erlang threads
    //------------------------------------------------------------

    sendMsg(msg_env, ATOM_STREAMING_END, pid);

    if(out_offset)
        enif_release_binary(&bin);

    enif_free_env(msg_env);

    return work_result();

}   // RangeScanTask::operator()

ErlNifResourceType * RangeScanTask::sync_handle_resource_ = NULL;

void RangeScanTask::CreateSyncHandleType(ErlNifEnv * env)
{
    ErlNifResourceFlags flags = (ErlNifResourceFlags)(ERL_NIF_RT_CREATE
                                                      | ERL_NIF_RT_TAKEOVER);
    sync_handle_resource_ =
        enif_open_resource_type(env, NULL, "eleveldb_range_scan_sync_handle",
                                &RangeScanTask::SyncHandleResourceCleanup,
                                flags, NULL);
    return;
}

RangeScanTask::SyncHandle *
RangeScanTask::CreateSyncHandle(const RangeScanOptions & options)
{
    SyncObject * sync_obj = new SyncObject(options);
    sync_obj->RefInc();
    SyncHandle * handle =
        (SyncHandle*)enif_alloc_resource(sync_handle_resource_,
                                         sizeof(SyncHandle));
    handle->sync_obj_ = sync_obj;
    return handle;
}

RangeScanTask::SyncHandle *
RangeScanTask::RetrieveSyncHandle(ErlNifEnv * env, ERL_NIF_TERM term)
{
    void * resource_ptr;
    if (enif_get_resource(env, term, sync_handle_resource_, &resource_ptr))
        return (SyncHandle *)resource_ptr;
    return NULL;
}

void RangeScanTask::SyncHandleResourceCleanup(ErlNifEnv * env, void * arg)
{
    SyncHandle * handle = (SyncHandle*)arg;
    if (handle->sync_obj_) {
        handle->sync_obj_->MarkConsumerDead();
        handle->sync_obj_->RefDec();
        handle->sync_obj_ = NULL;
    }
}


} // namespace eleveldb


