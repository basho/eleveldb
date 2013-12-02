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
    assert(result != 0);
    assert(value != 0);
    assert(s.data() != 0);
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
	assert(m_ItrWrap->m_CurrentData == 0);
    leveldb::Iterator* itr = m_ItrWrap->get();

    if(NULL == itr)
        return work_result(local_env(), ATOM_ERROR, ATOM_ITERATOR_CLOSED);

    switch(action)
    {
        case FIRST: itr->SeekToFirst(); read_single(itr); break;

        case LAST:  itr->SeekToLast();  read_single(itr); break;

        case PREFETCH:
        case NEXT:
        case PREV:  read_batch(itr); break;

        case SEEK:
        {
            leveldb::Slice key_slice(seek_target);
            itr->Seek(key_slice);
            read_single(itr);
            break;
        }   // case

        default:
            // JFW: note: *not* { ERROR, badarg() } here-- we want the exception:
            // JDB: note: We can't send an exception as a message. It crashes Erlang.
            //            Changing to be {error, badarg}.
            return work_result(local_env(), ATOM_ERROR, ATOM_BADARG);
            break;

    }   // switch


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

        if(m_ItrWrap->Valid())
        {
            if (PREFETCH==action)
                prepare_recycle();

            // erlang is waiting, send message
            work_result r(local_env(), ATOM_OK, m_ItrWrap->m_CurrentData);
            m_ItrWrap->m_CurrentData = 0;
            return r;
        }   // if
        else
        {
            assert(m_ItrWrap->m_CurrentData == 0);
            return work_result(local_env(), ATOM_ERROR, ATOM_INVALID_ITERATOR);
        }   // else

    }   // else

    return(work_result());
}

void MoveTask::read_batch(leveldb::Iterator* itr)
{
    const bool keys_only = m_ItrWrap->m_KeysOnly;
    std::vector<ERL_NIF_TERM> list;
    list.reserve(batch_size);
    for (int k = 0; k < batch_size && itr->Valid(); ++k) {

        apply_action(itr);
        if (!itr->Valid())
        {
            break;
        }
        list.push_back(extract(itr, keys_only));
    }
    if (list.size() != 0)
    {
        assert(m_ItrWrap->m_CurrentData == 0);
        m_ItrWrap->m_CurrentData = enif_make_list_from_array(local_env(), &list[0], list.size());
    }
    else
    {
        assert(m_ItrWrap->m_CurrentData == 0);
        //m_ItrWrap->m_CurrentData = 0;
    }
}

ERL_NIF_TERM MoveTask::extract(leveldb::Iterator* itr, const bool keys_only)
{
    assert(itr->Valid());
    ERL_NIF_TERM elem;
    if(keys_only)
    {
        elem = slice_to_binary(local_env(), itr->key());
    }
    else
    {
        elem = enif_make_tuple2(local_env(),
                slice_to_binary(local_env(), itr->key()),
                slice_to_binary(local_env(), itr->value()));
    }
    return elem;
}

void MoveTask::read_single(leveldb::Iterator* itr)
{
    if(itr->Valid())
    {
        const bool keys_only = m_ItrWrap->m_KeysOnly;
        m_ItrWrap->m_CurrentData = enif_make_list1(local_env(), extract(itr, keys_only));
    }
    else
    {
        m_ItrWrap->m_CurrentData = 0;
    }

}

void MoveTask::apply_action(leveldb::Iterator* itr)
{
    switch (action) {
    case PREFETCH:
    case NEXT:  itr->Next(); break;
    case PREV:  itr->Prev(); break;
    default: break;
    }
}


ErlNifEnv *
MoveTask::local_env()
{
    if (NULL==local_env_)
        local_env_ = enif_alloc_env();

    if (!terms_set)
    {
        caller_ref_term = enif_make_copy(local_env_, m_ItrWrap->m_Snap->itr_ref);
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
        {
            assert(m_ItrWrap->m_CurrentData == 0);
            enif_clear_env(local_env_);
        }

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


