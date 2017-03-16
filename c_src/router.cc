// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2016-2017 Basho Technologies, Inc. All Rights Reserved.
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

#include <string.h>

#ifndef INCL_ROUTER_H
    #include "router.h"
#endif

#include "leveldb/env.h"   // for Log()
#include "util/expiry_os.h"
#include "util/prop_cache.h"  // hmm, not in OS builds

namespace eleveldb {

static ERL_NIF_TERM parse_expiry_properties(ErlNifEnv* env, ERL_NIF_TERM item,
                                            leveldb::ExpiryModuleOS& opts);

// based upon error_tuple in c_src/eleveldb.cc
static ERL_NIF_TERM
error_tuple_message(
    ErlNifEnv* Env,
    ERL_NIF_TERM Error,
    const char * Message)
{
    ERL_NIF_TERM err_term, msg_term;

    if (NULL==Message || '\0'==*Message)
        Message="(empty message)";

    msg_term = enif_make_string(Env, Message, ERL_NIF_LATIN1);

    err_term = enif_make_tuple2(Env, eleveldb::ATOM_ERROR,
                                enif_make_tuple2(Env, Error, msg_term));
    return(err_term);

}   // error_tuple_message



bool
leveldb_callback(
    leveldb::EleveldbRouterActions_t Action,
    int ParamCount,
    const void ** Params)
{
    bool ret_flag(false);
    ErlNifPid pid_ptr;

    switch(Action)
    {
        // 0 - type string, 1 - bucket string, 2 - slice for key
        case leveldb::eGetBucketProperties:
        {
            ERL_NIF_TERM callback_pid;

            // defensive test
            if (3==ParamCount && NULL!=Params[1] && NULL!=Params[2]
                && gBucketPropCallback.GetPid(callback_pid))
            {
                ERL_NIF_TERM bucket_term, type_term, key_term, tuple_term;
                ErlNifEnv *msg_env = enif_alloc_env();
                int ret_val;
                unsigned char * temp_ptr;
                leveldb::Slice * key_slice;

                // build bucket and key first since used by both messages
                //   (no documented fail case to enif_make_new_binary ... ouch)
                temp_ptr=enif_make_new_binary(msg_env,strlen((const char *)Params[1]),&bucket_term);
                memcpy(temp_ptr, Params[1], strlen((const char *)Params[1]));
                key_slice=(leveldb::Slice *)Params[2];
                temp_ptr=enif_make_new_binary(msg_env,key_slice->size(),&key_term);
                memcpy(temp_ptr, key_slice->data(), key_slice->size());

                // bucket only
                if (NULL==Params[0] || '\0'==*(const char *)Params[0])
                {
                    // make some arrays
                    tuple_term=enif_make_tuple3(msg_env,
                        ATOM_INVOKE,
                        enif_make_list1(msg_env, bucket_term),
                        enif_make_list1(msg_env, key_term));
                }   // if

                // bucket type and bucket
                else
                {
                    // build type binary
                    temp_ptr=enif_make_new_binary(msg_env,strlen((const char *)Params[0]),&type_term);
                    memcpy(temp_ptr, Params[0], strlen((const char *)Params[0]));
                    tuple_term=enif_make_tuple2(msg_env, type_term, bucket_term);
                    // Make some arrays

                    tuple_term=enif_make_tuple3(msg_env,
                        ATOM_INVOKE,
                        enif_make_list1(msg_env, tuple_term),
                        enif_make_list1(msg_env, key_term));
                }   // else

                ret_val=enif_get_local_pid(msg_env, callback_pid, &pid_ptr);
                if (0!=ret_val)
                    ret_val=enif_send(NULL, &pid_ptr, msg_env, tuple_term);

                ret_flag=(0!=ret_val);
                enif_free_env(msg_env);
            }   // if
            break;
        }   // eGetBucketProperties

        // no default case ... just leave ret_flag as false

    }   // switch

    return(ret_flag);

}   // leveldb_callback


/**
 * Convert Riak Erlang properties into ExpiryModule object.
 *  Insert object into cache.
 */
ERL_NIF_TERM
property_cache(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM ret_term(ATOM_BADARG);

    // ignore if bad params
    if (argc==2 && enif_is_binary(env, argv[0]) && enif_is_list(env, argv[1]))
    {
        leveldb::ExpiryPropPtr_t cache;

        const ERL_NIF_TERM& composite_bucket  = argv[0];
        const ERL_NIF_TERM& properties        = argv[1];
        ErlNifBinary key_bin;

        enif_inspect_binary(env, composite_bucket, &key_bin);
        leveldb::Slice key_slice((const char *)key_bin.data, key_bin.size);

        // reduce property list to struct we care about
        //  (use options fold thingie?)
        leveldb::ExpiryModuleOS * opt =
            (leveldb::ExpiryModuleOS *)leveldb::ExpiryModule::CreateExpiryModule(
                &eleveldb::leveldb_callback);

        fold(env, properties, parse_expiry_properties, *opt);

        // send insert command to prop_cache ... insert should broadcast to Wait()
        if (cache.Insert(key_slice, opt))
            ret_term=ATOM_OK;
        else
            ret_term=error_tuple_message(env, ATOM_EINVAL,
                                         "eleveldb::property_cache cache.Insert() failed");
    }   // if
    else
    {
        ret_term=error_tuple_message(env, ATOM_BADARG,
                                     "eleveldb::property_cache called with bad arg count or arg types");
    }   // else

    return(ret_term);

}   // property_cache


static ERL_NIF_TERM
parse_expiry_properties(
    ErlNifEnv* env,
    ERL_NIF_TERM item,
    leveldb::ExpiryModuleOS& opts)
{
    int arity;
    const ERL_NIF_TERM* option;


    if (enif_get_tuple(env, item, &arity, &option) && 2==arity)
    {
        char buffer[65]={""};

        // what if property set via json
        if (enif_is_binary(env, option[1]))
        {
            ErlNifBinary bin;
            if (0!=enif_inspect_binary(env, option[1], &bin))
            {
                strncpy(buffer,(char *)bin.data,(bin.size<65?bin.size:64));
                buffer[bin.size<65?bin.size:64]='\0';
            }   //if
            else
            {
                *buffer='\0';
            }   // else
        }   // if

        if (option[0] == eleveldb::ATOM_EXPIRATION)
        {
            opts.SetExpiryEnabled(option[1] == eleveldb::ATOM_ENABLED
                                   || option[1] == eleveldb::ATOM_ON
                                   || option[1] == eleveldb::ATOM_TRUE
                                   || 0==strcmp(buffer, "enabled")
                                   || 0==strcmp(buffer, "on")
                                   || 0==strcmp(buffer, "true"));
        }   // else if
        else if (option[0] == eleveldb::ATOM_DEFAULT_TIME_TO_LIVE)
        {
            if (option[1] == eleveldb::ATOM_UNLIMITED
                || 0==strcmp(buffer, "unlimited"))
            {
                opts.SetExpiryUnlimited(true);
            }   // else if

            // assume it is a cuttlefish duration string
            else if ('\0' != *buffer)
            {
                opts.SetExpiryMinutes(leveldb::CuttlefishDurationMinutes(buffer));
            }   // else
        }   // else if
        else if (option[0] == eleveldb::ATOM_EXPIRATION_MODE)
        {
            if (eleveldb::ATOM_WHOLE_FILE == option[1]
                || 0==strcmp(buffer, "whole_file"))
                opts.SetWholeFileExpiryEnabled(true);
            else if (eleveldb::ATOM_PER_ITEM == option[1]
                     || 0==strcmp(buffer,"per_item"))
                opts.SetWholeFileExpiryEnabled(false);
            // else do nothing ... use global setting

        }   // else if
    }   // if

    return eleveldb::ATOM_OK;

}   // parse_expiry_properties


/**
 * This routine retrieves data from the property cache
 *  and formats into a list of property pairs.  This routine
 *  is intended for unit tests, not production.
 */
ERL_NIF_TERM
property_cache_get(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM ret_term(ATOM_BADARG);

    // ignore if bad params
    if (argc==1 && enif_is_binary(env, argv[0]))
    {
        leveldb::ExpiryPropPtr_t cache;

        const ERL_NIF_TERM& composite_bucket  = argv[0];
        ErlNifBinary key_bin;

        enif_inspect_binary(env, composite_bucket, &key_bin);
        leveldb::Slice key_slice((const char *)key_bin.data, key_bin.size);

        if (cache.Lookup(key_slice))
        {
            ERL_NIF_TERM enabled_tuple, minutes_tuple, whole_file_tuple;

            // enabled
            if (cache->IsExpiryEnabled())
                enabled_tuple=enif_make_tuple2(env, ATOM_EXPIRY_ENABLED, ATOM_ENABLED);
            else
                enabled_tuple=enif_make_tuple2(env, ATOM_EXPIRY_ENABLED, ATOM_OFF);

            // minutes
            if (cache->IsExpiryUnlimited())
                minutes_tuple=enif_make_tuple2(env, ATOM_EXPIRY_MINUTES, ATOM_UNLIMITED);
            else
            {
                ERL_NIF_TERM minutes;
                minutes=enif_make_int(env, cache->GetExpiryMinutes());
                minutes_tuple=enif_make_tuple2(env, ATOM_EXPIRY_MINUTES, minutes);
            }   // else

            // whole file
            if (cache->IsWholeFileExpiryEnabled())
                whole_file_tuple=enif_make_tuple2(env, ATOM_EXPIRATION_MODE, ATOM_WHOLE_FILE);
            else
                whole_file_tuple=enif_make_tuple2(env, ATOM_EXPIRATION_MODE, ATOM_PER_ITEM);

            ret_term=enif_make_list3(env, enabled_tuple, minutes_tuple, whole_file_tuple);
        }   //if
        else
        {
            ret_term=ATOM_EINVAL;
        }   // else
    }   // if

    return(ret_term);

}   // property_cache_get


/**
 * NEVER USE THIS IN PRODUCTION
 *  this is to support integration testing
 */
ERL_NIF_TERM
property_cache_flush(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    leveldb::PropertyCache::Flush();

    return(ATOM_OK);

}   // property_cache_flush


ERL_NIF_TERM
set_metadata_pid(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM ret_term;

    // ignore if bad params
    if (argc==2 && enif_is_pid(env, argv[1]))
    {
        // would use switch(argv[0]) but ATOM_BUCKET_PROPS is actually a
        //  variable, not a constant
        if (argv[0]==ATOM_BUCKET_PROPS)
        {
            gBucketPropCallback.SetPid(argv[1]);
            ret_term=ATOM_OK;
        }   // if
        else
        {
            ret_term=error_tuple_message(env, ATOM_BADARG,
                                         "eleveldb::set_metadata_pid called with unknown atom");
        }   // else
    }   // if
    else
    {
        ret_term=error_tuple_message(env, ATOM_BADARG,
                                     "eleveldb::set_metadata_pid called with bad arg count or pid");
    }   // else

    return(ret_term);

}   // set_metadata_pid


ERL_NIF_TERM
remove_metadata_pid(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM ret_term(ATOM_BADARG);

    // ignore if bad params
    if (argc==2 && enif_is_pid(env, argv[1]))
    {
        ERL_NIF_TERM cur_pid;

        // would use switch(argv[0]) but ATOM_BUCKET_PROPS is actually a
        //  variable, not a constant
        if (argv[0]==ATOM_BUCKET_PROPS)
        {
            if (gBucketPropCallback.GetPid(cur_pid) && argv[1]==cur_pid)
                gBucketPropCallback.Disable();
            ret_term=ATOM_OK;
        }   // if
        else
        {
            ret_term=error_tuple_message(env, ATOM_BADARG,
                                         "eleveldb::remove_metadata_pid called with unknown atom");
        }   // else
    }   // if
    else
    {
        ret_term=error_tuple_message(env, ATOM_BADARG,
                                     "eleveldb::remove_metadata_pid called with bad arg count or pid");
    }   // else

    return ret_term;

}   // remove_metadata_pid


/**
 * get_metadata_pid is used by unit tests to verify
 *  actions of set_metadata_pid and remove_metadata_pid.
 *  No production code is known to use this.
 */
ERL_NIF_TERM
get_metadata_pid(
    ErlNifEnv* env,
    int argc,
    const ERL_NIF_TERM argv[])
{
    ERL_NIF_TERM ret_term(ATOM_BADARG);

    // ignore if bad params
    if (argc==1)
    {
        ERL_NIF_TERM cur_pid;

        // would use switch(argv[0]) but ATOM_BUCKET_PROPS is actually a
        //  variable, not a constant
        if (argv[0]==ATOM_BUCKET_PROPS)
        {
            if (gBucketPropCallback.GetPid(cur_pid))
                ret_term=cur_pid;
            else
                ret_term=ATOM_EINVAL;
        }   // if
        else
        {
            ret_term=ATOM_BADARG;
        }   // else
    }   // if
    else
    {
        ret_term=ATOM_BADARG;
    }   // else

    return ret_term;

}   // get_metadata_pid

} // namespace eleveldb


