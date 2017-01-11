// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
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


#ifndef INCL_ROUTER_H
    #include "router.h"
#endif

#include "leveldb/env.h"   // for Log()
#include "leveldb_ee/prop_cache.h"  // hmm, not in OS builds
#include "util/expiry_os.h"

namespace eleveldb {

static ERL_NIF_TERM parse_expiry_properties(ErlNifEnv* env, ERL_NIF_TERM item,
                                            leveldb::ExpiryModuleOS& opts);


bool
leveldb_callback(
    leveldb::EleveldbRouterActions_t Action,
    int ParamCount,
    const void ** Params)
{
    bool ret_flag(false);

    switch(Action)
    {
        // 0 - type string, 1 - bucket string, 2 - slice for key
        case leveldb::eGetBucketProperties:
        {
            // defensive test
            if (3==ParamCount && NULL!=Params[1] && NULL!=Params[2])
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
                    tuple_term=enif_make_tuple3(msg_env, ATOM_GET_BUCKET_PROPERTIES, bucket_term,
                                                key_term);
                }   // if

                // bucket type and bucket
                else
                {
                    // build type binary
                    temp_ptr=enif_make_new_binary(msg_env,strlen((const char *)Params[0]),&type_term);
                    memcpy(temp_ptr, Params[0], strlen((const char *)Params[0]));
                    tuple_term=enif_make_tuple4(msg_env, ATOM_GET_BUCKET_PROPERTIES,
                                                type_term, bucket_term, key_term);
                }   // else

                ret_val=enif_send(NULL, &gCallbackRouterPid, msg_env, tuple_term);

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
        if (!cache.Insert(key_slice, opt))
            leveldb::Log(NULL, "eleveldb::property_cache cache.Insert failed");
    }   // if
    else
    {
        leveldb::Log(NULL, "eleveldb::property_cache called with bad object (argc %d)", argc);
    }   // else

    return ATOM_OK;

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
        if (option[0] == eleveldb::ATOM_EXPIRY_ENABLED)
        {
            opts.expiry_enabled = (option[1] == eleveldb::ATOM_TRUE);
        }   // else if
        else if (option[0] == eleveldb::ATOM_EXPIRY_MINUTES)
        {
            unsigned long minutes(0);
            if (enif_get_ulong(env, option[1], &minutes))
            {
                opts.expiry_minutes = minutes;
            }   // if
        }   // else if
        else if (option[0] == eleveldb::ATOM_WHOLE_FILE_EXPIRY)
        {
            opts.whole_file_expiry = (option[1] == eleveldb::ATOM_TRUE);
        }   // else if
    }   // if

    return eleveldb::ATOM_OK;

}   // parse_expiry_properties

} // namespace eleveldb


