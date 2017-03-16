%% -------------------------------------------------------------------
%%
%%  eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
%%
%% Copyright (c) 2017 Basho Technologies, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(bucket_expiry).

-compile(export_all).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

iterator_test_() ->
    {spawn,
     [{setup,
       fun setup/0,
       fun cleanup/1,
       fun(Ref) ->
       [
        set_remove_pid(Ref),
        set_verify_prop(Ref)
       ]
       end}]
    }.


%% Property Cache needs database to be running, but does not care
%%  if there is data in the db or not
setup() ->
    os:cmd("rm -rf be_test"),  % NOTE
    {ok, Ref} = eleveldb:open("be_test", [{create_if_missing, true}]),
    Ref.

cleanup(Ref) ->
      eleveldb:close(Ref).

set_remove_pid(_Ref) ->
    fun() ->
            % validate default return expectations
            ?assertEqual( einval, eleveldb:get_metadata_pid(bucket_props)),
            ?assertEqual( badarg, eleveldb:get_metadata_pid(no_arg)),

            % wrong property name atom
            Reply1 = eleveldb:set_metadata_pid(mystery_props, list_to_pid("<0.2.0>")),
            ?assertMatch({error, {badarg, _}}, Reply1),

            % wrong second parameter
            Reply2 = eleveldb:set_metadata_pid(bucket_props, 42),
            ?assertMatch({error, {badarg, _}}, Reply2),

            % simple set
            ok = eleveldb:set_metadata_pid(bucket_props, list_to_pid("<0.2.0>")),
            ?assertEqual(list_to_pid("<0.2.0>"), eleveldb:get_metadata_pid(bucket_props)),

            % update existing
            ok = eleveldb:set_metadata_pid(bucket_props, list_to_pid("<0.231.0>")),
            ?assertEqual(list_to_pid("<0.231.0>"), eleveldb:get_metadata_pid(bucket_props)),

            % remove: wrong property name atom
            Reply3 = eleveldb:remove_metadata_pid(mystery_props, list_to_pid("<0.2.0>")),
            ?assertMatch({error, {badarg, _}}, Reply3),

            % remove: wrong second parameter
            Reply4 = eleveldb:remove_metadata_pid(bucket_props, 42),
            ?assertMatch({error, {badarg, _}}, Reply4),

            % remove old (should do nothing)
            ok = eleveldb:remove_metadata_pid(bucket_props, list_to_pid("<0.2.0>")),
            ?assertEqual(list_to_pid("<0.231.0>"), eleveldb:get_metadata_pid(bucket_props)),

            % remove active
            ok = eleveldb:remove_metadata_pid(bucket_props, list_to_pid("<0.231.0>")),
            ?assertEqual(einval, eleveldb:get_metadata_pid(bucket_props))
    end.


%%
%% Currently this test ALWAYS FAILS in open source build.  This is
%%  due to the fact that property cache is not available in open source.
%%
set_verify_prop(_Ref) ->
    %% riak_core_bucket:get_bucket(<<"default">>).

    %% WARNING:  binary strings used for unit test ARE not representative
    %%           of binary strings passed from leveldb

    fun() ->
            %% test 1: simulated default properties, using atoms
            ok=eleveldb:property_cache(<<"default">>, [{name,<<"default">>},
                                                       {allow_mult,false},
                                                       {basic_quorum,false},
                                                       {big_vclock,50},
                                                       {chash_keyfun,{riak_core_util,chash_std_keyfun}},
                                                       {default_time_to_live, <<"2345m">>},
                                                       {dvv_enabled,false},
                                                       {dw,quorum},
                                                       {expiration, true},
                                                       {expiration_mode, whole_file},
                                                       {last_write_wins,false},
                                                       {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
                                                       {n_val,3},
                                                       {notfound_ok,true},
                                                       {old_vclock,86400},
                                                       {postcommit,[]},
                                                       {pr,0},
                                                       {precommit,[]},
                                                       {pw,0},
                                                       {r,quorum},
                                                       {rw,quorum},
                                                       {small_vclock,50},
                                                       {w,quorum},
                                                       {write_once,false},
                                                       {young_vclock,20}]),
              ?assertEqual([{expiry_enabled, enabled}, {expiry_minutes, 2345}, {expiration_mode, whole_file}],
               eleveldb:property_cache_get(<<"default">>)),

            %% test 2:  only relevant properties, using JSON strings
            ok=eleveldb:property_cache(<<"test2">>, [{default_time_to_live, <<"2h5m">>},
                                                       {expiration, <<"true">>},
                                                       {expiration_mode, <<"whole_file">>}]),
              ?assertEqual([{expiry_enabled, enabled}, {expiry_minutes, 125}, {expiration_mode, whole_file}],
               eleveldb:property_cache_get(<<"test2">>)),

            %% test 3:  different JSON strings, different property order
            ok=eleveldb:property_cache(<<"test3">>, [{expiration, <<"on">>},
                                                       {expiration_mode, per_item},
                                                       {default_time_to_live, <<"30d">>}]),
              ?assertEqual([{expiry_enabled, enabled}, {expiry_minutes, 43200}, {expiration_mode, per_item}],
               eleveldb:property_cache_get(<<"test3">>)),


               %% test 4:  be sure test 2 is still live
              ?assertEqual([{expiry_enabled, enabled}, {expiry_minutes, 125}, {expiration_mode, whole_file}],
                           eleveldb:property_cache_get(<<"test2">>)),

            %% test 5: does flush work?
            ok=eleveldb:property_cache_flush(),
            ?assertEqual(einval, eleveldb:property_cache_get(<<"test2">>))

    end.

-endif.
