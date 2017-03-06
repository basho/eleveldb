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
        set_remove_pid(Ref)
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

            % simple set
            ok = eleveldb:set_metadata_pid(bucket_props, list_to_pid("<0.2.0>")),
            ?assertEqual(list_to_pid("<0.2.0>"), eleveldb:get_metadata_pid(bucket_props)),

            % update existing
            ok = eleveldb:set_metadata_pid(bucket_props, list_to_pid("<0.231.0>")),
            ?assertEqual(list_to_pid("<0.231.0>"), eleveldb:get_metadata_pid(bucket_props)),

            % remove old (should do nothing)
            ok = eleveldb:remove_metadata_pid(bucket_props, list_to_pid("<0.2.0>")),
            ?assertEqual(list_to_pid("<0.231.0>"), eleveldb:get_metadata_pid(bucket_props)),

            % remove active
            ok = eleveldb:remove_metadata_pid(bucket_props, list_to_pid("<0.231.0>")),
            ?assertEqual(einval, eleveldb:get_metadata_pid(bucket_props))
    end.

set_verify_prop(_Ref)
{
}
-endif.
