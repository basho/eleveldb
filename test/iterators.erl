%% -------------------------------------------------------------------
%%
%%  eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
%%
%% Copyright (c) 2010-2013 Basho Technologies, Inc. All Rights Reserved.
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
-module(iterators).

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
        prev_test_case(Ref),
        seek_and_next_test_case(Ref),
        basic_prefetch_test_case(Ref),
        seek_and_prefetch_test_case(Ref)
       ]
       end}]
    }.

setup() ->
    os:cmd("rm -rf ltest"),  % NOTE
    {ok, Ref} = eleveldb:open("ltest", [{create_if_missing, true}]),
    eleveldb:put(Ref, <<"a">>, <<"w">>, []),
    eleveldb:put(Ref, <<"b">>, <<"x">>, []),
    eleveldb:put(Ref, <<"c">>, <<"y">>, []),
    eleveldb:put(Ref, <<"d">>, <<"z">>, []),
    Ref.

cleanup(Ref) ->
      eleveldb:close(Ref).

prev_test_case(Ref) ->
    fun() ->
            {ok, I} = eleveldb:iterator(Ref, []),
            ?assertEqual({ok, <<"a">>, <<"w">>},eleveldb:iterator_move(I, <<>>)),
            ?assertEqual({ok, <<"b">>, <<"x">>},eleveldb:iterator_move(I, next)),
            ?assertEqual({ok, <<"a">>, <<"w">>},eleveldb:iterator_move(I, prev))
    end.

seek_and_next_test_case(Ref) ->
    fun() ->
            {ok, I} = eleveldb:iterator(Ref, []),
            ?assertEqual({ok, <<"b">>, <<"x">>},eleveldb:iterator_move(I, <<"b">>)),
            ?assertEqual({ok, <<"c">>, <<"y">>},eleveldb:iterator_move(I, next)),
            ?assertEqual({ok, <<"a">>, <<"w">>},eleveldb:iterator_move(I, <<"a">>)),
            ?assertEqual({ok, <<"b">>, <<"x">>},eleveldb:iterator_move(I, next)),
            ?assertEqual({ok, <<"c">>, <<"y">>},eleveldb:iterator_move(I, next))
    end.

basic_prefetch_test_case(Ref) ->
    fun() ->
            {ok, I} = eleveldb:iterator(Ref, []),
            ?assertEqual({ok, <<"a">>, <<"w">>},eleveldb:iterator_move(I, <<>>)),
            ?assertEqual({ok, <<"b">>, <<"x">>},eleveldb:iterator_move(I, prefetch)),
            ?assertEqual({ok, <<"c">>, <<"y">>},eleveldb:iterator_move(I, prefetch)),
            ?assertEqual({ok, <<"d">>, <<"z">>},eleveldb:iterator_move(I, prefetch))
    end.

seek_and_prefetch_test_case(Ref) ->
    fun() ->
            {ok, I} = eleveldb:iterator(Ref, []),
            ?assertEqual({ok, <<"b">>, <<"x">>},eleveldb:iterator_move(I, <<"b">>)),
            ?assertEqual({ok, <<"c">>, <<"y">>},eleveldb:iterator_move(I, prefetch)),
            ?assertEqual({ok, <<"d">>, <<"z">>},eleveldb:iterator_move(I, prefetch_stop)),
            ?assertEqual({ok, <<"a">>, <<"w">>},eleveldb:iterator_move(I, <<"a">>)),
            ?assertEqual({ok, <<"b">>, <<"x">>},eleveldb:iterator_move(I, prefetch)),
            ?assertEqual({ok, <<"c">>, <<"y">>},eleveldb:iterator_move(I, prefetch_stop)),
            ?assertEqual({ok, <<"a">>, <<"w">>},eleveldb:iterator_move(I, <<"a">>)),
            ?assertEqual({ok, <<"b">>, <<"x">>},eleveldb:iterator_move(I, prefetch_stop)),
            ?assertEqual({ok, <<"c">>, <<"y">>},eleveldb:iterator_move(I, prefetch_stop)),
            ?assertEqual({ok, <<"d">>, <<"z">>},eleveldb:iterator_move(I, prefetch_stop)),
            ?assertEqual({ok, <<"a">>, <<"w">>},eleveldb:iterator_move(I, <<"a">>)),
            ?assertEqual({ok, <<"b">>, <<"x">>},eleveldb:iterator_move(I, prefetch)),
            ?assertEqual({ok, <<"c">>, <<"y">>},eleveldb:iterator_move(I, prefetch_stop)),
            ?assertEqual({ok, <<"d">>, <<"z">>},eleveldb:iterator_move(I, prefetch))
    end.

-endif.
