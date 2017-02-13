%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2017 Basho Technologies, Inc.
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

iterator_test_() ->
    {spawn, [
        {setup,
            fun setup/0,
            fun cleanup/1,
            fun({_, Ref}) -> [
                prev_test_case(Ref),
                seek_and_next_test_case(Ref),
                basic_prefetch_test_case(Ref),
                seek_and_prefetch_test_case(Ref),
                aae_prefetch1(Ref),
                aae_prefetch2(Ref),
                aae_prefetch3(Ref)
            ] end
        }]}.

setup() ->
    Dir = eleveldb:create_test_dir(),
    Ref = eleveldb:assert_open(Dir),
    ?assertEqual(ok, eleveldb:put(Ref, <<"a">>, <<"w">>, [])),
    ?assertEqual(ok, eleveldb:put(Ref, <<"b">>, <<"x">>, [])),
    ?assertEqual(ok, eleveldb:put(Ref, <<"c">>, <<"y">>, [])),
    ?assertEqual(ok, eleveldb:put(Ref, <<"d">>, <<"z">>, [])),
    {Dir, Ref}.

cleanup({Dir, Ref}) ->
    eleveldb:assert_close(Ref),
    eleveldb:delete_test_dir(Dir).

assert_iterator(DbRef, ItrOpts) ->
    ItrRet = eleveldb:iterator(DbRef, ItrOpts),
    ?assertMatch({ok, _}, ItrRet),
    {_, Itr} = ItrRet,
    Itr.

prev_test_case(Ref) ->
    fun() ->
        I = assert_iterator(Ref, []),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, <<>>)),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, next)),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, prev))
    end.

seek_and_next_test_case(Ref) ->
    fun() ->
        I = assert_iterator(Ref, []),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, <<"b">>)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, next)),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, <<"a">>)),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, next)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, next))
    end.

basic_prefetch_test_case(Ref) ->
    fun() ->
        I = assert_iterator(Ref, []),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, <<>>)),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({ok, <<"d">>, <<"z">>}, eleveldb:iterator_move(I, prefetch))
    end.

seek_and_prefetch_test_case(Ref) ->
    fun() ->
        I = assert_iterator(Ref, []),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, <<"b">>)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({ok, <<"d">>, <<"z">>}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, <<"a">>)),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, <<"a">>)),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({ok, <<"d">>, <<"z">>}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, <<"a">>)),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({ok, <<"d">>, <<"z">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, <<"a">>)),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({ok, <<"d">>, <<"z">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(I, prefetch_stop)),
        ?assertEqual({ok, <<"a">>, <<"w">>}, eleveldb:iterator_move(I, <<"a">>))
    end.

aae_prefetch1(Ref) ->
    fun() ->
        I = assert_iterator(Ref, []),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, <<"b">>)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, prefetch_stop)),

        J = assert_iterator(Ref, []),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(J, <<"z">>)),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(J, prefetch_stop))
    end.

aae_prefetch2(Ref) ->
    fun() ->
        I = assert_iterator(Ref, []),
        ?assertEqual({ok, <<"b">>, <<"x">>}, eleveldb:iterator_move(I, <<"b">>)),
        ?assertEqual({ok, <<"c">>, <<"y">>}, eleveldb:iterator_move(I, prefetch)),
        ?assertEqual({ok, <<"d">>, <<"z">>}, eleveldb:iterator_move(I, prefetch_stop)),

        J = assert_iterator(Ref, []),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(J, <<"z">>)),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(J, prefetch)),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(J, prefetch_stop))
    end.

aae_prefetch3(Ref) ->
    fun() ->
        I = assert_iterator(Ref, []),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(I, prefetch_stop))
    end.

-endif. % TEST
