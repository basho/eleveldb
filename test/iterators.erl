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

-include_lib("eunit/include/eunit.hrl").

prev_test() ->
    os:cmd("rm -rf ltest"),  % NOTE
    {ok, Ref} = eleveldb:open("ltest", [{create_if_missing, true}]),
    try
      eleveldb:put(Ref, <<"a">>, <<"x">>, []),
      eleveldb:put(Ref, <<"b">>, <<"y">>, []),
      {ok, I} = eleveldb:iterator(Ref, []),
      io:format("got iter~n"),
        ?assertEqual({ok, [{<<"a">>, <<"x">>}]},eleveldb:iterator_move(I, <<>> , 1)),
        io:format("1~n"),
        ?assertEqual({ok, [{<<"b">>, <<"y">>}]},eleveldb:iterator_move(I, next, 1)),
      io:format("2~n"),
        ?assertEqual({ok, [{<<"a">>, <<"x">>}]},eleveldb:iterator_move(I, prev, 1)),
      io:format("3~n"),
      
      eleveldb:put(Ref, <<"c">>, <<"z">>, []),
      {ok, I2} = eleveldb:iterator(Ref, []),
      io:format("4~n"),
        ?assertEqual({ok, [{<<"a">>, <<"x">>}]},eleveldb:iterator_move(I2, <<>> , 1)),
      io:format("5~n"),
        ?assertEqual({ok, [{<<"b">>, <<"y">>}, {<<"c">>, <<"z">>}]}, eleveldb:iterator_move(I2, next, 2)),
      
      {ok, I3} = eleveldb:iterator(Ref, []),
      io:format("6~n"),
        ?assertEqual({ok, [{<<"a">>, <<"x">>}]},eleveldb:iterator_move(I3, <<>> , 1)),
      io:format("7~n"),
        ?assertEqual({ok, [{<<"b">>, <<"y">>}]}, eleveldb:iterator_move(I3, prefetch, 1)),
        ?assertEqual({ok, [{<<"c">>, <<"z">>}]}, eleveldb:iterator_move(I3, prefetch, 1)),
      
      {ok, I4} = eleveldb:iterator(Ref, []),
      io:format("8~n"),
        ?assertEqual({ok, [{<<"a">>, <<"x">>}]},eleveldb:iterator_move(I4, <<>> , 10)),
      io:format("9~n"),
        ?assertEqual({ok, [{<<"b">>, <<"y">>}, {<<"c">>, <<"z">>}]}, eleveldb:iterator_move(I4, prefetch, 20)),
      io:format("9~n"),
        ?assertEqual({error, invalid_iterator}, eleveldb:iterator_move(I4, prefetch, 2)),
      
      io:format("10~n"),
      ?assertEqual([<<"cz">>, <<"by">>, <<"ax">>], eleveldb:fold(Ref, fun({K,V}, Acc) -> [<<K/binary, V/binary>>|Acc] end, [], [], 2)),
      io:format("11~n"),
      ?assertEqual([<<"cz">>, <<"by">>, <<"ax">>], eleveldb:fold(Ref, fun({K,V}, Acc) -> [<<K/binary, V/binary>>|Acc] end, [], [], 20))
    after
      eleveldb:close(Ref)
    end.
