%% -------------------------------------------------------------------
%%
%%  eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
%%
%% Copyright (c) 2010 Basho Technologies, Inc. All Rights Reserved.
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

%% Test various scenarios that properly and improperly close LevelDB DB/iterator
%% handles and ensure everything cleans up properly.

-module(cleanup).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%% Purposely reopen an already opened database to test failure assumption
assumption_test() ->
    DB = open(),
    failed_open(),
    eleveldb:close(DB).

%% Open/close
open_close_test() ->
    DB = open(),
    eleveldb:close(DB),
    check().

%% Open w/o close
open_exit_test() ->
    spawn_wait(fun() ->
                       DB = open()
               end),
    check().

%% Iterator open/close
iterator_test() ->
    DB = open(),
    write(100, DB),
    {ok, Itr} = eleveldb:iterator(DB, []),
    iterate(Itr),
    eleveldb:iterator_close(Itr),
    eleveldb:close(DB),
    check(),
    ok.

%% Close DB while iterator running
%% Expected: reopen should fail while iterator reference alive
%%           however, iterator should fail after DB is closed
%%           once iterator process exits, open should succeed
iterator_db_close_test() ->
    DB = open(),
    write(100, DB),
    Parent = self(),
    spawn_monitor(fun() ->
                          {ok, Itr} = eleveldb:iterator(DB, []),
                          Parent ! continue,
                          try
                              iterate(Itr, 10)
                          catch
                              error:badarg ->
                                  ok
                          end,
                          eleveldb:iterator_close(Itr)
                  end),
    receive continue -> ok end,
    eleveldb:close(DB),
    failed_open(),
    wait_down(),
    check(),
    ok.

%% Iterate open, iterator process exit w/o close
iterator_exit_test() ->
    DB = open(),
    write(100, DB),
    spawn_wait(fun() ->
                       {ok, Itr} = eleveldb:iterator(DB, []),
                       iterate(Itr)
               end),
    eleveldb:close(DB),
    check(),
    ok.

spawn_wait(F) ->
    spawn_monitor(F),
    wait_down().

wait_down() ->
    receive {'DOWN', _, process, _, _} ->
            ok
    end.

check() ->
    DB = open(),
    eleveldb:close(DB).

open() ->
    {ok, Ref} = eleveldb:open("/tmp/eleveldb.cleanup.test",
                              [{create_if_missing, true}]),
    Ref.

failed_open() ->
    {error, {db_open, _}} = eleveldb:open("/tmp/eleveldb.cleanup.test",
                                          [{create_if_missing, true}]).

write(N, DB) ->
    write(0, N, DB).
write(Same, Same, DB) ->
    ok;
write(N, End, DB) ->
    eleveldb:put(DB, <<N:64/integer>>, <<N:64/integer>>, []),
    write(N+1, End, DB).

iterate(Itr) ->
    iterate(Itr, 0).
iterate(Itr, Delay) ->
    do_iterate(eleveldb:iterator_move(Itr, <<0:64/integer>>), {Itr, 0, Delay}).

do_iterate({error, invalid_iterator}, _) ->
    ok;
do_iterate({ok, K, _V}, {Itr, Expected, Delay}) ->
    <<N:64/integer>> = K,
    ?assertEqual(Expected, N),
    (Delay == 0) orelse timer:sleep(Delay),
    do_iterate(eleveldb:iterator_move(Itr, next),
               {Itr, Expected + 1, Delay}).
