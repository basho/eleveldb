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

%% Test various scenarios that properly and improperly close LevelDB
%% DB/iterator handles and ensure everything cleans up properly.
-module(cleanup).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(local_test(TestFunc),
    fun(TestRoot) ->
        Title = erlang:atom_to_list(TestFunc),
        TestDir = filename:join(TestRoot, TestFunc),
        {Title, fun() -> TestFunc(TestDir) end}
    end
).

cleanup_test_() ->
    {foreach,
        fun eleveldb:create_test_dir/0,
        fun eleveldb:delete_test_dir/1,
        [
            ?local_test(test_open_twice),
            ?local_test(test_open_close),
            ?local_test(test_open_exit),
            ?local_test(test_iterator),
            ?local_test(test_iterator_db_close),
            ?local_test(test_iterator_exit)
        ]
    }.

%% Purposely reopen an already opened database to test failure assumption
test_open_twice(TestDir) ->
    DB = eleveldb:assert_open(TestDir),
    ?assertMatch({error, {db_open, _}},
        eleveldb:open(TestDir, [{create_if_missing, true}])),
    eleveldb:assert_close(DB).

%% Open/close
test_open_close(TestDir) ->
    check_open_close(TestDir),
    check_open_close(TestDir).

%% Open w/o close
test_open_exit(TestDir) ->
    spawn_wait(fun() -> eleveldb:assert_open(TestDir) end),
    check_open_close(TestDir).

%% Iterator open/close
test_iterator(TestDir) ->
    DB = eleveldb:assert_open(TestDir),
    ?assertEqual(ok, write(100, DB)),
    ItrRet = eleveldb:iterator(DB, []),
    ?assertMatch({ok, _}, ItrRet),
    {_, Itr} = ItrRet,
    ?assertEqual(ok, iterate(Itr)),
    ?assertEqual(ok, eleveldb:iterator_close(Itr)),
    eleveldb:assert_close(DB),
    check_open_close(TestDir).

%% Close DB while iterator running
%% Expected: reopen should fail while iterator reference alive
%%           however, iterator should fail after DB is closed
%%           once iterator process exits, open should succeed
test_iterator_db_close(TestDir) ->
    DB = eleveldb:assert_open(TestDir),
    ?assertEqual(ok, write(100, DB)),
    Parent = self(),
    {Pid, Mon} = Proc = erlang:spawn_monitor(
        fun() ->
            {ok, Itr} = eleveldb:iterator(DB, []),
            Parent ! continue,
            try
                iterate(Itr, 10)
            catch
                error:badarg ->
                    ok
            end,
            try
                eleveldb:iterator_close(Itr)
            catch
                error:badarg ->
                    ok
            end
        end),
    ?assertEqual(ok, receive
        continue ->
            ok;
        {'DOWN', Mon, process, Pid, Info} ->
            Info
    end),
    eleveldb:assert_close(DB),
    ?assertEqual(ok, wait_down(Proc)),
    check_open_close(TestDir).

%% Iterate open, iterator process exit w/o close
test_iterator_exit(TestDir) ->
    DB = eleveldb:assert_open(TestDir),
    ?assertEqual(ok, write(100, DB)),
    spawn_wait(fun() ->
        {ok, Itr} = eleveldb:iterator(DB, []),
        iterate(Itr)
    end),
    eleveldb:assert_close(DB),
    check_open_close(TestDir).

spawn_wait(F) ->
    wait_down(erlang:spawn_monitor(F)).

wait_down({Pid, Mon}) when erlang:is_pid(Pid) andalso erlang:is_reference(Mon) ->
    receive
        {'DOWN', Mon, process, Pid, _} ->
            ok
    end;
wait_down(Mon) when erlang:is_reference(Mon) ->
    receive
        {'DOWN', Mon, process, _, _} ->
            ok
    end;
wait_down(Pid) when erlang:is_pid(Pid) ->
    receive
        {'DOWN', _, process, Pid, _} ->
            ok
    end.

check_open_close(TestDir) ->
    eleveldb:assert_close(eleveldb:assert_open(TestDir)).

write(N, DB) ->
    write(0, N, DB).
write(Same, Same, _DB) ->
    ok;
write(N, End, DB) ->
    KV = <<N:64/integer>>,
    ?assertEqual(ok, eleveldb:put(DB, KV, KV, [])),
    write((N + 1), End, DB).

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
    do_iterate(eleveldb:iterator_move(Itr, next), {Itr, (Expected + 1), Delay}).

-endif. % TEST
