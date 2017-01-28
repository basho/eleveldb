%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2017 Basho Technologies, Inc.
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

-module(cacheleak).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(KV_PAIRS,   (1000 * 10)).
-define(VAL_SIZE,   (1024 * 10)).
-define(MAX_RSS,    (1000 * 500)).  % driven by ?KV_PAIRS and ?VAL_SIZE ?

-define(TEST_LOOPS, 10).
-define(TIMEOUT,    (?TEST_LOOPS * 60)).

cacheleak_test_() ->
    TestRoot = eleveldb:create_test_dir(),
    TestDir = filename:join(TestRoot, ?MODULE),
    {setup,
        fun() -> TestRoot end,
        fun eleveldb:delete_test_dir/1,
        {timeout, ?TIMEOUT, fun() ->
            Bytes = compressible_bytes(?VAL_SIZE),
            Blobs = [{<<I:128/unsigned>>, Bytes} || I <- lists:seq(1, ?KV_PAIRS)],
            eleveldb:terminal_format("RSS limit: ~b\n", [?MAX_RSS]),
            cacheleak_loop(?TEST_LOOPS, Blobs, ?MAX_RSS, TestDir)
        end}}.

%% It's very important for this test that the data is compressible. Otherwise,
%% the file will be mmapped, and nothing will fill up the cache.
compressible_bytes(Count) ->
    erlang:list_to_binary(lists:duplicate(Count, 0)).

cacheleak_loop(0, _Blobs, _MaxFinalRSS, _TestDir) ->
    ok;
cacheleak_loop(Count, Blobs, MaxFinalRSS, TestDir) ->
    %% We spawn a process to open a LevelDB instance and do a series of
    %% reads/writes to fill up the cache. When the process exits, the LevelDB
    %% ref will get GC'd and we can re-evaluate the memory footprint of the
    %% process to make sure everything got cleaned up as expected.
    F = fun() ->
        Ref = eleveldb:assert_open(TestDir,
            [{create_if_missing, true}, {limited_developer_mem, true}]),
        lists:foreach(
            fun({Key, Val}) ->
                ?assertEqual(ok, eleveldb:put(Ref, Key, Val, []))
            end, Blobs),
        ?assertEqual([], eleveldb:fold(Ref,
            fun({_K, _V}, A) -> A end, [], [{fill_cache, true}])),
        lists:foreach(
            fun({Key, Val}) ->
                ?assertEqual({ok, Val}, eleveldb:get(Ref, Key, []))
            end, Blobs),
        eleveldb:assert_close(Ref),
        erlang:garbage_collect(),
        eleveldb:terminal_format("RSS1: ~p\n", [rssmem()])
    end,
    {_Pid, Mref} = spawn_monitor(F),
    receive
        {'DOWN', Mref, process, _, _} ->
            ok
    end,
    RSS = rssmem(),
    ?assert(MaxFinalRSS > RSS),
    cacheleak_loop(Count - 1, Blobs, MaxFinalRSS, TestDir).

rssmem() ->
    Cmd = io_lib:format("ps -o rss= -p ~s", [os:getpid()]),
    % Don't try to use eunit's ?cmd macro here, it won't do the right thing.
    S = string:strip(os:cmd(Cmd), left),  % only matters that the 1st character is $0-$9
    case string:to_integer(S) of
        {error, _} ->
            eleveldb:terminal_format("Error parsing integer in: ~s\n", [S]),
            error;
        {I, _} ->
            I
    end.

-endif. % TEST
