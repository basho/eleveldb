%% -------------------------------------------------------------------
%%
%%  e_leveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
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
-module(e_leveldb).

-export([open/2,
         get/3,
         put/4,
         delete/3,
         write/3,
         fold/4,
         destroy/2,
         repair/2]).

-export([iterator/2,
         iterator_move/2,
         iterator_close/1]).

-on_load(init/0).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

init() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            case code:which(?MODULE) of
                Filename when is_list(Filename) ->
                    SoName = filename:join([filename:dirname(Filename),"../priv", ?MODULE]);
                _ ->
                    SoName = filename:join("../priv", ?MODULE)
            end;
        Dir ->
            SoName = filename:join(Dir, ?MODULE)
    end,
    erlang:load_nif(SoName, 0).

%% Open options
%% * {create_if_missing, false}
%% * {error_if_exists, false}
%% * {write_buffer_sz, 4194304}
%% * {max_open_files, 1000}
%% * {block_size, 4096}
%% * {block_restart_interval, 16}

%% Unsupported
%% * {info_log, Filename}
%% * {paranoid_checks, false}
%% * {block_cache, Megabytes} (default is 8MB)

open(Name, Opts) ->
    ok.

get(Ref,Key, Opts) ->
    ok.

put(Ref, Key, Value, Opts) ->
    write(Ref, [{put, Key, Value}], Opts).

delete(Ref, Key, Opts) ->
    write(Ref, [{delete, Key}], Opts).

write(Ref, Updates, Opts) ->
    ok.

iterator(Ref, Opts) ->
    %% [first, {key, Key}, last] -> {ok, IRef}
    ok.

iterator_move(IRef, Loc) ->
    %% Loc = [first, next, prev, last, BinKey]
    %% {ok, K, V} | {error, Reason}
    ok.

iterator_close(IRef) ->
    ok.

fold(Ref, Fun, Acc0, Opts) ->
    case iterator(Ref, Opts) of
        {ok, Itr} ->
            try
                fold_loop(iterator_move(Itr, first), Itr, Fun, Acc0)
            after
                iterator_close(Itr)
            end;
         {error, Reason} ->
            {error, Reason}
    end.

destroy(Name, Opts) ->
    ok.

repair(Name, Opts) ->
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0) ->
    Acc0;
fold_loop({ok, K, V}, Itr, Fun, Acc0) ->
    Acc = Fun({K, V}, Acc0),
    fold_loop(iterator_move(Itr, next), Itr, Fun, Acc).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

open_test() ->
    os:cmd("rm -rf /tmp/eleveldb.open.test"),
    {ok, Ref} = open("/tmp/eleveldb.open.test", [{create_if_missing, true}]),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    {ok, <<"123">>} = ?MODULE:get(Ref, <<"abc">>, []),
    not_found = ?MODULE:get(Ref, <<"def">>, []).

fold_test() ->
    os:cmd("rm -rf /tmp/eleveldb.fold.test"),
    {ok, Ref} = open("/tmp/eleveldb.fold.test", [{create_if_missing, true}]),
    ok = ?MODULE:put(Ref, <<"def">>, <<"456">>, []),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    ok = ?MODULE:put(Ref, <<"hij">>, <<"789">>, []),
    [{<<"abc">>, <<"123">>},
     {<<"def">>, <<"456">>},
     {<<"hij">>, <<"789">>}] = lists:reverse(fold(Ref, fun({K, V}, Acc) -> [{K, V} | Acc] end,
                                                  [], [])).


-endif.
