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

-spec init() -> ok | {error, any()}.
init() ->
    SoName = case code:priv_dir(?MODULE) of
                 {error, bad_name} ->
                     case code:which(?MODULE) of
                         Filename when is_list(Filename) ->
                             filename:join([filename:dirname(Filename),"../priv", "e_leveldb"]);
                         _ ->
                             filename:join("../priv", "e_leveldb")
                     end;
                 Dir ->
                     filename:join(Dir, "e_leveldb")
             end,
    erlang:load_nif(SoName, 0).

-type open_options() :: [{create_if_missing, boolean()} |
                         {error_if_exists, boolean()} |
                         {write_buffer_size, pos_integer()} |
                         {max_open_files, pos_integer()} |
                         {block_size, pos_integer()} |
                         {block_restart_interval, pos_integer()}].

-type read_options() :: [{verify_checksums, boolean()} |
                         {fill_cache, boolean()}].

-type write_options() :: [{sync, boolean()}].

-type write_actions() :: [{put, Key::binary(), Value::binary()} |
                          {delete, Key::binary()} |
                          clear].

-type iterator_action() :: first | last | next | prev | binary().

-opaque db_ref() :: binary().

-opaque itr_ref() :: binary().

-spec open(string(), open_options()) -> {ok, db_ref()} | {error, any()}.
open(_Name, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec get(db_ref(), binary(), read_options()) -> {ok, binary()} | not_found | {error, any()}.
get(_Ref, _Key, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec put(db_ref(), binary(), binary(), write_options()) -> ok | {error, any()}.
put(Ref, Key, Value, Opts) ->
    write(Ref, [{put, Key, Value}], Opts).

-spec delete(db_ref(), binary(), write_options()) -> ok | {error, any()}.
delete(Ref, Key, Opts) ->
    write(Ref, [{delete, Key}], Opts).

-spec write(db_ref(), write_actions(), write_options()) -> ok | {error, any()}.
write(_Ref, _Updates, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec iterator(db_ref(), read_options()) -> {ok, itr_ref()}.
iterator(_Ref, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec iterator_move(itr_ref(), iterator_action()) -> {ok, Key::binary(), Value::binary()} |
                                                     {error, invalid_iterator} |
                                                     {error, iterator_closed}.
iterator_move(_IRef, _Loc) ->
    erlang:nif_error({error, not_loaded}).


-spec iterator_close(itr_ref()) -> ok.
iterator_close(_IRef) ->
    erlang:nif_error({error, not_loaded}).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

-spec fold(db_ref(), fold_fun(), any(), read_options()) -> any().
fold(Ref, Fun, Acc0, Opts) ->
    {ok, Itr} = iterator(Ref, Opts),
    try
        fold_loop(iterator_move(Itr, first), Itr, Fun, Acc0)
    after
        iterator_close(Itr)
    end.

destroy(_Name, _Opts) ->
    ok.

repair(_Name, _Opts) ->
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
