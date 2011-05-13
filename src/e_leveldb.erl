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
    %% {ok, Ref} | {error, diff_opts} | {error, Reason}
    ok.

get(Ref,Key, Opts) ->
    ok.

put(Ref, Key, Value, Opts) ->
    write(Ref, [{put, Key, Value}], Opts).

delete(Ref, Key, Opts) ->
    write(Ref, [{delete, Key}], Opts).

write(Ref, Updates, Opts) ->
    ok.

fold(Ref, Fun, Acc0, Opts) ->
    %% Opts: [{seek, Key}]
    ok.

destroy(Name, Opts) ->
    ok.

repair(Name, Opts) ->
    ok.

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

-endif.
