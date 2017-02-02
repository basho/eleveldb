%% -------------------------------------------------------------------
%%
%%  eleveldb_metadata: Erlang interface to allow LevelDB to retrieve riak_core metadata
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
-module(eleveldb_metadata).

-export([start_link/0]).

-export([callback_router/3]).

%% Application messages are sent to this process directly from
%% NIF-land instead of other Erlang modules, so this does not
%% implement an OTP behavior.
%%
%% See http://erlang.org/doc/design_principles/spec_proc.html for
%% information on these system message handlers.
-export([system_continue/3, system_terminate/4,
         system_get_state/1, system_replace_state/2,
         system_code_change/4]).

-ifdef(TEST).
-export([start/1]).

start(Fun) ->
    Pid = proc_lib:spawn(?MODULE, callback_router,
                         [Fun, self(), sys:debug_options([])]),
    {ok, Pid}.
-endif.

start_link() ->
    Fun = fun riak_core_bucket:get_bucket/1,
    Pid = proc_lib:spawn_link(?MODULE, callback_router,
                              [Fun, self(), sys:debug_options([])]),

    %% Inform leveldb where to send messages
    eleveldb:property_cache(set_pid, Pid),
    {ok, Pid}.

-spec callback_router(fun(), pid(), term()) -> ok.
callback_router(Fun, Parent, Debug) ->
    receive
        {get_bucket_properties, Name, Key}=Msg ->
            Debug0 = sys:handle_debug(Debug, fun write_debug/3,
                                      ?MODULE,
                                      {in, Msg}),
            Props = Fun(Name),
            eleveldb:property_cache(Key, Props),
            callback_router(Fun, Parent, Debug0);

        {system, From, Request} ->
            %% No recursion here; `system_continue/3' will be invoked
            %% if appropriate
            sys:handle_system_msg(Request, From, Parent,
                                  ?MODULE, Debug, Fun);

        callback_shutdown=Msg ->
            sys:handle_debug(Debug, fun write_debug/3,
                             ?MODULE,
                             {in, Msg}),
            ok;

        Msg ->
            Debug0 = sys:handle_debug(Debug, fun write_debug/3,
                                      ?MODULE,
                                      {in, Msg}),
            callback_router(Fun, Parent, Debug0)
    end.

%% Made visible to `standard_io' via `sys:trace(Pid, true)'
write_debug(Device, Event, Module) ->
    io:format(Device, "~p event: ~p~n", [Module, Event]).

%% System functions invoked by `sys:handle_system_msg'.
system_continue(Parent, Debug, Fun) ->
    callback_router(Fun, Parent, Debug).

system_terminate(Reason, _Parent, _Debug, _State) ->
    exit(Reason).

system_get_state(State) ->
    {ok, State}.

system_replace_state(StateFun, State) ->
    NewState = StateFun(State),
    {ok, NewState, NewState}.

system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.
