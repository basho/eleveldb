%% -------------------------------------------------------------------
%%
%% Copyright (c) 2010-2017 Basho Technologies, Inc.
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

%% @doc Erlang NIF wrapper for LevelDB
-module(eleveldb).

-export([open/2,
         close/1,
         get/3,
         put/4,
         async_put/5,
         delete/3,
         write/3,
         fold/4,
         fold_keys/4,
         status/2,
         destroy/2,
         repair/2,
         is_empty/1]).

-export([option_types/1,
         validate_options/2]).

-export([iterator/2,
         iterator/3,
         iterator_move/2,
         iterator_close/1]).

-export([property_cache/2,
         property_cache_get/1,
         property_cache_flush/0,
         set_metadata_pid/2,
         remove_metadata_pid/2,
         get_metadata_pid/1]).

-export_type([db_ref/0,
              itr_ref/0]).

-on_load(init/0).

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P), eqc:on_output(fun terminal_format/2, P)).
-endif. % EQC
-include_lib("eunit/include/eunit.hrl").

%% Maximum number of distinct database instances to create in any test.
%% The highest runtime limit used is the lower of this value or
%%  ((num-schedulers x 4) + 1).
%% This limit is driven by filesystem size constraints on builders - MvM's
%% trials have shown this value to work on the majority of builders the
%% majority of the time.
-define(MAX_TEST_OPEN, 21).
-endif. % TEST

%% This cannot be a separate function. Code must be inline to trigger
%% Erlang compiler's use of optimized selective receive.
-define(WAIT_FOR_REPLY(Ref),
        receive {Ref, Reply} ->
                Reply
        end).

-define(COMPRESSION_ENUM, [snappy, lz4, false]).
-define(EXPIRY_ENUM, [unlimited, integer]).

-spec init() -> ok | {error, any()}.
init() ->
    SoName = case code:priv_dir(?MODULE) of
                 {error, bad_name} ->
                     case code:which(?MODULE) of
                         Filename when is_list(Filename) ->
                             filename:join([filename:dirname(Filename),"../priv", "eleveldb"]);
                         _ ->
                             filename:join("../priv", "eleveldb")
                     end;
                 Dir ->
                     filename:join(Dir, "eleveldb")
             end,
    erlang:load_nif(SoName, application:get_all_env(eleveldb)).

-type compression_algorithm() :: snappy | lz4 | false.
-type open_options() :: [{create_if_missing, boolean()} |
                         {error_if_exists, boolean()} |
                         {write_buffer_size, pos_integer()} |
                         {block_size, pos_integer()} |                  %% DEPRECATED
                         {sst_block_size, pos_integer()} |
                         {block_restart_interval, pos_integer()} |
                         {block_size_steps, pos_integer()} |
                         {paranoid_checks, boolean()} |
                         {verify_compactions, boolean()} |
                         {compression, [compression_algorithm()]} |
                         {use_bloomfilter, boolean() | pos_integer()} |
                         {total_memory, pos_integer()} |
                         {total_leveldb_mem, pos_integer()} |
                         {total_leveldb_mem_percent, pos_integer()} |
                         {is_internal_db, boolean()} |
                         {limited_developer_mem, boolean()} |
                         {eleveldb_threads, pos_integer()} |
                         {fadvise_willneed, boolean()} |
                         {block_cache_threshold, pos_integer()} |
                         {delete_threshold, pos_integer()} |
                         {tiered_slow_level, pos_integer()} |
                         {tiered_fast_prefix, string()} |
                         {tiered_slow_prefix, string()} |
                         {antidote, boolean()} |
                         {cache_object_warming, boolean()} |
                         {expiry_enabled, boolean()} |
                         {expiry_minutes, pos_integer()} |
                         {whole_file_expiry, boolean()}
                        ].

-type read_option() :: {verify_checksums, boolean()} |
                       {fill_cache, boolean()} |
                       {iterator_refresh, boolean()}.

-type read_options() :: [read_option()].

-type fold_option()  :: {first_key, Key::binary()}.
-type fold_options() :: [read_option() | fold_option()].

-type write_options() :: [{sync, boolean()}].

-type write_actions() :: [{put, Key::binary(), Value::binary()} |
                          {delete, Key::binary()} |
                          clear].

-type iterator_action() :: first | last | next | prev | prefetch | prefetch_stop | binary().

-opaque db_ref() :: binary().

-opaque itr_ref() :: binary().

-spec async_open(reference(), string(), open_options()) -> ok.
async_open(_CallerRef, _Name, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec open(string(), open_options()) -> {ok, db_ref()} | {error, any()}.
open(Name, Opts) ->
    CallerRef = make_ref(),
    Opts2 = add_open_defaults(Opts),
    async_open(CallerRef, Name, Opts2),
    ?WAIT_FOR_REPLY(CallerRef).

-spec close(db_ref()) -> ok | {error, any()}.
close(Ref) ->
    CallerRef = make_ref(),
    async_close(CallerRef, Ref),
    ?WAIT_FOR_REPLY(CallerRef).

async_close(_CallerRef, _Ref) ->
    erlang:nif_error({error, not_loaded}).

-spec async_get(reference(), db_ref(), binary(), read_options()) -> ok.
async_get(_CallerRef, _Dbh, _Key, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec get(db_ref(), binary(), read_options()) -> {ok, binary()} | not_found | {error, any()}.
get(Dbh, Key, Opts) ->
    CallerRef = make_ref(),
    async_get(CallerRef, Dbh, Key, Opts),
    ?WAIT_FOR_REPLY(CallerRef).

-spec put(db_ref(), binary(), binary(), write_options()) -> ok | {error, any()}.
put(Ref, Key, Value, Opts) -> write(Ref, [{put, Key, Value}], Opts).

-spec delete(db_ref(), binary(), write_options()) -> ok | {error, any()}.
delete(Ref, Key, Opts) -> write(Ref, [{delete, Key}], Opts).

-spec write(db_ref(), write_actions(), write_options()) -> ok | {error, any()}.
write(Ref, Updates, Opts) ->
    CallerRef = make_ref(),
    async_write(CallerRef, Ref, Updates, Opts),
    ?WAIT_FOR_REPLY(CallerRef).

-spec async_put(db_ref(), reference(), binary(), binary(), write_options()) -> ok.
async_put(Ref, Context, Key, Value, Opts) ->
    Updates = [{put, Key, Value}],
    async_write(Context, Ref, Updates, Opts),
    ok.

-spec async_write(reference(), db_ref(), write_actions(), write_options()) -> ok.
async_write(_CallerRef, _Ref, _Updates, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec async_iterator(reference(), db_ref(), read_options()) -> ok.
async_iterator(_CallerRef, _Ref, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec async_iterator(reference(), db_ref(), read_options(), keys_only) -> ok.
async_iterator(_CallerRef, _Ref, _Opts, keys_only) ->
    erlang:nif_error({error, not_loaded}).

-spec iterator(db_ref(), read_options()) -> {ok, itr_ref()}.
iterator(Ref, Opts) ->
    CallerRef = make_ref(),
    async_iterator(CallerRef, Ref, Opts),
    ?WAIT_FOR_REPLY(CallerRef).

-spec iterator(db_ref(), read_options(), keys_only) -> {ok, itr_ref()}.
iterator(Ref, Opts, keys_only) ->
    CallerRef = make_ref(),
    async_iterator(CallerRef, Ref, Opts, keys_only),
    ?WAIT_FOR_REPLY(CallerRef).

-spec async_iterator_move(reference()|undefined, itr_ref(), iterator_action()) -> reference() |
                                                                        {ok, Key::binary(), Value::binary()} |
                                                                        {ok, Key::binary()} |
                                                                        {error, invalid_iterator} |
                                                                        {error, iterator_closed}.
async_iterator_move(_CallerRef, _IterRef, _IterAction) ->
    erlang:nif_error({error, not_loaded}).

-spec iterator_move(itr_ref(), iterator_action()) -> {ok, Key::binary(), Value::binary()} |
                                                     {ok, Key::binary()} |
                                                     {error, invalid_iterator} |
                                                     {error, iterator_closed}.
iterator_move(_IRef, _Loc) ->
    case async_iterator_move(undefined, _IRef, _Loc) of
    Ref when is_reference(Ref) ->
        receive
            {Ref, X}                    -> X
        end;
    {ok, _}=Key -> Key;
    {ok, _, _}=KeyVal -> KeyVal;
    ER -> ER
    end.

-spec iterator_close(itr_ref()) -> ok.
iterator_close(IRef) ->
    CallerRef = make_ref(),
    async_iterator_close(CallerRef, IRef),
    ?WAIT_FOR_REPLY(CallerRef).

async_iterator_close(_CallerRef, _IRef) ->
    erlang:nif_error({error, not_loaded}).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

%% Fold over the keys and values in the database
%% will throw an exception if the database is closed while the fold runs
-spec fold(db_ref(), fold_fun(), any(), fold_options()) -> any().
fold(Ref, Fun, Acc0, Opts) ->
    {ok, Itr} = iterator(Ref, Opts),
    do_fold(Itr, Fun, Acc0, Opts).

-type fold_keys_fun() :: fun((Key::binary(), any()) -> any()).

%% Fold over the keys in the database
%% will throw an exception if the database is closed while the fold runs
-spec fold_keys(db_ref(), fold_keys_fun(), any(), read_options()) -> any().
fold_keys(Ref, Fun, Acc0, Opts) ->
    {ok, Itr} = iterator(Ref, Opts, keys_only),
    do_fold(Itr, Fun, Acc0, Opts).

-spec status(db_ref(), Key::binary()) -> {ok, binary()} | error.
status(Ref, Key) ->
    eleveldb_bump:small(),
    status_int(Ref, Key).

status_int(_Ref, _Key) ->
    erlang:nif_error({error, not_loaded}).

-spec async_destroy(reference(), string(), open_options()) -> ok.
async_destroy(_CallerRef, _Name, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec destroy(string(), open_options()) -> ok | {error, any()}.
destroy(Name, Opts) ->
    CallerRef = make_ref(),
    Opts2 = add_open_defaults(Opts),
    async_destroy(CallerRef, Name, Opts2),
    ?WAIT_FOR_REPLY(CallerRef).

repair(Name, Opts) ->
    eleveldb_bump:big(),
    repair_int(Name, Opts).

repair_int(_Name, _Opts) ->
    erlang:nif_error({erlang, not_loaded}).

-spec is_empty(db_ref()) -> boolean().
is_empty(Ref) ->
    eleveldb_bump:big(),
    is_empty_int(Ref).

is_empty_int(_Ref) ->
    erlang:nif_error({error, not_loaded}).

-spec option_types(open | read | write) -> [{atom(), bool | integer | [compression_algorithm()] | any}].
option_types(open) ->
    [{create_if_missing, bool},
     {error_if_exists, bool},
     {write_buffer_size, integer},
     {block_size, integer},                            %% DEPRECATED
     {sst_block_size, integer},
     {block_restart_interval, integer},
     {block_size_steps, integer},
     {paranoid_checks, bool},
     {verify_compactions, bool},
     {compression, ?COMPRESSION_ENUM},
     {use_bloomfilter, any},
     {total_memory, integer},
     {total_leveldb_mem, integer},
     {total_leveldb_mem_percent, integer},
     {is_internal_db, bool},
     {limited_developer_mem, bool},
     {eleveldb_threads, integer},
     {fadvise_willneed, bool},
     {block_cache_threshold, integer},
     {delete_threshold, integer},
     {tiered_slow_level, integer},
     {tiered_fast_prefix, any},
     {tiered_slow_prefix, any},
     {cache_object_warming, bool},
     {expiry_enabled, bool},
     {expiry_minutes, ?EXPIRY_ENUM},
     {whole_file_expiry, bool}];

option_types(read) ->
    [{verify_checksums, bool},
     {fill_cache, bool},
     {iterator_refresh, bool}];
option_types(write) ->
     [{sync, bool}].

-spec validate_options(open | read | write, [{atom(), any()}]) ->
                              {[{atom(), any()}], [{atom(), any()}]}.
validate_options(Type, Opts) ->
    Types = option_types(Type),
    lists:partition(fun({K, V}) ->
                            KType = lists:keyfind(K, 1, Types),
                            validate_type(KType, V)
                    end, Opts).

-spec property_cache(string(), string()) -> ok.
property_cache(_BucketKey, _Properties) ->
    erlang:nif_error({error, not_loaded}).

-spec property_cache_get(string()) -> badarg | einval | [{atom(), any()}].
property_cache_get(_BucketKey) ->
    erlang:nif_error({error, not_loaded}).

%% do NOT use property_cache_flush in production ...
%%   it's a segfault waiting to happen
-spec property_cache_flush() -> ok.
property_cache_flush() ->
    erlang:nif_error({error, not_loaded}).

-spec set_metadata_pid(atom(),pid()) -> ok | {error, any()}.
set_metadata_pid(_Context, _Pid) ->
    erlang:nif_error({error, not_loaded}).

-spec remove_metadata_pid(atom(),pid()) -> ok | {error, any()}.
remove_metadata_pid(_Context, _Pid) ->
    erlang:nif_error({error, not_loaded}).

-spec get_metadata_pid(atom()) -> badarg | einval | pid().
get_metadata_pid(_Context) ->
    erlang:nif_error({error, not_loaded}).


%% ===================================================================
%% Internal functions
%% ===================================================================
%% @doc Appends default open arguments that are better figured out
%% in the Erlang side of things. Most get a default down in leveldb
%% code. Currently only system total memory reported by memsup,
%% if available.
add_open_defaults(Opts) ->
    case not proplists:is_defined(total_memory, Opts)
        andalso is_pid(whereis(memsup)) of
        true ->
            case proplists:get_value(system_total_memory,
                                     memsup:get_system_memory_data(),
                                     undefined) of
                N when is_integer(N) ->
                    [{total_memory, N}|Opts];
                _ ->
                    Opts
            end;
        false ->
            Opts
    end.


do_fold(Itr, Fun, Acc0, Opts) ->
    try
        %% Extract {first_key, binary()} and seek to that key as a starting
        %% point for the iteration. The folding function should use throw if it
        %% wishes to terminate before the end of the fold.
        Start = proplists:get_value(first_key, Opts, first),
        true = is_binary(Start) or (Start == first),
        fold_loop(iterator_move(Itr, Start), Itr, Fun, Acc0)
    after
        %% This clause shouldn't change the operation's result.
        %% If the iterator has been invalidated by it or the db being closed,
        %% the try clause above will raise an exception, and that's the one we
        %% want to propagate. Catch the exception this raises in that case and
        %% ignore it so we don't obscure the original.
        catch iterator_close(Itr)
    end.

fold_loop({error, iterator_closed}, _Itr, _Fun, Acc0) ->
    throw({iterator_closed, Acc0});
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0) ->
    Acc0;
fold_loop({ok, K}, Itr, Fun, Acc0) ->
    Acc = Fun(K, Acc0),
    fold_loop(iterator_move(Itr, prefetch), Itr, Fun, Acc);
fold_loop({ok, K, V}, Itr, Fun, Acc0) ->
    Acc = Fun({K, V}, Acc0),
    fold_loop(iterator_move(Itr, prefetch), Itr, Fun, Acc).

validate_type({_Key, bool}, true)                                  -> true;
validate_type({_Key, bool}, false)                                 -> true;
validate_type({_Key, integer}, Value) when is_integer(Value)       -> true;
validate_type({_Key, any}, _Value)                                 -> true;
validate_type({_Key, ?COMPRESSION_ENUM}, snappy)                   -> true;
validate_type({_Key, ?COMPRESSION_ENUM}, lz4)                      -> true;
validate_type({_Key, ?COMPRESSION_ENUM}, false)                    -> true;
validate_type({_Key, ?EXPIRY_ENUM}, unlimited)                     -> true;
validate_type({_Key, ?EXPIRY_ENUM}, Value) when is_integer(Value)  -> true;
validate_type(_, _)                                                -> false.


%% ===================================================================
%% Tests
%% ===================================================================
-ifdef(TEST).

%% ===================================================================
%% Exported Test Helpers
%% ===================================================================

-spec assert_close(DbRef :: db_ref()) -> ok | no_return().
%%
%% Closes DbRef inside an ?assert... macro.
%%
assert_close(DbRef) ->
    ?assertEqual(ok, ?MODULE:close(DbRef)).

-spec assert_open(DbPath :: string()) -> db_ref() | no_return().
%%
%% Opens Path inside an ?assert... macro, creating the database directory if needed.
%%
assert_open(DbPath) ->
    assert_open(DbPath, [{create_if_missing, true}]).

-spec assert_open(DbPath :: string(), OpenOpts :: open_options())
            -> db_ref() | no_return().
%%
%% Opens DbPath, with OpenOpts, inside an ?assert... macro.
%%
assert_open(DbPath, OpenOpts) ->
    OpenRet = ?MODULE:open(DbPath, OpenOpts),
    ?assertMatch({ok, _}, OpenRet),
    {_, DbRef} = OpenRet,
    DbRef.

-spec assert_open_small(DbPath :: string()) -> db_ref() | no_return().
%%
%% Opens Path inside an ?assert... macro, using a limited storage footprint
%% and creating the database directory if needed.
%%
assert_open_small(DbPath) ->
    assert_open(DbPath, [{create_if_missing, true}, {limited_developer_mem, true}]).

-spec create_test_dir() -> string() | no_return().
%%
%% Creates a new, empty, uniquely-named directory for testing and returns
%% its full path. This operation *should* never fail, but would raise an
%% ?assert...-ish exception if it did.
%%
create_test_dir() ->
    string:strip(?cmd("mktemp -d /tmp/" ?MODULE_STRING ".XXXXXXX"), both, $\n).

-spec delete_test_dir(Dir :: string()) -> ok | no_return().
%%
%% Deletes a test directory fully, whether or not it exists.
%% This operation *should* never fail, but would raise an ?assert...-ish
%% exception if it did.
%%
delete_test_dir(Dir) ->
    ?assertCmd("rm -rf " ++ Dir).

-spec terminal_format(Fmt :: io:format(), Args :: list()) -> ok.
%%
%% Writes directly to the terminal, bypassing EUnit hooks.
%%
terminal_format(Fmt, Args) ->
    io:format(user, Fmt, Args).

%% ===================================================================
%% EUnit Tests
%% ===================================================================

-define(local_test(Timeout, TestFunc),
    fun(TestRoot) ->
        Title = erlang:atom_to_list(TestFunc),
        TestDir = filename:join(TestRoot, TestFunc),
        {Title, {timeout, Timeout, fun() -> TestFunc(TestDir) end}}
    end
).
-define(local_test(TestFunc), ?local_test(10, TestFunc)).
-define(max_test_open(Calc),  erlang:min(?MAX_TEST_OPEN, Calc)).

eleveldb_test_() ->
    {foreach,
        fun create_test_dir/0,
        fun delete_test_dir/1,
        [
            ?local_test(test_open),
            ?local_test(test_close),
            ?local_test(test_destroy),
            ?local_test(test_fold),
            ?local_test(test_fold_keys),
            ?local_test(test_fold_from_key),
            ?local_test(test_close_fold),
            % On weak machines the following can take a while, so we tweak
            % them a bit to avoid timeouts. On anything resembling a competent
            % computer, these should complete in a small fraction of a second,
            % but on some lightweight VMs used for validation, that can be
            % extended by orders of magnitude.
            ?local_test(15, test_compression),
            fun(TestRoot) ->
                TestName = "test_open_many",
                TestDir = filename:join(TestRoot, TestName),
                Count = ?max_test_open(erlang:system_info(schedulers) * 4 + 1),
                Title = lists:flatten(io_lib:format("~s(~b)", [TestName, Count])),
                {Title, {timeout, 30, fun() -> test_open_many(TestDir, Count) end}}
            end
        ]
    }.

%% fold accumulator used in a few tests
accumulate(Val, Acc) ->
    [Val | Acc].

%%
%% Individual tests
%%

test_open(TestDir) ->
    Ref = assert_open(TestDir),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"abc">>, <<"123">>, [])),
    ?assertEqual({ok, <<"123">>}, ?MODULE:get(Ref, <<"abc">>, [])),
    ?assertEqual(not_found, ?MODULE:get(Ref, <<"def">>, [])),
    assert_close(Ref).

test_open_many(TestDir, HowMany) ->
    Insts   = lists:seq(1, HowMany),
    KNonce  = erlang:make_ref(),
    VNonce  = erlang:self(),
    WorkSet = [
        begin
            D = lists:flatten(io_lib:format("~s.~b", [TestDir, N])),
            T = os:timestamp(),
            K = erlang:phash2([T, N, KNonce], 1 bsl 32),
            V = erlang:phash2([N, T, VNonce], 1 bsl 32),
            {assert_open_small(D),
                <<K:32/unsigned>>, <<V:32/unsigned, 0:64, K:32/unsigned>>}
        end || N <- Insts],
    lists:foreach(
        fun({Ref, Key, Val}) ->
            ?assertEqual(ok, ?MODULE:put(Ref, Key, Val, []))
        end, WorkSet),
    lists:foreach(
        fun({Ref, Key, Val}) ->
            ?assertEqual({ok, Val}, ?MODULE:get(Ref, Key, []))
        end, WorkSet),
    lists:foreach(fun assert_close/1, [R || {R, _, _} <- WorkSet]).

test_close(TestDir) ->
    Ref = assert_open(TestDir, [{create_if_missing, true}]),
    assert_close(Ref),
    ?assertError(badarg, ?MODULE:close(Ref)).

test_fold(TestDir) ->
    Ref = assert_open(TestDir),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"def">>, <<"456">>, [])),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"abc">>, <<"123">>, [])),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"hij">>, <<"789">>, [])),
    ?assertEqual(
        [{<<"abc">>, <<"123">>}, {<<"def">>, <<"456">>}, {<<"hij">>, <<"789">>}],
        lists:reverse(?MODULE:fold(Ref, fun accumulate/2, [], []))),
    assert_close(Ref).

test_fold_keys(TestDir) ->
    Ref = assert_open(TestDir),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"def">>, <<"456">>, [])),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"abc">>, <<"123">>, [])),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"hij">>, <<"789">>, [])),
    ?assertEqual(
        [<<"abc">>, <<"def">>, <<"hij">>],
        lists:reverse(?MODULE:fold_keys(Ref, fun accumulate/2, [], []))),
    assert_close(Ref).

test_fold_from_key(TestDir) ->
    Ref = assert_open(TestDir),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"def">>, <<"456">>, [])),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"abc">>, <<"123">>, [])),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"hij">>, <<"789">>, [])),
    ?assertEqual([<<"def">>, <<"hij">>], lists:reverse(
        ?MODULE:fold_keys(Ref, fun accumulate/2, [], [{first_key, <<"d">>}]))),
    assert_close(Ref).

test_destroy(TestDir) ->
    Ref = assert_open(TestDir),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"def">>, <<"456">>, [])),
    ?assertEqual({ok, <<"456">>}, ?MODULE:get(Ref, <<"def">>, [])),
    assert_close(Ref),
    ?assertEqual(ok, ?MODULE:destroy(TestDir, [])),
    ?assertMatch({error, {db_open, _}}, ?MODULE:open(TestDir, [{error_if_exists, true}])).

test_compression(TestDir) ->
    IntSeq = lists:seq(1, 10),
    CompressibleData = list_to_binary(lists:duplicate(20, 0)),

    Ref0 = assert_open(TestDir ++ ".0", [
        {write_buffer_size, 5}, {create_if_missing, true}, {compression, false}]),
    lists:foreach(
        fun(I) ->
            ?assertEqual(ok,
                ?MODULE:put(Ref0, <<I:64/unsigned>>, CompressibleData, [{sync, true}]))
        end, IntSeq),

    Ref1 = assert_open(TestDir ++ ".1", [
        {write_buffer_size, 5}, {create_if_missing, true}, {compression, true}]),
    lists:foreach(
        fun(I) ->
            ?assertEqual(ok,
                ?MODULE:put(Ref1, <<I:64/unsigned>>, CompressibleData, [{sync, true}]))
        end, IntSeq),

    %% Check both of the LOG files created to see if the compression option was
    %% passed down correctly
    lists:foreach(
        fun(Val) ->
            File = filename:join(TestDir ++ [$. | Val], "LOG"),
            RRet = file:read_file(File),
            ?assertMatch({ok, _}, RRet),
            {_, Data} = RRet,
            Pattern = "Options.compression: " ++ Val,
            ?assertMatch({match, _}, re:run(Data, Pattern))
        end, ["0", "1"]),
    assert_close(Ref0),
    assert_close(Ref1).

test_close_fold(TestDir) ->
    Ref = assert_open(TestDir),
    ?assertEqual(ok, ?MODULE:put(Ref, <<"k">>,<<"v">>,[])),
    ?assertError(badarg,
        ?MODULE:fold(Ref, fun(_,_) -> assert_close(Ref) end, undefined, [])).

%%
%% Parallel tests
%%

parallel_test_() ->
    ParaCnt = ?max_test_open(erlang:system_info(schedulers) * 2 + 1),
    LoadCnt = 99,
    TestSeq = lists:seq(1, ParaCnt),
    {foreach,
        fun create_test_dir/0,
        fun delete_test_dir/1,
        [fun(TestRoot) ->
            {inparallel, [begin
                T = lists:flatten(io_lib:format("load proc ~b", [N])),
                D = filename:join(TestRoot, io_lib:format("parallel_test.~b", [N])),
                S = lists:seq(N, (N + LoadCnt - 1)),
                {T, fun() -> run_load(D, S) end}
            end || N <- TestSeq]}
        end]
    }.

run_load(TestDir, IntSeq) ->
    KNonce  = [os:timestamp(), erlang:self()],
    Ref     = assert_open_small(TestDir),
    VNonce  = [erlang:make_ref(), os:timestamp()],
    KVIn    = [
        begin
            K = erlang:phash2([N | KNonce], 1 bsl 32),
            V = erlang:phash2([N | VNonce], 1 bsl 32),
            {<<K:32/unsigned>>, <<V:32/unsigned, 0:64, K:32/unsigned>>}
        end || N <- IntSeq],
    lists:foreach(
        fun({Key, Val}) ->
            ?assertEqual(ok, ?MODULE:put(Ref, Key, Val, []))
        end, KVIn),
    {L, R}  = lists:split(erlang:hd(IntSeq), KVIn),
    KVOut   = R ++ L,
    lists:foreach(
        fun({Key, Val}) ->
            ?assertEqual({ok, Val}, ?MODULE:get(Ref, Key, []))
        end, KVOut),
    assert_close(Ref).

%% ===================================================================
%% QuickCheck Tests
%% ===================================================================

-ifdef(EQC).

qc(P) ->
    ?assert(eqc:quickcheck(?QC_OUT(P))).

keys() ->
    eqc_gen:non_empty(list(eqc_gen:non_empty(binary()))).

values() ->
    eqc_gen:non_empty(list(binary())).

ops(Keys, Values) ->
    {oneof([put, async_put, delete]), oneof(Keys), oneof(Values)}.

apply_kv_ops([], _Ref, Acc0) ->
    Acc0;
apply_kv_ops([{put, K, V} | Rest], Ref, Acc0) ->
    ?assertEqual(ok, ?MODULE:put(Ref, K, V, [])),
    apply_kv_ops(Rest, Ref, orddict:store(K, V, Acc0));
apply_kv_ops([{async_put, K, V} | Rest], Ref, Acc0) ->
    MyRef = make_ref(),
    Context = {my_context, MyRef},
    ?assertEqual(ok, ?MODULE:async_put(Ref, Context, K, V, [])),
    receive
        {Context, ok} ->
            apply_kv_ops(Rest, Ref, orddict:store(K, V, Acc0));
        Msg ->
            erlang:error({unexpected_msg, Msg})
    end;
apply_kv_ops([{delete, K, _} | Rest], Ref, Acc0) ->
    ?assertEqual(ok, ?MODULE:delete(Ref, K, [])),
    apply_kv_ops(Rest, Ref, orddict:store(K, deleted, Acc0)).

prop_put_delete(TestDir) ->
    ?LET({Keys, Values}, {keys(), values()},
        ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
            begin
                delete_test_dir(TestDir),
                Ref = assert_open(TestDir, [{create_if_missing, true}]),
                Model = apply_kv_ops(Ops, Ref, []),

                %% Validate that all deleted values return not_found
                lists:foreach(
                    fun({K, deleted}) ->
                        ?assertEqual(not_found, ?MODULE:get(Ref, K, []));
                    ({K, V}) ->
                        ?assertEqual({ok, V}, ?MODULE:get(Ref, K, []))
                end, Model),

                %% Validate that a fold returns sorted values
                Actual = lists:reverse(
                    ?MODULE:fold(Ref, fun({K, V}, Acc) -> [{K, V} | Acc] end, [], [])),
                ?assertEqual([{K, V} || {K, V} <- Model, V /= deleted], Actual),
                assert_close(Ref),
                true
            end)).

prop_put_delete_test_() ->
    Timeout1 = 10,
    Timeout2 = 15,
    {foreach,
        fun create_test_dir/0,
        fun delete_test_dir/1,
        [
            fun(TestRoot) ->
                TestDir = filename:join(TestRoot, "putdelete.qc"),
                InnerTO = Timeout1,
                OuterTO = (InnerTO * 3),
                Title   = "Without ?ALWAYS()",
                TestFun = fun() ->
                    qc(eqc:testing_time(InnerTO, prop_put_delete(TestDir)))
                end,
                {timeout, OuterTO, {Title, TestFun}}
            end,
            fun(TestRoot) ->
                TestDir = filename:join(TestRoot, "putdelete.qc"),
                InnerTO = Timeout2,
                OuterTO = (InnerTO * 10),
                AwCount = (InnerTO * 9),
                %% We use the ?ALWAYS(AwCount, ...) wrapper as a regression test.
                %% It's not clear how this is effectively different than the first
                %% fixture, but I'm leaving it here in case I'm missing something.
                Title   = lists:flatten(io_lib:format("With ?ALWAYS(~b)", [AwCount])),
                TestFun = fun() ->
                    qc(eqc:testing_time(InnerTO,
                        ?ALWAYS(AwCount, prop_put_delete(TestDir))))
                end,
                {timeout, OuterTO, {Title, TestFun}}
            end
        ]
    }.

-endif. % EQC

-endif. % TEST
