%% -------------------------------------------------------------------
%%
%%  eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
%%
%% Copyright (c) 2010-2012 Basho Technologies, Inc. All Rights Reserved.
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
-module(eleveldb).

-export([open/2,
         close/1,
         open_family/3,
         close_family/2,
         get/3,
         get/4,
         put/4,
         put/5,
         async_put/5,
         delete/3,
         delete/4,
         write/3,
         write/4,
         fold/4,
         foldtest1/4,
         fold_keys/4,
         status/2,
         destroy/2,
         repair/2,
         append_string/2,
         append_point/3,
         ts_batch_to_binary/1,
         ts_key/1,
         parse_string/1,
         is_empty/1,
	 encode/2,
	 current_usec/0]).

%% for testing
-export([
	 ts_key_TEST/1
	]).

-export([option_types/1,
         validate_options/2]).

-export([iterator/2,
         iterator/3,
         iterator_move/2,
         iterator_close/1,
         range_scan/4,
         range_scan_ack/2,
         range_scan_fold/6]).

-export([emlfold1/4,
	 emlfold2/1]).

-export_type([db_ref/0,
              itr_ref/0]).

-on_load(init/0).

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

%% This cannot be a separate function. Code must be inline to trigger
%% Erlang compiler's use of optimized selective receive.
-define(WAIT_FOR_REPLY(Ref),
        receive {Ref, Reply} ->
                Reply
        end).

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

-type open_options() :: [{create_if_missing, boolean()} |
                         {error_if_exists, boolean()} |
                         {write_buffer_size, pos_integer()} |
                         {block_size, pos_integer()} |                  %% DEPRECATED
                         {sst_block_size, pos_integer()} |
                         {block_restart_interval, pos_integer()} |
                         {block_size_steps, pos_integer()} |
                         {paranoid_checks, boolean()} |
                         {verify_compactions, boolean()} |
                         {compression, boolean()} |
                         {use_bloomfilter, boolean() | pos_integer()} |
                         {total_memory, pos_integer()} |
                         {total_leveldb_mem, pos_integer()} |
                         {total_leveldb_mem_percent, pos_integer()} |
                         % TODO: Probably only good for prototype.
                         % Higher level might have to tweak specific knobs in
                         % different ways for time series tables.
                         {time_series, boolean()} |
                         {is_internal_db, boolean()} |
                         {limited_developer_mem, boolean()} |
                         {eleveldb_threads, pos_integer()} |
                         {fadvise_willneed, boolean()} |
                         {block_cache_threshold, pos_integer()} |
                         {delete_threshold, pos_integer()} |
                         {tiered_slow_level, pos_integer()} |
                         {tiered_fast_prefix, string()} |
                         {tiered_slow_prefix, string()}].

-type read_option() :: {verify_checksums, boolean()} |
                       {fill_cache, boolean()}.

-type itr_option() :: {iterator_refresh, boolean()}.

-type itr_options() :: [read_option() | itr_option()].

-type read_options() :: [{verify_checksums, boolean()} |
                         {fill_cache, boolean()} |
                         {iterator_refresh, boolean()}].

-type write_options() :: [{sync, boolean()}].

-type write_actions() :: [{put, Key::binary(), Value::binary()} |
                          {delete, Key::binary()} |
                          clear] | binary().

-type range_scan_options() :: [{start_inclusive, boolean()} |
                               {end_inclusive, boolean()} |
                               {fill_cache, boolean()} |
                               {max_batch_size, pos_integer()} |
                               {max_unacked_bytes, pos_integer()}].

-type streaming_option() :: {max_batch_bytes, pos_integer()} |
                            {max_unacked_bytes, pos_integer()}.

-type streaming_options() :: [streaming_option()].

-type fold_method() :: iterator | streaming.

-type fold_options() :: [read_option() |
                         {fold_method, fold_method()} |
                         {first_key, binary()} |
                         {last_key, binary() | undefined} |
                         {start_inclusive, boolean()} |
                         {end_inclusive, boolean()} |
                         {limit, pos_integer()} |
                         streaming_option()].

-type iterator_action() :: first | last | next | prev | prefetch | binary().

-opaque db_ref() :: binary().

-opaque itr_ref() :: binary().

-type stream_ref() :: {reference(), binary()}.

encode(Val, timestamp) ->
    <<Val:64>>;
encode(Val, binary) when is_binary(Val)->
    Val;
encode(Val, _) ->
    term_to_binary(Val).

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

async_open_family(_CallerRef, _Dbh, _Name, _Opts) ->
    erlang:nif_error({error, not_loaded}).

open_family(Dbh, Name, Opts) ->
    CallerRef = make_ref(),
    async_open_family(CallerRef, Dbh, Name, Opts),
    ?WAIT_FOR_REPLY(CallerRef).

async_close_family(_CallerRef, _Dbh, _Name) ->
    erlang:nif_error({error, not_loaded}).

close_family(Dbh, Name) ->
    CallerRef = make_ref(),
    async_close_family(CallerRef, Dbh, Name),
    ?WAIT_FOR_REPLY(CallerRef).
 
async_get(_CallerRef, _Dbh, _Family, _Key, _Opts) ->
    erlang:nif_error({error, not_loaded}).

get(Dbh, Family, Key, Opts) ->
    CallerRef = make_ref(),
    async_get(CallerRef, Dbh, Family, Key, Opts),
    ?WAIT_FOR_REPLY(CallerRef).

async_write(_CallerRef, _Ref, _Family, _Updates, _Opts) ->
    erlang:nif_error({error, not_loaded}).
    
put(Ref, Family, Key, Value, Opts) -> write(Ref, Family, [{put, Key, Value}], Opts).
    
delete(Ref, Family, Key, Opts) -> write(Ref, Family, [{delete, Key}], Opts).

write(Ref, Family, Updates, Opts) ->
    CallerRef = make_ref(),
    async_write(CallerRef, Ref, Family, Updates, Opts),
    ?WAIT_FOR_REPLY(CallerRef).

    
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

-spec async_iterator(reference(), db_ref(), itr_options()) -> ok.
async_iterator(_CallerRef, _Ref, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec async_iterator(reference(), db_ref(), itr_options(), keys_only) -> ok.
async_iterator(_CallerRef, _Ref, _Opts, keys_only) ->
    erlang:nif_error({error, not_loaded}).

-spec iterator(db_ref(), itr_options()) -> {ok, itr_ref()}.
iterator(Ref, Opts) ->
    CallerRef = make_ref(),
    async_iterator(CallerRef, Ref, Opts),
    ?WAIT_FOR_REPLY(CallerRef).

-spec iterator(db_ref(), itr_options(), keys_only) -> {ok, itr_ref()}.
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

-spec range_scan(db_ref(), binary(), binary()|undefined, range_scan_options()) ->
    {ok, {itr_ref(), reference()}} | {error, any()}.
range_scan(_DBRef, _StartKey, _EndKey, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec range_scan_ack(reference(), pos_integer()|0) -> ok | needs_reack.
range_scan_ack(_Ref, _NumBytes) ->
    erlang:nif_error({error, not_loaded}).

-spec streaming_start(db_ref(), binary(), binary() | undefined,
                      streaming_options()) ->
    {ok, stream_ref()} | {error, any()}.
streaming_start(_DBRef, _StartKey, _EndKey, _Opts) ->
    erlang:nif_error({error, not_loaded}).

-spec streaming_ack(binary(), pos_integer()) -> ok.
streaming_ack(_AckRef, _NumBytes) ->
    erlang:nif_error({error, not_loaded}).

-spec streaming_stop(binary()) -> ok.
streaming_stop(_AckRef) ->
    erlang:nif_error({error, not_loaded}).

-type fold_fun() :: fun(({Key::binary(), Value::binary()}, any()) -> any()).

range_scan_fold(Ref, Fun, Acc0, SKey, EKey, Opts) ->
    {ok, {MsgRef, AckRef}} = range_scan(Ref, SKey, EKey, Opts),
    do_range_scan_fold(MsgRef, AckRef, Fun, Acc0).

do_range_scan_batch(<<>>, _Fun, Acc) ->
    Acc;
do_range_scan_batch(Bin, Fun, Acc) ->
    {K, Bin2} = parse_string(Bin),
    {V, Bin3} = parse_string(Bin2),
    Acc2 = Fun({K, V}, Acc),
    do_range_scan_batch(Bin3, Fun, Acc2).

do_range_scan_ack(MsgRef, AckRef, Size, Fun, Acc) ->
    case range_scan_ack(AckRef, Size) of
        ok ->
            do_range_scan_fold(MsgRef, AckRef, Fun, Acc);
        needs_reack ->
            do_range_scan_ack(MsgRef, AckRef, 0, Fun, Acc)
    end.

do_range_scan_fold(MsgRef, AckRef, Fun, Acc) ->
    receive
        {range_scan_end, MsgRef} ->
            Acc;
        {range_scan_batch, MsgRef, Batch} ->
            Size = byte_size(Batch),
            Acc2 = do_range_scan_batch(Batch, Fun, Acc),
            do_range_scan_ack(MsgRef, AckRef, Size, Fun, Acc2);
        Msg ->
            lager:info("Range scan got unexpected message: ~p\n", [Msg])
    end.

do_streaming_batch(<<>>, _Fun, Acc) ->
    Acc;
do_streaming_batch(Bin, Fun, Acc) ->
    {K, Bin2} = parse_string(Bin),
    {V, Bin3} = parse_string(Bin2),
    Acc2 = Fun({K, V}, Acc),
    do_streaming_batch(Bin3, Fun, Acc2).

do_streaming_fold(StreamRef = {MsgRef, AckRef}, Fun, Acc) ->
    receive
        {streaming_end, MsgRef} ->
            Acc;
        {streaming_batch, MsgRef, Batch} ->
            Size = byte_size(Batch),
            Acc2 = do_streaming_batch(Batch, Fun, Acc),
            _ = streaming_ack(AckRef, Size),
            do_streaming_fold(StreamRef, Fun, Acc2)
    end.

do_streaming_fold_test1(StreamRef = {MsgRef, AckRef}, Fun, Acc) ->
    receive
        {streaming_end, MsgRef} ->
            Acc;
        {streaming_batch, MsgRef, Batch} ->
            Size = byte_size(Batch),
            _ = streaming_ack(AckRef, Size),
            do_streaming_fold(StreamRef, Fun, Acc)
    end.

current_usec() ->
    erlang:nif_error({error, not_loaded}).

parse_string(Bin) ->
    parse_string(0, 0, Bin).

parse_string(Size, Shift, <<1:1, N:7, Bin/binary>>) ->
    Size1 = Size + (N bsl Shift),
    parse_string(Size1, Shift + 7, Bin);
parse_string(Size, Shift, <<0:1, N:7, Bin/binary>>) ->
    Size1 = Size + (N bsl Shift),
    <<String:Size1/binary, Rest/binary>> = Bin,
    {String, Rest}.

%% Fold over the keys and values in the database
%% will throw an exception if the database is closed while the fold runs
-spec fold(db_ref(), fold_fun(), any(), fold_options()) -> any().
fold(Ref, Fun, Acc0, Opts) ->
    case proplists:get_value(fold_method, Opts, iterator) of
        iterator ->
	    {ok, Itr} = iterator(Ref, Opts),
            do_itr_fold(Itr, Fun, Acc0, Opts);
        streaming ->
            SKey = proplists:get_value(first_key, Opts, <<>>),
            EKey = proplists:get_value(last_key, Opts),
	    io:format("SKey = ~n"),
	    io:format(SKey),
	    io:format("~n"),
	    io:format("EKey = ~n"),
	    io:format(EKey),
	    io:format("~n"),
            {ok, StreamRef} = streaming_start(Ref, SKey, EKey, Opts),
            {_, AckRef} = StreamRef,
            try
                do_streaming_fold(StreamRef, Fun, Acc0)
            after
                %% Close early, do not wait for garbage collection.
                streaming_stop(AckRef)
            end
    end.

-spec foldtest1(db_ref(), fold_fun(), any(), fold_options()) -> any().
foldtest1(Ref, Fun, Acc0, Opts) ->
    case proplists:get_value(fold_method, Opts, iterator) of
        iterator ->
	    {ok, Itr} = iterator(Ref, Opts),
            do_itr_fold(Itr, Fun, Acc0, Opts);
        streaming ->
            SKey = proplists:get_value(first_key, Opts, <<>>),
            EKey = proplists:get_value(last_key, Opts),
            {ok, StreamRef} = streaming_start(Ref, SKey, EKey, Opts),
            {_, AckRef} = StreamRef,
            try
                do_streaming_fold_test1(StreamRef, Fun, Acc0)
            after
                %% Close early, do not wait for garbage collection.
                streaming_stop(AckRef)
            end
    end.

emlfold1(Ref, _, _, Opts) ->
    {ok, Itr} = iterator(Ref, Opts),
    Start = proplists:get_value(first_key, Opts, first),
    true = is_binary(Start) or (Start == first),
    iterator_move(Itr, Start).

emlfold2(Ref) ->
    {ok, Itr} = iterator(Ref, []),
    iterator_move(Itr, first).

-type fold_keys_fun() :: fun((Key::binary(), any()) -> any()).

%% Fold over the keys in the database
%% will throw an exception if the database is closed while the fold runs
-spec fold_keys(db_ref(), fold_keys_fun(), any(), read_options()) -> any().
fold_keys(Ref, Fun, Acc0, Opts) ->
    {ok, Itr} = iterator(Ref, Opts, keys_only),
    do_itr_fold(Itr, Fun, Acc0, Opts).

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

-spec option_types(open | read | write) -> [{atom(), bool | integer | any}].
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
     {compression, bool},
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
     {tiered_slow_prefix, any}];

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


do_itr_fold(Itr, Fun, Acc0, Opts) ->
    try
        %% Extract {first_key, binary()} and seek to that key as a starting
        %% point for the iteration. The folding function should use throw if it
        %% wishes to terminate before the end of the fold.
        Start = proplists:get_value(first_key, Opts, first),
        true = is_binary(Start) or (Start == first),
        fold_loop(iterator_move(Itr, Start), Itr, Fun, Acc0)
    after
        iterator_close(Itr)
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

validate_type({_Key, bool}, true)                            -> true;
validate_type({_Key, bool}, false)                           -> true;
validate_type({_Key, integer}, Value) when is_integer(Value) -> true;
validate_type({_Key, any}, _Value)                           -> true;
validate_type(_, _)                                          -> false.

append_varint(N, Bin) ->
    N2 = N bsr 7,
    case N2 of
        0 ->
            C = N rem 128,
            <<Bin/binary, C:8>>;
        _ ->
            C = (N rem 128) + 128,
            append_varint(N2, <<Bin/binary, C:8>>)
    end.

append_string(S, Bin) ->
    L = byte_size(S),
    B2 = append_varint(L, Bin),
    <<B2/binary, S/binary>>.

append_point(Timestamp, Value, Bin) ->
    Bin2 = <<Bin/binary, Timestamp:64>>,
    append_string(Value, Bin2).

append_points([], Bin) ->
    Bin;
append_points([{Timestamp, Value}|MorePoints], Bin) ->
    Bin2 = append_point(Timestamp, Value, Bin),
    append_points(MorePoints, Bin2).

ts_batch_to_binary({ts_batch, Family, Series, Points}) ->
    B2 = append_string(Family, <<>>),
    B3 = append_string(Series, B2),
    append_points(Points, B3).

ts_key(List) when is_list(List) ->
    ts_l2(lists:reverse(List), <<>>).

ts_l2([], Acc) ->
    Acc;
ts_l2([H | T], Acc) -> 
    ts_l2(T, append_string(H, Acc)). 

ts_key_TEST({Family, Series, Time}) ->
    B1 = <<Time:64>>,
    B2 = append_string(Family, B1),
    append_string(Series, B2).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

open_test() -> [{open_test_Z(), l} || l <- lists:seq(1, 20)].
open_test_Z() ->
    os:cmd("rm -rf /tmp/eleveldb.open.test"),
    {ok, Ref} = open("/tmp/eleveldb.open.test", [{create_if_missing, true}]),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    {ok, <<"123">>} = ?MODULE:get(Ref, <<"abc">>, []),
    not_found = ?MODULE:get(Ref, <<"def">>, []).

fold_test() -> [{fold_test_Z(), l} || l <- lists:seq(1, 20)].
fold_test_Z() ->
    os:cmd("rm -rf /tmp/eleveldb.fold.test"),
    {ok, Ref} = open("/tmp/eleveldb.fold.test", [{create_if_missing, true}]),
    ok = ?MODULE:put(Ref, <<"def">>, <<"456">>, []),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    ok = ?MODULE:put(Ref, <<"hij">>, <<"789">>, []),
    [{<<"abc">>, <<"123">>},
     {<<"def">>, <<"456">>},
     {<<"hij">>, <<"789">>}] = lists:reverse(fold(Ref, fun({K, V}, Acc) -> [{K, V} | Acc] end,
                                                  [], [])).

fold_keys_test() -> [{fold_keys_test_Z(), l} || l <- lists:seq(1, 20)].
fold_keys_test_Z() ->
    os:cmd("rm -rf /tmp/eleveldb.fold.keys.test"),
    {ok, Ref} = open("/tmp/eleveldb.fold.keys.test", [{create_if_missing, true}]),
    ok = ?MODULE:put(Ref, <<"def">>, <<"456">>, []),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    ok = ?MODULE:put(Ref, <<"hij">>, <<"789">>, []),
    [<<"abc">>, <<"def">>, <<"hij">>] = lists:reverse(fold_keys(Ref,
                                                                fun(K, Acc) -> [K | Acc] end,
                                                                [], [])).

fold_from_key_test() -> [{fold_from_key_test_Z(), l} || l <- lists:seq(1, 20)].
fold_from_key_test_Z() ->
    os:cmd("rm -rf /tmp/eleveldb.fold.fromkeys.test"),
    {ok, Ref} = open("/tmp/eleveldb.fromfold.keys.test", [{create_if_missing, true}]),
    ok = ?MODULE:put(Ref, <<"def">>, <<"456">>, []),
    ok = ?MODULE:put(Ref, <<"abc">>, <<"123">>, []),
    ok = ?MODULE:put(Ref, <<"hij">>, <<"789">>, []),
    [<<"def">>, <<"hij">>] = lists:reverse(fold_keys(Ref,
                                                     fun(K, Acc) -> [K | Acc] end,
                                                     [], [{first_key, <<"d">>}])).

destroy_test() -> [{destroy_test_Z(), l} || l <- lists:seq(1, 20)].
destroy_test_Z() ->
    os:cmd("rm -rf /tmp/eleveldb.destroy.test"),
    {ok, Ref} = open("/tmp/eleveldb.destroy.test", [{create_if_missing, true}]),
    ok = ?MODULE:put(Ref, <<"def">>, <<"456">>, []),
    {ok, <<"456">>} = ?MODULE:get(Ref, <<"def">>, []),
    close(Ref),
    ok = ?MODULE:destroy("/tmp/eleveldb.destroy.test", []),
    {error, {db_open, _}} = open("/tmp/eleveldb.destroy.test", [{error_if_exists, true}]).

compression_test() -> [{compression_test_Z(), l} || l <- lists:seq(1, 20)].
compression_test_Z() ->
    CompressibleData = list_to_binary([0 || _X <- lists:seq(1,20)]),
    os:cmd("rm -rf /tmp/eleveldb.compress.0 /tmp/eleveldb.compress.1"),
    {ok, Ref0} = open("/tmp/eleveldb.compress.0", [{write_buffer_size, 5},
                                                   {create_if_missing, true},
                                                   {compression, false}]),
    [ok = ?MODULE:put(Ref0, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
        I <- lists:seq(1,10)],
    {ok, Ref1} = open("/tmp/eleveldb.compress.1", [{write_buffer_size, 5},
                                                   {create_if_missing, true},
                                                   {compression, true}]),
    [ok = ?MODULE:put(Ref1, <<I:64/unsigned>>, CompressibleData, [{sync, true}]) ||
        I <- lists:seq(1,10)],
	%% Check both of the LOG files created to see if the compression option was correctly
	%% passed down
	MatchCompressOption =
		fun(File, Expected) ->
				{ok, Contents} = file:read_file(File),
				case re:run(Contents, "Options.compression: " ++ Expected) of
					{match, _} -> match;
					nomatch -> nomatch
				end
		end,
	Log0Option = MatchCompressOption("/tmp/eleveldb.compress.0/LOG", "0"),
	Log1Option = MatchCompressOption("/tmp/eleveldb.compress.1/LOG", "1"),
	?assert(Log0Option =:= match andalso Log1Option =:= match).


close_test() -> [{close_test_Z(), l} || l <- lists:seq(1, 20)].
close_test_Z() ->
    os:cmd("rm -rf /tmp/eleveldb.close.test"),
    {ok, Ref} = open("/tmp/eleveldb.close.test", [{create_if_missing, true}]),
    ?assertEqual(ok, close(Ref)),
    ?assertEqual({error, einval}, close(Ref)).

close_fold_test() -> [{close_fold_test_Z(), l} || l <- lists:seq(1, 20)].
close_fold_test_Z() ->
    os:cmd("rm -rf /tmp/eleveldb.close_fold.test"),
    {ok, Ref} = open("/tmp/eleveldb.close_fold.test", [{create_if_missing, true}]),
    ok = eleveldb:put(Ref, <<"k">>,<<"v">>,[]),
    ?assertException(throw, {iterator_closed, ok}, % ok is returned by close as the acc
                     eleveldb:fold(Ref, fun(_,_A) -> eleveldb:close(Ref) end, undefined, [])).

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
    ok = eleveldb:put(Ref, K, V, []),
    apply_kv_ops(Rest, Ref, orddict:store(K, V, Acc0));
apply_kv_ops([{async_put, K, V} | Rest], Ref, Acc0) ->
    MyRef = make_ref(),
    Context = {my_context, MyRef},
    ok = eleveldb:async_put(Ref, Context, K, V, []),
    receive
        {Context, ok} ->
            apply_kv_ops(Rest, Ref, orddict:store(K, V, Acc0));
        Msg ->
            error({unexpected_msg, Msg})
    end;
apply_kv_ops([{delete, K, _} | Rest], Ref, Acc0) ->
    ok = eleveldb:delete(Ref, K, []),
    apply_kv_ops(Rest, Ref, orddict:store(K, deleted, Acc0)).

prop_put_delete() ->
    ?LET({Keys, Values}, {keys(), values()},
         ?FORALL(Ops, eqc_gen:non_empty(list(ops(Keys, Values))),
                 begin
                     ?cmd("rm -rf /tmp/eleveldb.putdelete.qc"),
                     {ok, Ref} = eleveldb:open("/tmp/eleveldb.putdelete.qc",
                                                [{create_if_missing, true}]),
                     Model = apply_kv_ops(Ops, Ref, []),

                     %% Valdiate that all deleted values return not_found
                     F = fun({K, deleted}) ->
                                 ?assertEqual(not_found, eleveldb:get(Ref, K, []));
                            ({K, V}) ->
                                 ?assertEqual({ok, V}, eleveldb:get(Ref, K, []))
                         end,
                     lists:map(F, Model),

                     %% Validate that a fold returns sorted values
                     Actual = lists:reverse(fold(Ref, fun({K, V}, Acc) -> [{K, V} | Acc] end,
                                                 [], [])),
                     ?assertEqual([{K, V} || {K, V} <- Model, V /= deleted],
                                  Actual),
                     ok = eleveldb:close(Ref),
                     true
                 end)).

prop_put_delete_test_() ->
    Timeout1 = 10,
    Timeout2 = 15,
    %% We use the ?ALWAYS(300, ...) wrapper around the second test as a
    %% regression test.
    [{timeout, 3*Timeout1, {"No ?ALWAYS()", fun() -> qc(eqc:testing_time(Timeout1,prop_put_delete())) end}},
     {timeout, 10*Timeout2, {"With ?ALWAYS()", fun() -> qc(eqc:testing_time(Timeout2,?ALWAYS(150,prop_put_delete()))) end}}].

-endif.

-endif.
