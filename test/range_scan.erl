-module(range_scan).

-compile(export_all).


-include_lib("eunit/include/eunit.hrl").

-define(FAMILY, <<"family">>).
-define(SERIES, <<"series">>).

range_scan_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun test_range_query/1}.


setup() ->
    {ok, Ref} = eleveldb:open("/tmp/eleveldb.range_scan.test",
                              [{create_if_missing, true},
                               {time_series, true}]),
    populate(Ref),
    Ref.

cleanup(Ref) ->
    ok = eleveldb:close(Ref),
    os:cmd("rm -rf /tmp/eleveldb.range_scan.test").

populate(Ref) ->
    EmptyBatch = append_string(?SERIES, append_string(?FAMILY, <<>>)),
    Batch = lists:foldl(fun(Time, Batch) ->
                append_record([{<<"field_1">>, Time}, {<<"field_2">>, Time}], <<Batch/binary, Time:64>>)
                        end, EmptyBatch, lists:seq(0, 1000000)),
    ok = eleveldb:write(Ref, Batch, []).

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
append_record(Record, Bin) ->
    RecordData = msgpack:pack(Record, [{format, jsx}]),
    append_string(RecordData, Bin).

append_string(S, Bin) ->
    L = byte_size(S),
    B2 = append_varint(L, Bin),
    <<B2/binary, S/binary>>.

test_range_query(Ref) ->
    {timeout, 50000, fun() ->
        lists:foreach(fun(_X) ->
            read_items_with_level_filter(Ref, 0, 1000000, 50000, 200000)
        end, lists:seq(0,9)),
        lists:foreach(fun(_X) ->
            read_items_with_erlang_filter(Ref, 0, 1000000, 50000, 200000)
        end, lists:seq(0,9))
    end}.

read_items_with_level_filter(Ref, Start0, End0, F1, F2) ->
    Start = eleveldb:ts_key_TEST({?FAMILY, ?SERIES, Start0}),
    End = eleveldb:ts_key_TEST({?FAMILY, ?SERIES, End0}),
    Opts = [{range_filter, {"and", [
                {">=", [{field, "field_1"}, {const, F1}]},
                {"<=", [{field, "field_2"}, {const, F2}]}
            ]}}],
    ExpectedRows = calc_expected_rows(End0, F2, Start0, F1),
    time_result("With Level Filtering", fun() ->
        {ok, {MsgRef, AckRef}} = eleveldb:range_scan(Ref, Start, End, Opts),
        receive_batch(AckRef, MsgRef, ExpectedRows, 0, 0, lists:max([Start0, F1]), fun(_Row) -> true end)
        end).

calc_expected_rows(End0, F2, Start0, F1) ->
    lists:min([End0, F2]) - lists:max([Start0, F1]) + 1.

read_items_with_erlang_filter(Ref, Start0, End0, F1, F2) ->
    Start = eleveldb:ts_key_TEST({?FAMILY, ?SERIES, Start0}),
    End = eleveldb:ts_key_TEST({?FAMILY, ?SERIES, End0}),
    Opts = [],
    ExpectedRows = calc_expected_rows(End0, F2, Start0, F1),
    time_result("With Erlang Filtering", fun() ->
        {ok, {MsgRef, AckRef}} = eleveldb:range_scan(Ref, Start, End, Opts),
        receive_batch(AckRef, MsgRef, ExpectedRows, 0, 0, Start0, create_erlang_filter(F1, F2))
    end).

create_erlang_filter(F1Val, F2Val) ->
    fun(Row) ->
        [{<<"field_1">>, F1}, {<<"field_2">>, F2}] = Row,
        (F1 >= F1Val) and (F2 =< F2Val)
    end.

time_result(Title, Fun) ->
    T0 = os:timestamp(),
    Fun(),
    T1 = os:timestamp(),
    io:format(user, "~n~s: ~p microseconds~n", [Title, timer:now_diff(T1, T0)]).

receive_batch(AckRef, MsgRef, ExpectedRows, ActualRows0, RowsProcessed0, StartOffset, RowFilt) ->
    receive
        {range_scan_end, MsgRef} ->
            ?assertEqual(ExpectedRows, ActualRows0),
            ok;
        {range_scan_batch, MsgRef, Batch} ->
            Size = byte_size(Batch),
            {RowsProcessed, ActualRows} = process_batch(Batch, RowsProcessed0, StartOffset, ActualRows0, RowFilt),
            ok = eleveldb:range_scan_ack(AckRef, Size),
            receive_batch(AckRef, MsgRef, ExpectedRows, ActualRows, RowsProcessed, StartOffset, RowFilt)
    end.

process_batch(<<>>, End, _StartOffset, Count, _RowFilt) ->
    {End, Count};

process_batch(Batch, Start0, StartOffset, Count, RowFilt) ->
    Start = Start0 + StartOffset,
    {Key, B1} = eleveldb:parse_string(Batch),
    {Val, B2} = eleveldb:parse_string(B1),
    <<Time:64, _/binary>> = Key,
    ?assertEqual(Start, Time),
    {ok, Unpacked} = msgpack:unpack(Val, [{format, jsx}]),
    NewCount = case RowFilt(Unpacked) of
                   true ->
                       ?assertEqual([{<<"field_1">>,Start},{<<"field_2">>, Start}], Unpacked),
                       Count + 1;
                   _ -> Count
               end,
    process_batch(B2, Start0+1, StartOffset, NewCount, RowFilt).

