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
                             append_string(<<"val">>, <<Batch/binary, Time:64>>)
                        end, EmptyBatch, lists:seq(1, 10000)),
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

append_string(S, Bin) ->
    L = byte_size(S),
    B2 = append_varint(L, Bin),
    <<B2/binary, S/binary>>.

test_range_query(Ref) ->
    fun() -> read_10_items(Ref) end.

read_10_items(Ref) ->
    Start = eleveldb:ts_key({?FAMILY, ?SERIES, 1}),
    End = eleveldb:ts_key({?FAMILY, ?SERIES, 10}),
    Opts = [{range_filter, {"==", <<"val">>}}],
    {ok, {MsgRef, AckRef}} = eleveldb:range_scan(Ref, Start, End, Opts),
    receive
        {range_scan_end, MsgRef} ->
            ok;
        {range_scan_batch, MsgRef, Batch} ->
            Size = byte_size(Batch),
            assert_range(1, 10, Batch),
            ok = eleveldb:range_scan_ack(AckRef, Size)
    end.

assert_range(Start, End, <<>>) ->
    ?assertEqual(Start, End);
assert_range(Start, End, Batch) ->
    {Key, B1} = eleveldb:parse_string(Batch),
    {Val, B2} = eleveldb:parse_string(B1),
    <<Time:64, _/binary>> = Key,
    ?assertEqual(Time, Start),
    ?assertEqual(Val, <<"val">>),
    assert_range(Start + 1, End, B2).

