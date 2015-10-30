-module(eleveldb_ts).

-export([
	 encode_key/1,
	 encode_record/1,
	 decode_record/1
	 ]).

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

encode_key(Elements) when is_list(Elements) ->
    encode_k2(Elements, <<>>).

%% TODO recheck this packing
%% binary list stuff is shocking here too
encode_k2([],                    Bin) -> Bin;
encode_k2([{timestamp, Ts} | T], Bin) -> encode_k2(T, append(<<Ts:64>>, Bin));
encode_k2([{double, F}     | T], Bin) -> encode_k2(T, append(<<F:64/float>>,  Bin));
encode_k2([{sint64, I}     | T], Bin) -> encode_k2(T, append(<<I:64/integer>>, Bin));
encode_k2([{varchar, B}    | T], Bin) when is_binary(B) -> encode_k2(T, append(B, Bin));
encode_k2([{binary, L}     | T], Bin) when is_list(L)   -> B = list_to_binary(L),
                                                           encode_k2(T, append(B, Bin)).

encode_record(Record) -> 
    msgpack:pack(Record, [{format, jsx}]).

decode_record(Bin) when is_binary(Bin) ->
    {ok, Record} = msgpack:unpack(Bin, [{format, jsx}]),
    Record.


%%
%% Internal Funs
%%

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

append(S, Bin) when is_binary(S)  andalso
                    is_binary(Bin) ->
    L = byte_size(S),
    B2 = append_varint(L, Bin),
    <<B2/binary, S/binary>>.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_encode_key_test() ->
    Key = [{timestamp, 1}, {varchar, <<"abc">>}, {double, 1.0}, {sint64, 9}],
    Got = encode_key(Key),
    Exp = <<8,0,0,0,0,0,0,0,01,3,97,98,99,8,63,240,0,0,0,0,0,0,8,0,0,0,0,0,0,0,9>>,
    ?assertEqual(Exp, Got).

simple_encode_record_test() ->
    Rec = [{<<"field_1">>, 123}, {<<"field_2">>, "abdce"}],
    Got = encode_record(Rec),
    Exp = <<130,167,102,105,101,108,100,95,49,123,167,102,105,101,108,100,95,50,149,97,98,100,99,101>>,
    ?assertEqual(Exp, Got).

simple_decode_record_test() ->
    Rec = <<130,167,102,105,101,108,100,95,49,123,167,102,105,101,108,100,95,50,149,97,98,100,99,101>>,
    Got = decode_record(Rec),
    Exp = [{<<"field_1">>, 123}, {<<"field_2">>, "abdce"}],
    ?assertEqual(Exp, Got).

-endif.
