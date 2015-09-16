-module(sut).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

%%=======================================================================
%% This is all stuff we need to mock up msgpack-encoding of riak objects
%%=======================================================================

-define(V1_VERS, 1).
-define(MAGIC, 53).
-define(EMPTY_VTAG_BIN, <<"e">>).
-define(MD_VTAG,     <<"X-Riak-VTag">>).
-define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(MD_DELETED,  <<"X-Riak-Deleted">>).

-record(r_content, {
          metadata :: dict() | list(),
          value :: term()
         }).

-record(r_object, {
          bucket :: riak_object:bucket(),
          key :: riak_object:key(),
          contents :: [#r_content{}],
          vclock = vclock:fresh() :: vclock:vclock(),
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).

is_robject(#r_object{}) ->
    true;
is_robject(_) ->
    false.

-spec get_contents(riak_object:riak_object()) -> [{dict(), term()}].
get_contents(#r_object{contents=Contents}) ->
    [{Content#r_content.metadata, Content#r_content.value} ||
        Content <- Contents].

-spec bucket(riak_object:riak_object()) -> riak_object:bucket().
bucket(Obj1) ->
    Obj1#r_object.bucket.

-spec contents(riak_object:riak_object()) -> term().
contents(Obj1) ->
    Obj1#r_object.contents#r_content.value.


-type binary_version() :: v0 | v1.
-type encoding() :: erlang | msgpack.


-spec to_binary(binary_version(), riak_object:riak_object()) -> binary().
-spec to_binary(binary_version(), riak_object:riak_object(), encoding()) -> binary().

to_binary(Vers, Robj) ->
    to_binary(Vers, Robj, erlang).
to_binary(v0, RObj, _) ->
    term_to_binary(RObj);
to_binary(v1, #r_object{contents=Contents, vclock=VClock}, Enc) ->
    new_v1(VClock, Contents, Enc).

new_v1(Vclock, Siblings, Enc) ->
    VclockBin = term_to_binary(Vclock),
    VclockLen = byte_size(VclockBin),
    SibCount = length(Siblings),
    SibsBin = bin_contents(Siblings, Enc),
    <<?MAGIC:8/integer, ?V1_VERS:8/integer, VclockLen:32/integer, VclockBin/binary, SibCount:32/integer, SibsBin/binary>>.

bin_content(#r_content{metadata=Meta, value=Val}, Enc) ->
    ValBin = encode(Val, Enc),
    ValLen = byte_size(ValBin),
    MetaBin = meta_bin(Meta),
    MetaLen = byte_size(MetaBin),
    <<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary>>.

bin_contents(Contents, Enc) ->
    F = fun(Content, Acc) ->
                <<Acc/binary, (bin_content(Content, Enc))/binary>>
        end,
    lists:foldl(F, <<>>, Contents).

meta_bin(MD) ->
    {{VTagVal, Deleted, LastModVal}, RestBin} = dict:fold(fun fold_meta_to_bin/3,
                                                          {{undefined, <<0>>, undefined}, <<>>},
                                                          MD),
    VTagBin = case VTagVal of
                  undefined ->  ?EMPTY_VTAG_BIN;
                  _ -> list_to_binary(VTagVal)
              end,
    VTagLen = byte_size(VTagBin),
    LastModBin = case LastModVal of
                     undefined -> <<0:32/integer, 0:32/integer, 0:32/integer>>;
                     {Mega,Secs,Micro} -> <<Mega:32/integer, Secs:32/integer, Micro:32/integer>>
                 end,
    <<LastModBin/binary, VTagLen:8/integer, VTagBin:VTagLen/binary,
      Deleted:1/binary-unit:8, RestBin/binary>>.

fold_meta_to_bin(?MD_VTAG, Value, {{_Vt,Del,Lm},RestBin}) ->
    {{Value, Del, Lm}, RestBin};
fold_meta_to_bin(?MD_LASTMOD, Value, {{Vt,Del,_Lm},RestBin}) ->
     {{Vt, Del, Value}, RestBin};
fold_meta_to_bin(?MD_DELETED, true, {{Vt,_Del,Lm},RestBin})->
     {{Vt, <<1>>, Lm}, RestBin};
fold_meta_to_bin(?MD_DELETED, "true", Acc) ->
    fold_meta_to_bin(?MD_DELETED, true, Acc);
fold_meta_to_bin(?MD_DELETED, _, {{Vt,_Del,Lm},RestBin}) ->
    {{Vt, <<0>>, Lm}, RestBin};
fold_meta_to_bin(Key, Value, {{_Vt,_Del,_Lm}=Elems,RestBin}) ->
    ValueBin = encode_maybe_binary(Value),
    ValueLen = byte_size(ValueBin),
    KeyBin = encode_maybe_binary(Key),
    KeyLen = byte_size(KeyBin),
    MetaBin = <<KeyLen:32/integer, KeyBin/binary, ValueLen:32/integer, ValueBin/binary>>,
    {Elems, <<RestBin/binary, MetaBin/binary>>}.

encode(Bin, Enc) when Enc == erlang ->
    encode_maybe_binary(Bin);
encode(Bin, Enc) when Enc == msgpack ->
    encode_msgpack(Bin).

encode_msgpack(Bin) ->
    MsgBin = msgpack:pack(Bin, [{format, jsx}]),
    MsgBin.

em(Bin) ->
    MsgBin = msgpack:pack(Bin, [{format, jsx}]),
    MsgBin.

encode_maybe_binary(Bin) when is_binary(Bin) ->
    <<1, Bin/binary>>;
encode_maybe_binary(Bin) ->    
    <<0, (term_to_binary(Bin))/binary>>.


%% @doc return the binary version the riak object binary is encoded in
-spec binary_version(binary()) -> binary_version().
binary_version(<<131,_/binary>>) -> v0;
binary_version(<<?MAGIC:8/integer, 1:8/integer, _/binary>>) -> v1.

%% @doc Convert binary object to riak object
-spec from_binary(riak_object:bucket(),riak_object:key(),binary()) ->
    riak_object:riak_object() | {error, 'bad_object_format'}.
from_binary(B,K,Obj) ->
    from_binary(B,K,Obj,erlang).

-spec from_binary(riak_object:bucket(),riak_object:key(),binary(), encoding()) ->
    riak_object:riak_object() | {error, 'bad_object_format'}.
from_binary(_B,_K,<<131, _Rest/binary>>=ObjTerm, _) ->
    binary_to_term(ObjTerm);
from_binary(B,K,<<?MAGIC:8/integer, 1:8/integer, Rest/binary>>=_ObjBin, Enc) ->
    %% Version 1 of binary riak object
    case Rest of
        <<VclockLen:32/integer, VclockBin:VclockLen/binary, SibCount:32/integer, SibsBin/binary>> ->
            Vclock = binary_to_term(VclockBin),
            Contents = sibs_of_binary(SibCount, SibsBin, Enc),
            #r_object{bucket=B,key=K,contents=Contents,vclock=Vclock};
        _Other ->
            {error, bad_object_format}
    end;
from_binary(_B, _K, Obj = #r_object{}, _) ->
    Obj.

sibs_of_binary(Count,SibsBin, Enc) ->
    sibs_of_binary(Count, SibsBin, [], Enc).

sibs_of_binary(0, <<>>, Result, _) -> lists:reverse(Result);
sibs_of_binary(0, _NotEmpty, _Result, _) ->
    {error, corrupt_contents};
sibs_of_binary(Count, SibsBin, Result, Enc) ->
    {Sib, SibsRest} = sib_of_binary(SibsBin, Enc),
    sibs_of_binary(Count-1, SibsRest, [Sib | Result], Enc).

sib_of_binary(<<ValLen:32/integer, ValBin:ValLen/binary, MetaLen:32/integer, MetaBin:MetaLen/binary, Rest/binary>>, Enc) ->
    <<LMMega:32/integer, LMSecs:32/integer, LMMicro:32/integer, VTagLen:8/integer, VTag:VTagLen/binary, Deleted:1/binary-unit:8, MetaRestBin/binary>> = MetaBin,

    MDList0 = deleted_meta(Deleted, []),
    MDList1 = last_mod_meta({LMMega, LMSecs, LMMicro}, MDList0),
    MDList2 = vtag_meta(VTag, MDList1),
    MDList = meta_of_binary(MetaRestBin, MDList2),
    MD = dict:from_list(MDList),
    {#r_content{metadata=MD, value=decode(ValBin, Enc)}, Rest}.

deleted_meta(<<1>>, MDList) ->
    [{?MD_DELETED, "true"} | MDList];
deleted_meta(_, MDList) ->
    MDList.

last_mod_meta({0, 0, 0}, MDList) ->
    MDList;
last_mod_meta(LM, MDList) ->
    [{?MD_LASTMOD, LM} | MDList].

vtag_meta(?EMPTY_VTAG_BIN, MDList) ->
    MDList;
vtag_meta(VTag, MDList) ->
    [{?MD_VTAG, binary_to_list(VTag)} | MDList].

meta_of_binary(<<>>, Acc) ->
    Acc;
meta_of_binary(<<KeyLen:32/integer, KeyBin:KeyLen/binary, ValueLen:32/integer, ValueBin:ValueLen/binary, Rest/binary>>, ResultList) ->
    Key = decode_maybe_binary(KeyBin),
    Value = decode_maybe_binary(ValueBin),
    meta_of_binary(Rest, [{Key, Value} | ResultList]).

decode(ValBin, Enc) when Enc == erlang ->
    decode_maybe_binary(ValBin);
decode(ValBin, Enc) when Enc == msgpack ->
    decode_msgpack(ValBin).

decode_msgpack(ValBin) ->
    {ok, Unpacked} = msgpack:unpack(ValBin, [{format, jsx}]),
    Unpacked.

decode_maybe_binary(<<1, Bin/binary>>) ->
    Bin;
decode_maybe_binary(<<0, Bin/binary>>) ->
    binary_to_term(Bin).

%%=======================================================================
%% Generic utilities we need to run tests
%%=======================================================================

open() ->
    {ok, Ref} = eleveldb:open("ltest", [{create_if_missing, true}]),
    Ref.

open(Table) ->
    {ok, Ref} = eleveldb:open(Table, [{create_if_missing, true}]),
    Ref.

close(Ref) ->
    eleveldb:close(Ref).

clearDb() ->
    os:cmd("rm -rf ltest").

myformat(T) ->
    io:format("\r"),
    io:format(T),
    io:nl().

myformat(T,Val) ->
    io:format("\r"),
    io:format(T,[Val]),
    io:nl().

myformat(T,Val1, Val2) ->
    io:format("\r"),
    io:format(T,[Val1, Val2]),
    io:nl().

%%=======================================================================
%% Utilities needed for streaming fold tests
%%=======================================================================

%%------------------------------------------------------------
%% Recursive function to putkeys in the database
%%------------------------------------------------------------

putkeysObj(N) -> 
    clearDb(),
    putkeysObj(open(), N).
putkeysObj(Ref,N) -> 
    putkeysObj(Ref,N,1).
putkeysObj(Ref,N,Acc) when Acc == 1 -> 
    ValList = [{<<"field1">>,    1}, {<<"field2">>, <<"test2">>}, {<<"field3">>, 3}, {<<"field4">>, [1,2,3]}, {<<"field5">>, false}],
    addKey(Ref, ValList, Acc, N);
putkeysObj(Ref,N,Acc) when Acc == 2 -> 
    ValList = [{<<"field1">>, -2.0}, {<<"field2">>, <<"test2">>}, {<<"field3">>, 3}, {<<"field4">>, [1,2,3]}, {<<"field5">>, false}],
    addKey(Ref, ValList, Acc, N);
putkeysObj(Ref,N,Acc) when Acc == 3 -> 
    ValList = [{<<"field1">>,   3}, {<<"field2">>, <<"test2">>}, {<<"field3">>, 3}, {<<"field4">>, [1,2,3]}, {<<"field5">>, false}],
    addKey(Ref, ValList, Acc, N);
putkeysObj(Ref,N,Acc) when Acc < N -> 
    ValList = [{<<"field1">>, Acc},  {<<"field2">>, <<"test2">>}, {<<"field3">>, 3}, {<<"field4">>, [1,2,3]}, {<<"field5">>, false}],
    addKey(Ref, ValList, Acc, N);
putkeysObj(Ref,N,Acc) when Acc == N -> 
    ValList = [{<<"field1">>, Acc},  {<<"field2">>, <<"test2">>}, {<<"field3">>, 3}, {<<"field4">>, [1,2,3]}, {<<"field5">>, false}],
    addKey(Ref, ValList, Acc, N);
putkeysObj(Ref,N,Acc) when Acc > N ->
    close(Ref).

%%------------------------------------------------------------
%% Iterative add a key to the backend
%%------------------------------------------------------------

addKey(Ref, ValList, Acc, N) ->
    Key = list_to_binary("key"++integer_to_list(Acc)),
    Obj = riak_object:new(<<"bucket">>, Key, ValList),
    Val = to_binary(v1, Obj, msgpack),
    ok = eleveldb:put(Ref, Key, Val, []),

    putkeysObj(Ref,N,Acc+1).

%%------------------------------------------------------------
%% Explicit add a key to the backend
%%------------------------------------------------------------

addKey(Ref, KeyNum, ValList) ->
    Key = list_to_binary("key"++integer_to_list(KeyNum)),
    Obj = riak_object:new(<<"bucket">>, Key, ValList),
    Val = to_binary(v1, Obj, msgpack),
    ok = eleveldb:put(Ref, Key, Val, []).

%%------------------------------------------------------------
%% Get the value (contents) of a msgpack-encoded riak object
%%------------------------------------------------------------

getKeyVal(K,V) ->
    Obj = from_binary(<<"bucket">>, K, V, msgpack),
    [{_,Contents}] = get_contents(Obj),
    Contents.

%%------------------------------------------------------------
%% Fold over keys using streaming folds
%%------------------------------------------------------------

streamFoldTest(Filter, PutKeyFun) ->
    clearDb(),
    Opts=[{fold_method, streaming},
	  {range_filter, Filter},
	  {encoding, msgpack}],
    Ref = open(),

    PutKeyFun(Ref),

% Build a list of returned keys

    FF = fun({K,V}, Acc) -> 
		 Acc ++ [getKeyVal(K,V)]
	 end,

    Acc = eleveldb:fold(Ref, FF, [], Opts),
    ok = eleveldb:close(Ref),
    Acc.

get_field(Field, List) ->
    lists:keyfind(Field, 1, List).

match([], _, _, _) ->
    0;
match(List, Field, CompVal, CompFun) ->
%    io:format("~p~n", [lists:flatten(List)]),
    {_,Val} = get_field(Field, List),
    Match = match(Val,CompVal,CompFun),
%    io:format("Checking ~p again ~p match = ~p ~n", [Val, CompVal, Match]),
    Match.

match(V1,{CompVal},CompFun) ->
    match(V1, CompVal, CompFun);
match(V1,{_FilterVal, CompVal},CompFun) ->
    match(V1, CompVal, CompFun);
match(V1,V2,CompFun) ->
    case CompFun(V1,V2) of
	true -> 1;
	_ -> 0
    end.

fieldsMatching(Vals, Field, CompVal, CompFun) ->
%    io:format("Got Vals = ~p~n", [lists:flatten(Vals)]),
    lists:foldl(fun(Val, {N, Nmatch}) -> 
			{N + 1, Nmatch + match(Val, Field, CompVal, CompFun)} end, {0,0}, Vals).

%%=======================================================================
%% Actual tests begin here
%%=======================================================================

%% Test that we can pack and unpack erlang/msgpack-encoded objects

packObj_test() ->
    Obj = riak_object:new(<<"bucket">>, <<"key">>, [{<<"field1">>, 1}, {<<"field2">>, 2.123}]),
    PackedErl = to_binary(v1, Obj, erlang),
    PackedMsg = to_binary(v1, Obj, msgpack),
    ObjErl = from_binary(<<"bucket">>, <<"key">>, PackedErl, erlang),
    ObjMsg = from_binary(<<"bucket">>, <<"key">>, PackedMsg, msgpack),
    (ObjErl == Obj) and (ObjMsg == Obj).

putKeyNormalOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>, 1}, {<<"f2">>, "test1"}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [{<<"f1">>, 2}, {<<"f2">>, "test2"}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>, 3}, {<<"f2">>, "test3"}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>, 4}, {<<"f2">>, "test4"}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

defaultEvalFn({N,Nmatch}) ->
    (N > 0) and (N == Nmatch).

filterVal({FilterVal}) ->
    FilterVal;
filterVal({FilterVal, _CompVal}) ->
    FilterVal.

    
eqOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'==', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 == V2 end)).

neqOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'!=', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 /= V2 end)).

gtOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'>', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 > V2 end)).

gteOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'>=', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 >= V2 end)).

ltOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'<', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 < V2 end)).

lteOps({Field, Val, Type, PutFn, EvalFn}) ->
    Filter = {'=<', {field, Field, Type}, {const, filterVal(Val)}},
    Keys = streamFoldTest(Filter, PutFn),
    EvalFn(fieldsMatching(Keys, Field, Val, fun(V1,V2) -> V1 =< V2 end)).

allOps(Args) ->
    eqOps(Args) and neqOps(Args) and 
	gtOps(Args) and gteOps(Args) and
	ltOps(Args) and lteOps(Args).

eqOpsOnly(Args) ->
    eqOps(Args) and neqOps(Args).

allCompOps(Args) ->
    gtOps(Args) and gteOps(Args) and
	ltOps(Args) and lteOps(Args).

anyCompOps(Args) ->
    gtOps(Args) or gteOps(Args) or
	ltOps(Args) or lteOps(Args).

timestampOps_test() ->
    io:format("timestampOps_test~n"),
    F = <<"f6">>,
    Val = 2000,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    allOps({F, {Val}, timestamp, PutFn, EvalFn}).

intOps_test() ->
    io:format("intOps_test~n"),
    F = <<"f1">>,
    Val = 3,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    allOps({F, {Val}, integer, PutFn, EvalFn}).

binaryOps_test() ->
    io:format("binaryOps_test~n"),
    F = <<"f2">>,
    Val = "test3",
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    eqOpsOnly({F, {Val}, binary, PutFn, EvalFn}) and (anyCompOps({F, {Val}, binary, PutFn, EvalFn}) == false).

floatOps_test() ->
    io:format("floatOps_test~n"),
    F = <<"f3">>,
    Val = 3.0,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    allOps({F, {Val}, float, PutFn, EvalFn}).

boolOps_test() ->
    io:format("boolOps_test~n"),
    F = <<"f4">>,
    Val = true,
    PutFn = fun putKeyNormalOps/1,
    EvalFn = fun defaultEvalFn/1,
    eqOpsOnly({F, {Val}, boolean, PutFn, EvalFn}) and (anyCompOps({F, {Val}, boolean, PutFn, EvalFn}) == false).

anyOps_test() ->
    io:format("anyOps_test~n"),
    F = <<"f5">>,
    Val = sut:em([1,2,3]),
    CompVal = [1,2,3],
    PutFn  = fun sut:putKeyNormalOps/1,
    EvalFn = fun sut:defaultEvalFn/1,
    eqOpsOnly({F, {Val, CompVal}, any, PutFn, EvalFn}) and (anyCompOps({F, {Val, CompVal}, any, PutFn, EvalFn}) == false).

normalOps_test() ->
    io:format("normalOps_test~n"),
    intOps_test() and binaryOps_test() and boolOps_test() and floatOps_test() and anyOps_test() and timestampOps_test().

%%=======================================================================
%% Test malformed keys
%%=======================================================================

putKeyAbnormalOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>,   1}, {<<"f2">>, "test1"}, {<<"f3">>,   1.0}, {<<"f4">>,  false}, {<<"f5">>, em([1,2,3])}, {<<"f6">>,  1000}]),
    addKey(Ref, 2, [{<<"f1">>, 2.1}, {<<"f2">>, "test2"}, {<<"f3">>,   2.0}, {<<"f4">>,   true}, {<<"f5">>, em([2,3,4])}, {<<"f6">>, -2000}]),
    addKey(Ref, 3, [{<<"f1">>,   3}, {<<"f2">>,       3}, {<<"f3">>, "3.0"}, {<<"f4">>,  false}, {<<"f5">>, em([3,4,5])}, {<<"f6">>,  3000}]),
    addKey(Ref, 4, [{<<"f1">>,   4}, {<<"f2">>, "test4"}, {<<"f3">>,   4.0}, {<<"f4">>, "true"}, {<<"f5">>,           4}, {<<"f6">>,  4000}]).

abnormalEvalFn({N,_Nmatch}) ->
    (N == 0).

badInt_test() ->
    io:format("badInt_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    gtOps({F, {Val}, integer, PutFn, EvalFn}).

badTimestamp_test() ->
    io:format("badTimestamp_test~n"),
    F = <<"f6">>,
    Val = 2000,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    gtOps({F, {Val}, timestamp, PutFn, EvalFn}).

badBinary_test() ->
    io:format("badBinary_test~n"),
    F = <<"f2">>,
    Val = "test",
    PutFn = fun putKeyAbnormalOps/1,
    neqOps({F, {Val}, binary, PutFn, fun({N,_}) -> N == 4 end}).

badFloat_test() ->
    io:format("badFloat_test~n"),
    F = <<"f3">>,
    Val = 0.0,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    gtOps({F, {Val}, float, PutFn, EvalFn}).

badBool_test() ->
    io:format("badBool_test~n"),
    F = <<"f4">>,
    Val = false,
    PutFn = fun putKeyAbnormalOps/1,
    EvalFn = fun abnormalEvalFn/1,
    eqOps({F, {Val}, boolean, PutFn, EvalFn}).

badAny_test() ->
    io:format("badAny_test~n"),
    F = <<"f5">>,
    Val = sut:em([1,2,3]),
    CompVal = [1,2,3],
    PutFn  = fun sut:putKeyNormalOps/1,
    neqOps({F, {Val, CompVal}, any, PutFn, fun({N,_}) -> N == 3 end}).

abnormalOps_test() ->
    io:format("abnormalOps_test~n"),
    badInt_test() and badBinary_test() and badFloat_test() and badBool_test() and badTimestamp_test() and badAny_test().

%%=======================================================================
%% Test various exceptional conditions
%%=======================================================================

putKeyMissingOps(Ref) ->
    addKey(Ref, 1, [{<<"f1">>, 1}, {<<"f2">>, "test1"}, {<<"f3">>, 1.0}, {<<"f4">>, false}, {<<"f5">>, [1,2,3]}, {<<"f6">>, 1000}]),
    addKey(Ref, 2, [{<<"f2">>, "test2"}, {<<"f3">>, 2.0}, {<<"f4">>, true},  {<<"f5">>, [2,3,4]}, {<<"f6">>, 2000}]),
    addKey(Ref, 3, [{<<"f1">>, 3}, {<<"f2">>, "test3"}, {<<"f3">>, 3.0}, {<<"f4">>, false}, {<<"f5">>, [3,4,5]}, {<<"f6">>, 3000}]),
    addKey(Ref, 4, [{<<"f1">>, 4}, {<<"f2">>, "test4"}, {<<"f3">>, 4.0}, {<<"f4">>, true},  {<<"f5">>, [4,5,6]}, {<<"f6">>, 4000}]).

%% Valid filter, but values are missing for referenced keys

missingKey_test() ->
    io:format("missingKey_test~n"),
    F = <<"f1">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    gtOps({F, {Val}, integer, PutFn, EvalFn}).

%% Filter references a key that doesn't exist

filterRefMissingKey_test() ->
    io:format("filterRefMissingKey_test~n"),
    F = <<"f0">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    gtOps({F, {Val}, integer, PutFn, EvalFn}).

%% Filter specified the wrong type for a valid key

filterRefWrongType_test() ->
    io:format("filterRefWrongType_test~n"),
    F = <<"f2">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    gtOps({F, {Val}, integer, PutFn, EvalFn}).

%% Filter specifies type that's not supported

filterRefInvalidType_test() ->
    io:format("filterRefInvalidType_test~n"),
    F = <<"f2">>,
    Val = 0,
    PutFn = fun putKeyMissingOps/1,
    EvalFn = fun abnormalEvalFn/1,
    gtOps({F, {Val}, map, PutFn, EvalFn}).

variousExceptionalOps_test() ->
    missingKey_test() and filterRefMissingKey_test() and filterRefWrongType_test() and filterRefInvalidType_test().

%%=======================================================================
%% All test defined in this file
%%=======================================================================

all_test() ->
    normalOps_test() and abnormalOps_test() and variousExceptionalOps_test().
