%% Author: sidorov
%% Created: Nov 11, 2013
%% Description: TODO: Add description to fold_test
-module(fold_test).

-export([create_db/1, calc_crc32/3, calc_func/2]).

create_db(Path) ->
    {ok, Ref} = eleveldb:open(Path,  [{create_if_missing, true}]),
    try
        R = [
         eleveldb:put(Ref, <<X:64/integer, Y:32/integer>>, <<Y:32/integer, X:64/integer>>, []) || X <- lists:seq(1, 10000), Y <- lists:seq(1, 100)
         ],
    lists:all(fun(X)->X =:= ok end, R)
    after
        eleveldb:close(Ref)
    end.

calc_crc32(DbPath, BatchSize, Reference) ->
    {ok, Ref} = eleveldb:open(DbPath,  [{create_if_missing, false}]),
    try
        Start = erlang:now(),
        R = eleveldb:fold(Ref, fun calc_func/2, 0, [], BatchSize),
        End = erlang:now(),
        {{crc, R}, {time_msec, timer:now_diff(End, Start) / 1000}, {coinside, Reference =:= R} }
    after 
        eleveldb:close(Ref)
    end.

calc_func({K, V}, Acc) -> 
    R = erlang:crc32(Acc, K),
    erlang:crc32(R, V).
    