%% Author: sidorov
%% Created: Nov 11, 2013
%% Description: TODO: Add description to fold_test
-module(fold_test).

-export([create_db/1, calc_crc32/3, calc_count/3, calc_crc_func/2, calc_count_func/2]).

create_db(Path) ->
    {ok, Ref} = eleveldb:open(Path,  [{create_if_missing, true}]),
    try
        R = [
         eleveldb:put(Ref, <<X:64/integer, Y:32/integer>>, <<Y:32/integer, X:64/integer>>, []) || X <- lists:seq(1, 10000), Y <- lists:seq(1, 1000)
         ],
    lists:all(fun(X)->X =:= ok end, R)
    after
        eleveldb:close(Ref)
    end.

calc_crc32(DbPath, BatchSize, Reference) ->
    {ok, Ref} = eleveldb:open(DbPath,  [{create_if_missing, false}]),
    try
        Start = erlang:now(),
        R = try
                eleveldb:fold(Ref, fun calc_crc_func/2, 0, [], BatchSize)
            catch
                error:_ -> eleveldb:fold(Ref, fun calc_crc_func/2, 0, [])
            end,
        End = erlang:now(),
        {{crc, R}, {time_msec, timer:now_diff(End, Start) / 1000}, {coinside, Reference =:= R} }
    after 
        eleveldb:close(Ref)
    end.

calc_crc_func({K, V}, Acc) -> 
    R = erlang:crc32(Acc, K),
    erlang:crc32(R, V).

calc_count(DbPath, BatchSize, Reference) ->
    {ok, Ref} = eleveldb:open(DbPath,  [{create_if_missing, false}]),
    try
        Start = erlang:now(),
        R = try
                eleveldb:fold(Ref, fun calc_count_func/2, 0, [], BatchSize)
            catch
                error:_ -> eleveldb:fold(Ref, fun calc_count_func/2, 0, [])
            end,
        End = erlang:now(),
        {{count, R}, {time_msec, timer:now_diff(End, Start) / 1000}, {coinside, Reference =:= R} }
    after 
        eleveldb:close(Ref)
    end.

calc_count_func(_, Acc) ->
    Acc + 1.
    