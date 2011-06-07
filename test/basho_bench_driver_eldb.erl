
-module(basho_bench_driver_eldb).

-export([new/1,
         run/4]).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    %% Pull the e_leveldb_config key which has all the key/value pairs for the
    %% engine -- stuff everything into the e_leveldb application namespace
    %% so that starting the app will pull it in.
    application:load(e_leveldb),
    Config = basho_bench_config:get(e_leveldb_config, []),
    [ok = application:set_env(e_leveldb, K, V) || {K, V} <- Config],

    WorkDir = basho_bench_config:get(eldb_work_dir, "/tmp/eldb.bb"),
    case basho_bench_config:get(eldb_clear_work_dir, false) of
        true ->
            io:format("Clearing work dir: " ++ WorkDir ++ "\n"),
            os:cmd("rm -rf " ++ WorkDir ++ "/*");
        false ->
            ok
    end,

    case e_leveldb:open(WorkDir, [{create_if_missing, true},
                                  {cache_size, 1024 * 1024 * 128}]) of
        {ok, Ref} ->
            {ok, Ref};
        {error, Reason} ->
            {error, Reason}
    end.


run(get, KeyGen, _ValueGen, State) ->
    case e_leveldb:get(State, KeyGen(), []) of
        {ok, _Value} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    print_status(State, 1000),
    case e_leveldb:put(State, KeyGen(), ValueGen(), []) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end;
run(batch_put, KeyGen, ValueGen, State) ->
    print_status(State, 100),
    Batch = [{put, KeyGen(), ValueGen()} || _ <- lists:seq(1, 100)],
    case e_leveldb:write(State, Batch, []) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end.


print_status(Ref, Count) ->
    status_counter(Count, fun() ->
                               {ok, S} = e_leveldb:status(Ref, <<"leveldb.stats">>),
                               io:format("~s\n", [S])
                       end).

status_counter(Max, Fun) ->
    Curr = case erlang:get(status_counter) of
               undefined ->
                   -1;
               Value ->
                   Value
           end,
    Next = (Curr + 1) rem Max,
    erlang:put(status_counter, Next),
    case Next of
        0 -> Fun(), ok;
        _ -> ok
    end.

