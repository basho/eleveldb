
-module(basho_bench_driver_eldb).

-export([new/1,
         run/4]).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    %% Make sure e_leveldb module is available
    %% case code:which(e_leveldb) of
    %%     non_existing ->
    %%         ?FAIL_MSG("~s requires e_leveldb to be available on code path.\n",
    %%                   [?MODULE]);
    %%     _ ->
    %%         ok
    %% end,

    %% Pull the e_leveldb_config key which has all the key/value pairs for the 
    %% engine -- stuff everything into the e_leveldb application namespace
    %% so that starting the app will pull it in.
    application:load(e_leveldb),
    Config = basho_bench_config:get(e_leveldb_config, []),
    [ok = application:set_env(e_leveldb, K, V) || {K, V} <- Config],

    os:cmd("rm -rf /tmp/bb.test"),

    case e_leveldb:open("/tmp/bb.test", [{create_if_missing, true}]) of
        {ok, Ref} ->
            {ok, Ref};
        {error, Reason} ->
            {error, Reason}
    end.


run(get, KeyGen, _ValueGen, State) ->
    case e_leveldb:get(State, KeyGen(), []) of
        not_found ->
            {ok, State};
        {ok, _Value} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    print_status(State, 1000),
    case e_leveldb:put(State, fmt_key(KeyGen()), ValueGen(), []) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end;
run(batch_put, KeyGen, ValueGen, State) ->
    print_status(State, 10),
    Batch = [{put, fmt_key(KeyGen()), ValueGen()} || _ <- lists:seq(1, 100)],
    case e_leveldb:write(State, Batch, []) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end.


fmt_key(K) ->
    list_to_binary(lists:flatten(io_lib:format("~w", [K]))).

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

