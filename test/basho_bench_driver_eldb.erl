
-module(basho_bench_driver_eldb).

-record(state, { ref  :: e_leveldb:db_ref(),
                 itr  :: e_leveldb:itr_ref()}).

-export([new/1,
         run/4]).

%% ====================================================================
%% API
%% ====================================================================
-spec new(_) -> {ok, #state{}} | {error, term()}.
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

    case e_leveldb:open(WorkDir, [{create_if_missing, true}]) of
        {ok, Ref} ->
            {ok, Itr} = e_leveldb:iterator(Ref, []),
            {ok, #state { ref = Ref, itr = Itr }};
        {error, Reason} ->
            {error, Reason}
    end.

-spec run(get|put, fun(() -> e_leveldb:iterator_action()), _, #state{}) -> {ok, #state{}} | {error, term(), #state{}}.
run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case e_leveldb:iterator_move(State#state.itr, Key) of
        {ok, Key, _Value} ->
            {ok, State};
        {ok, OtherKey, _Value} ->
            io:format("~p vs ~p!!\n", [Key, OtherKey]),
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    print_status(State#state.ref, 1000),
    case e_leveldb:put(State#state.ref, KeyGen(), ValueGen(), []) of
        ok ->
            %% Reset the iterator to see the latest data
            e_leveldb:iterator_close(State#state.itr),
            Itr = e_leveldb:iterator(State#state.ref),
            {ok, State#state { itr = Itr }};
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

