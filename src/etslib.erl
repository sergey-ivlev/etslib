-module(etslib).
-behaviour(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/1]).
-export([put/3, put/4, get/2, delete/2, state/1]).

-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2, code_change/3]).

-record(field, {key, value, last_time_updated, ttl}).
-record(state, {table, ttl, check_timeout}).

-define(TIMEOUT, 5000).

%%% API
-spec put(pid(), any(), any()) -> ok | {error, timeout}.
put(Server, Key, Value) ->
    gen_server:call(Server, {put, Key, Value, undefined}, ?TIMEOUT).
put(Server, Key, Value, Ttl) ->
    gen_server:call(Server, {put, Key, Value, Ttl}, ?TIMEOUT).

-spec get(pid(), any()) -> {ok, any()} | {error, undefined} | {error, timeout}.
get(Server, Key) ->
    gen_server:call(Server, {get, Key}, ?TIMEOUT).

state(Server) ->
    gen_server:call(Server, state, ?TIMEOUT).

delete(Server, Key) ->
    gen_server:call(Server, {delete, Key}, ?TIMEOUT).

%%% Gen server implementation
-spec start_link(Opts) -> {ok, Pid} | {error, Reason} when
    Opts :: [Option],
    Option :: {ttl, non_neg_integer()} |
    {check_timeout, non_neg_integer()} |
    {bucket, atom()},
    Pid :: pid(),
    Reason :: any().
start_link(Opts) ->
    {bucket, Bucket} = lists:keyfind(bucket, 1, Opts),
    gen_server:start_link({local, Bucket}, ?MODULE, [Opts], []).

init([Opts]) ->
    Bucket = proplists:get_value(bucket, Opts),
    Ttl = proplists:get_value(ttl, Opts, 10000),
    CheckTimeout = proplists:get_value(check_timeout, Opts, 1000),
    Table = ets:new(Bucket, [set,
        compressed,
        public,
        {keypos, 2},
        {read_concurrency, true}]),
    self() ! clean_old,
    State = #state{
        table = Table,
        ttl = Ttl,
        check_timeout = CheckTimeout
    },
    {ok, State}.

handle_call({put, Key, Value}, _From, #state{ttl=Ttl} = State) ->
    handle_call({put, Key, Value, Ttl}, _From, State);
handle_call({put, Key, Value, undefined}, _From, #state{ttl=Ttl} = State) ->
    handle_call({put, Key, Value, Ttl}, _From, State) ;
handle_call({put, Key, Value, Ttl}, _From, #state{table=Table} = State) ->
    Field = #field{key=Key, value=Value, last_time_updated=now_unixtime(), ttl = Ttl},
    true = ets:insert(Table, Field),
    {reply, ok, State};
handle_call({get, Key}, _From, #state{table=Table} = State) ->
    Res = case ets:lookup(Table, Key) of
              [] ->
                  {error, undefined};
              [Field] ->
                  {ok, Field#field.value}
          end,
    {reply, Res, State};
handle_call({delete, Key}, _From, #state{table=Table} = State) ->
    true = ets:delete(Table, Key),
    {reply, ok, State};
handle_call(state, _From, #state{table=Table} = State) ->
    List = ets:tab2list(Table),
    {reply, List, State};
handle_call(_Request, _From, State) ->
    Reply = {error, unknow_req},
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(clean_old, #state{table=Table, check_timeout=CheckTimeout} = State) ->
    NowTime = now_unixtime(),
    Ms = ets:fun2ms(fun(#field{last_time_updated=Time, ttl=Ttl}) when Time + Ttl < NowTime -> true end),
    ets:select_delete(Table, Ms),
    timer:send_after(CheckTimeout, clean_old),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal helpers

now_unixtime() ->
    erlang:convert_time_unit(erlang:system_time(), native, milli_seconds).
