-module(async_queue).

-define(job(Fun), {job, Fun}).

%% ====================================================================
%% API functions
%% ====================================================================

-export([start_link/0]).
-export([run/2]).

-export([init/1, init/2]).
-export([system_continue/3, system_terminate/4, system_code_change/4]).

start_link() ->
  proc_lib:start_link(?MODULE, init, [self()]).

% functions

run(Pid, Fun) when is_pid(Pid), is_function(Fun, 0) -> send_job(Pid, Fun);
run(_Pid, _Fun) -> {error, invalid_function}.

%% ====================================================================
%% Internal functions
%% ====================================================================

-record(state, {queue, count, max}).

init(Parent) ->
  Max = get_default_max_processes(),
  init(Parent, Max).

init(Parent, Max) ->
  Debug = sys:debug_options([]),
  proc_lib:init_ack({ok, self()}),
  process_flag(priority, high),
  State = #state{queue = queue:new(), count = 0, max = Max},
  loop(Parent, Debug, State).

loop(Parent, Debug, State) ->
  Msg = receive
          Input -> Input
        end,
  handle_msg(Msg, Parent, Debug, State).

handle_msg({system, From, Request}, Parent, Debug, State) ->
  sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, State);
handle_msg({'DOWN', Ref, _, _, _}, Parent, Debug, State) ->
  NewState = handle_terminated(Ref, State),
  loop(Parent, Debug, NewState);
handle_msg(?job(Fun), Parent, Debug, State) ->
  NewState = process(Fun, State),
  loop(Parent, Debug, NewState);
handle_msg(_Msg, Parent, Debug, State) ->
  loop(Parent, Debug, State).

system_continue(Parent, Debug, State) -> loop(Parent, Debug, State).
system_terminate(Reason, _Parent, _Debug, _State) -> exit(Reason).
system_code_change(State, _Module, _OldVsn, _Extra) -> {ok, State}.

handle_terminated(_Ref, State = #state{queue = Queue, count = Count}) ->
  case queue:out(Queue) of
    {empty, _} -> State#state{count = Count - 1};
    {{value, Fun}, NewQueue} ->
      {_, _NewRef} = erlang:spawn_opt(Fun, [monitor, {priority, high}]),
      State#state{queue = NewQueue}
  end.

process(Fun, State = #state{queue = Queue, count = Max, max = Max}) ->
  NewQueue = queue:in(Fun, Queue),
  State#state{queue = NewQueue};
process(Fun, State = #state{count = Count}) ->
  {_, _Ref} = erlang:spawn_opt(Fun, [monitor, {priority, high}]),
  State#state{count = Count + 1}.

get_default_max_processes() ->
  erlang:system_info(schedulers).

send_job(Pid, Fun) ->
  Pid ! ?job(Fun),
  ok.