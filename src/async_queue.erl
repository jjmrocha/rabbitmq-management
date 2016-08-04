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

-record(state, {queue, count, cores, level, max, timer}).

init(Parent) ->
  CoreNumber = erlang:system_info(schedulers),
  init(Parent, CoreNumber).

init(Parent, CoreNumber) ->
  Debug = sys:debug_options([]),
  proc_lib:init_ack({ok, self()}),
  process_flag(priority, high),
  {ok, Timer} = timer:send_interval(60000, check_queue_size),
  State = #state{queue = queue:new(), count = 0, cores = CoreNumber, level = 1, max = CoreNumber, timer= Timer},
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
handle_msg(check_queue_size, Parent, Debug, State) ->
  NewState = check_queue_size(State),
  loop(Parent, Debug, NewState);
handle_msg(_Msg, Parent, Debug, State) ->
  loop(Parent, Debug, State).

system_continue(Parent, Debug, State) -> loop(Parent, Debug, State).
system_terminate(Reason, _Parent, _Debug, #state{timer=Timer}) ->
	timer:cancel(Timer),
	exit(Reason).
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

check_queue_size(State = #state{queue = Queue, level = Level}) ->
  Size = queue:len(Queue),
  if Size > 200000, Level =:= 10 -> exit(queue_to_big);
	 Size > 10000, Level < 10 -> compute(State, Level + 1);
	 Size < 1000, Level > 1 -> compute(State, Level - 1);
	 true -> State
  end.

compute(State = #state{cores = CoreNumber}, Level) ->
  State#state{level = Level, max = CoreNumber * Level}.

send_job(Pid, Fun) ->
  Pid ! ?job(Fun),
  ok.