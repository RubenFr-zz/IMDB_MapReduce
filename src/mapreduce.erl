%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, BGU
%%% @doc
%%%
%%% @end
%%% Created : 15. Aug 2021 10:04 PM
%%%-------------------------------------------------------------------
-module(mapreduce).
-author("Ruben").

-export([mapreduce/3]).

mapreduce(Table, MapFun, RedFun) -> 
	RedPID = spawn_link(fun() -> reduce(RedFun, [], 1) end),
	map(Table, MapFun, RedPID),
	receive
		Replies -> Replies
	end.

map(Table, MapFun, RedPID) -> 
	Running = lists:map( fun(X) -> spawn(fun() -> RedPID ! {MapFun(X), 1} end) end, ets:tab2list(Table) ),
	wait(Running),
	RedPID ! {stop, self()}.

reduce(RedFun, Replies, Acc) ->
	receive
		{Mapped, 1} ->
			case RedFun(Mapped) of
				false -> reduce(RedFun, Replies, Acc + 1);
				Reply -> reduce(RedFun, Reply ++ Replies, Acc + 1)
			end;

		{stop, PID} -> PID ! {Replies, Acc}
	end.

wait([]) -> ok;
wait(Running = [Proc | Next]) ->
	case is_process_alive(Proc) of
		false -> wait(Next);
		true -> 
			timer:sleep(1),
			wait(Running)
	end.
