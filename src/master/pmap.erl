%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Aug 2021 9:40 PM
%%%-------------------------------------------------------------------
-module(pmap).
-author("Ruben").

-export([map/2, map/3]).

map(F, Es) ->
	Parent = self(),
	Running = [
		spawn_monitor(fun() -> Parent ! {self(), F(E)} end) 
		|| E <- Es],
   	collect(Running, 5000).

map(F, Es, Timeout) ->
	Parent = self(),
	Running = [ 
		spawn_monitor(fun() -> Parent ! {self(), F(E)} end) 
		|| E <- Es],
   	collect(Running, Timeout).

collect([], _Timeout) -> [];
collect([{Pid, MRef} | Next], Timeout) ->
	receive
		{Pid, Res} ->
			erlang:demonitor(MRef, [flush]),
			% io:format("Received Reply (from ~p): ~p~n", [Pid, Res]),
			[Res | collect(Next, Timeout)];
		{'DOWN', MRef, process, Pid, Reason} ->
			[{error, Reason} | collect(Next, Timeout)]
	after Timeout ->
		exit(pmap_timeout)
	end.
