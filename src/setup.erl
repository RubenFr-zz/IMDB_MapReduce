-module(setup).
-author("Ruben").

-define(MASTER_FILES, ["master.erl", "dataInit.erl", "graph.erl", "graphviz.erl", "pmap.erl"]).

%% API
-export([start/0, start/1]).

start() -> start(0).


start(NumOfWorkers) ->
  compile_workers(NumOfWorkers),
  compile_master(),
  compile_client().


compile_workers(NumOfWorkers) ->
  {ok, Pwd} = file:get_cwd(),
  file:set_cwd(lists:concat([Pwd, "/server"])),

  io:format("~nCompiling server.erl...~n"),
  case compile:file("server.erl", [return_warnings]) of
    {ok, server, []} ->
      io:format("Successfully compiled server!~n"),
      lists:foreach(
        fun(X) ->
          Dir = lists:concat(["server", X]),
          file:make_dir(Dir),
          file:copy("server.beam", lists:concat([Dir, "/server.beam"]))
        end, lists:seq(1, NumOfWorkers)
      );
    {ok, server, Warnings} -> io:format("Successfully compiled server.erl with Warning(s): ~p~n", [Warnings]);
    error -> io:format("ERROR IN COMPILATION OF server.erl~n")
  end,
  file:set_cwd(Pwd).


compile_master() ->
  {ok, Pwd} = file:get_cwd(),
  file:set_cwd(lists:concat([Pwd, "/master"])),
  lists:foreach(
    fun(X) ->
      io:format("~nCompiling ~s...~n", [X]),
      case compile:file(X, [return_warnings]) of
        {ok, Module, []} -> io:format("Successfully compiled ~s!~n", [Module]);
        {ok, Module, Warnings} -> io:format("Successfully compiled ~p with Warning(s): ~p~n", [Module, Warnings]);
        error -> io:format("ERROR IN ~s COMPILATION~n", [X])
      end
    end, ?MASTER_FILES
  ),
  file:set_cwd(Pwd).


compile_client() ->
  {ok, Pwd} = file:get_cwd(),
  file:set_cwd(lists:concat([Pwd, "/client"])),
  io:format("~nCompiling client.erl...~n"),
  case compile:file("client.erl", [return_warnings]) of
    {ok, client, []} -> io:format("Successfully compiled client!~n~n");
    {ok, client, Warnings} -> io:format("Successfully compiled client.erl with Warning(s): ~p~n~n", [Warnings]);
    error -> io:format("ERROR IN client.erl COMPILATION~n~n")
  end,
  file:set_cwd(Pwd).
