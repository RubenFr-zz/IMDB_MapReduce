%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, BGU
%%% @doc
%%%
%%% @end
%%% Created : 12. Aug 2021 6:33 PM
%%%-------------------------------------------------------------------
-module(dataInit).
-author("Ruben").

-include_lib("constants.hrl").

%% API
-export([start_distribution/0, distribute/1, redistribute/2, servers/0]).

start_distribution() ->
  Servers = find_servers(?SERVERS, []),
  io:format("Found ~p server(s): ~p~n", [length(Servers), Servers]),

  NamePID = first_step(),
  distribute(Servers),
  {Servers, NamePID}.


distribute(Servers) ->
  second_step(Servers),
  third_step(Servers),
  io:format("Data Distributed to ~p (~p servers).~n", [Servers, length(Servers)]).


redistribute([], _) -> ok;
redistribute(Servers, Down) ->
  Chosen = distributeTo(Down, Servers),
  case gen_server:call({server, Chosen}, {merge, Down}) of
    ack -> ok;
    {error, _} -> redistribute(Servers -- [distributeTo(Down, Servers)], Down)
  end.


% First Step: Load Names file. Return PID of the running process with the data
first_step() -> spawn(fun() -> first_step(ets:new(names, [ordered_set, public, {read_concurrency, true}])) end).

first_step(Table) ->
  {ok, File} = file:open(?NAMES_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  first_step(Table, File).

first_step(Table, File) ->
  case io:get_line(File, "") of
    eof ->
      file:close(File),
      loop_names(Table);
    Line ->
      {ID, Name} = parseName(string:split(Line, "\t", all)),
      ets:insert_new(Table, {ID, Name})
  end,
  first_step(Table, File).

second_step([]) -> ok;
second_step(Servers) ->
  {ok, File} = file:open(?BASIC_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  ok = sendLines(Servers, File, step1),
  file:close(File).

third_step([]) -> ok;
third_step(Servers) ->
  {ok, File} = file:open(?PRINCIPALS_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  ok = sendLines(Servers, File, step2),
  file:close(File).

servers() -> lists:map(fun(X) -> list_to_atom(X) end, read_file(?SERVERS_FILENAME)).

%% find_servers - check each server if alive. Returns a list of server nodes which are alive
find_servers([], Servers) -> Servers;
find_servers([Server | Servers], Nodes) ->
  io:format("~nTrying to connect to ~p...~n", [Server]),
  case net_kernel:connect_node(Server) of
    true ->
      io:format("Connected to ~p.~n~n", [Server]),
      find_servers(Servers, [Server | Nodes]);
    false ->
      io:format("Failed to connect to ~p.~n~n", [Server]),
      find_servers(Servers, Nodes)
  end.

%% read_file - read file as strings separated by lines
read_file(FileName) ->
  try
    {ok, Binary} = file:read_file(FileName),
    string:tokens(erlang:binary_to_list(Binary), "\r\n")
  catch
    error: _Error -> {os:system_time(), error, "Cannot read the file"}
  end.

sendLines(Servers, File, Step) ->
  case io:get_line(File, "") of
    eof -> ok;
    Line ->
      Key = hd(string:split(Line, "\t")),
      ok = gen_server:cast({server, distributeTo(Key, Servers)}, {Step, Line}),
      sendLines(Servers, File, Step)
  end.

loop_names(Table) ->
  receive
    {From, ID} -> spawn(fun() -> fetch_name(From, ID, Table) end)
  end,
  loop_names(Table).

fetch_name(From, ID, Table) ->
  NameId = element(1, string:to_integer(string:sub_string(ID, 3))),
  case ets:lookup(Table, NameId) of
    [] -> From ! not_found;
    [{_, Found}] -> From ! Found
  end.

parseName(Person) ->
  {
    element(1, string:to_integer(string:sub_string(lists:nth(1, Person), 3))),
    lists:nth(2, Person)
  }.

distributeTo(Key, Servers) when is_integer(Key) ->
  Index = rand:uniform(length(Servers)),
  lists:nth(Index, Servers);

distributeTo(Key, Servers) ->
  Index = erlang:phash2(Key, length(Servers)),
  lists:nth(Index + 1, Servers).
