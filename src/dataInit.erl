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

%% API
-export([start_distribution/0]).

%% CONSTANTS
-define(BASIC_FILENAME, "InputFiles/basic1000.tsv").
-define(PRINCIPALS_FILENAME, "InputFiles/principals1000.tsv").
-define(NAMES_FILENAME, "InputFiles/names1000.tsv").
-define(SERVERS_FILENAME, "InputFiles/servers.txt").


start_distribution() ->
  Servers = find_servers(read_file([?SERVERS_FILENAME]), []),
  io:format("Found ~p server(s): ~p~n", [length(Servers), Servers]),

  NamePID = first_step(),
  io:format("First Step started...~n"),
  io:format("Second Step started...~n"),
  second_step(Servers),
  io:format("Second Step finished.~n"),
  io:format("Third Step started...~n"),
  third_step(Servers),
  io:format("Third Step finished.~n"),
  {Servers, NamePID}.

first_step() -> spawn(fun() -> first_step(ets:new(names, [set, public, {read_concurrency, true}])) end).

first_step(Table) ->
  {ok, File} = file:open(?NAMES_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  first_step(Table, File).

first_step(Table, File) ->
  case io:get_line(File, "") of
    eof ->
      io:format("First Step finished.~n"),
      file:close(File),
      loop_names(Table);
    Line ->
      {ID, Name} = parseName(string:split(Line, "\t", all)),
      % io:format("Added new name: ~p -> ~p~n", [ID, Name]),
      ets:insert_new(Table, {ID, Name})
  end,
  first_step(Table, File).

second_step(Servers) ->
  {ok, File} = file:open(?BASIC_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  ok = sendLines(Servers, File, step1),
  file:close(File).

third_step(Servers) ->
  {ok, File} = file:open(?PRINCIPALS_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  ok = sendLines(Servers, File, step2),
  file:close(File).

%% find_servers - check each server if alive. Returns a list of server nodes which are alive
find_servers([], Servers) -> Servers;
find_servers([Server | Servers], Nodes) ->
  Node = list_to_atom(Server),
  case net_kernel:connect_node(Node) of
    true -> find_servers(Servers, [Node | Nodes]);
    false -> find_servers(Servers, Nodes)
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
    {From, Name} -> spawn(fun() -> fetch_name(From, Name, Table) end)
  end,
  loop_names(Table).

fetch_name(From, Name, Table) ->
  NameId = element(1, string:to_integer(string:sub_string(Name, 3))),
  case ets:lookup(Table, NameId) of
    [] -> From ! not_found;
    [{_, Found}] -> From ! Found
  end.

parseName(Person) ->
  {
    element(1, string:to_integer(string:sub_string(lists:nth(1, Person), 3))),
    lists:nth(2, Person)
  }.

distributeTo(Key, Servers) ->
  Index = erlang:phash2(Key, length(Servers)),
  lists:nth(Index + 1, Servers).