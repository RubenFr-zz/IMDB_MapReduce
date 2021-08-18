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
-export([start_distribution/0, distribute/1, redistribute/2]).

%%%===================================================================
%%% Internal functions - start_distribution/0
%%%===================================================================

%% @doc Executed during init of master. Find Running servers and distribute the data
-spec start_distribution() -> {Servers :: [S :: atom()], NamePID :: pid()}.

start_distribution() ->
  Servers = find_servers(?SERVERS, []),
  io:format("Found ~p server(s): ~p~n", [length(Servers), Servers]),

  NamePID = first_step(),
  distribute(Servers),
  {Servers, NamePID}.

%%%===================================================================
%%% Internal functions - distribute/1
%%%===================================================================

%% @doc Distribute to the Running Servers the data
-spec distribute(Servers :: [S :: atom()]) -> ok.

distribute(Servers) ->
  second_step(Servers),
  third_step(Servers),
  io:format("Data Distributed to ~p (~p servers).~n", [Servers, length(Servers)]).

%%%===================================================================
%%% Internal functions - redistribute/2
%%%===================================================================

%% @doc In case a server is DOWN, ask for a running server to merge its data with the DOWN server
-spec redistribute(Servers :: [S :: atom()], Down :: atom()) -> ok.

redistribute([], _) -> ok;
redistribute(Servers, Down) ->
  Chosen = distribute_to(Down, Servers),
  case gen_server:call({server, Chosen}, {merge, Down}) of
    ack -> ok;
    {error, _} -> redistribute(Servers -- [distribute_to(Down, Servers)], Down)
  end.

%%%===================================================================
%%% Internal functions - first_step/0
%%%===================================================================

%% @doc Load the names DB and start a process to return the associated names when needed
-spec first_step() -> pid().

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
      {ID, Name} = parse_name(string:split(Line, "\t", all)),
      ets:insert_new(Table, {ID, Name})
  end,
  first_step(Table, File).

%%%===================================================================
%%% Internal functions - second_step/1
%%%===================================================================

%% @doc Send to the servers data from the Basic.tsv file
-spec second_step(Servers :: [S :: atom()]) -> ok.

second_step([]) -> ok;
second_step(Servers) ->
  {ok, File} = file:open(?BASIC_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  ok = send_lines(Servers, File, step1),
  file:close(File).

%%%===================================================================
%%% Internal functions - third_step/1
%%%===================================================================

%% @doc Send to the servers data from the Principals.tsv file
-spec third_step(Servers :: [S :: atom()]) -> ok.

third_step([]) -> ok;
third_step(Servers) ->
  {ok, File} = file:open(?PRINCIPALS_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  ok = send_lines(Servers, File, step2),
  file:close(File).

%%%===================================================================
%%% Internal functions - find_servers/1
%%%===================================================================

%% @doc Check each server if alive. Returns a list of server nodes which are running
-spec find_servers(Servers :: [S :: atom()], Nodes :: [N :: atom()]) -> Nodes.

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

%%%===================================================================
%%% Internal functions - send_lines/3
%%%===================================================================

%% @doc Send line to corresponding server
-spec send_lines(Servers :: [S :: atom()], File :: device(), Step :: step1 | step2) -> ok.

send_lines(Servers, File, Step) ->
  case io:get_line(File, "") of
    eof -> ok;
    Line ->
      Key = hd(string:split(Line, "\t")),
      ok = gen_server:cast({server, distribute_to(Key, Servers)}, {Step, Line}),
      send_lines(Servers, File, Step)
  end.

%%%===================================================================
%%% Internal functions - loop_names/1
%%%===================================================================

%% @doc Process that returns the name associated to the ID
-spec loop_names(Table :: tab()) -> ok.

loop_names(Table) ->
  receive
    {From, ID} ->
      spawn(fun() -> fetch_name(From, ID, Table) end),
      loop_names(Table);
    stop -> ok
  end.

%%%===================================================================
%%% Internal functions - fetch_name/3
%%%===================================================================

%% @doc Find the name in the table. Return not_found if ID not in table.
-spec fetch_name(From :: {Pid :: pid(), Tag :: term()}, ID :: string(), Table :: tab()) -> not_found | Found :: string().

fetch_name(From, ID, Table) ->
  NameId = element(1, string:to_integer(string:sub_string(ID, 3))),
  case ets:lookup(Table, NameId) of
    [] -> From ! not_found;
    [{_, Found}] -> From ! Found
  end.

%%%===================================================================
%%% Internal functions - parse_name/1
%%%===================================================================

%% @doc Parse a line from the name file
-spec parse_name(Person :: [string()]) -> {Id :: integer(), Name :: string()}.

parse_name(Person) ->
  {
    element(1, string:to_integer(string:sub_string(lists:nth(1, Person), 3))),
    lists:nth(2, Person)
  }.

%%%===================================================================
%%% Internal functions - distribute_to/2
%%%===================================================================

%% @doc Return a random or hash based server from the list
-spec distribute_to(K :: term(), Servers :: [S :: atom()]) -> S.

distribute_to(Key, Servers) when is_integer(Key) ->
  Index = rand:uniform(length(Servers)),
  lists:nth(Index, Servers);

distribute_to(Key, Servers) ->
  Index = erlang:phash2(Key, length(Servers)),
  lists:nth(Index + 1, Servers).
