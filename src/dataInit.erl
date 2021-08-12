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
-define(BASIC_FILENAME, "Input Files/basic_short.tsv").
-define(PRINCIPALS_FILENAME, "Input Files/principals_short.tsv").
-define(NAMES_FILENAME, "Input Files/names.tsv").
-define(SERVERS_FILENAME, "Input Files/names.tsv").

-define(NUM_OF_TITLES, 10).
%%-define(NUM_OF_TITLES, 8163533).


start_distribution() ->
  Servers = find_servers(read_file([?SERVERS_FILENAME]), []),

  NamePID = first_step(),
  second_step(Servers),
  third_step(Servers, NamePID),
  Servers.

first_step() ->
  erlang:error(not_implemented).

second_step(Servers) ->
  {ok, File} = file:open(?BASIC_FILENAME, [read]),
  _headers = io:get_line(File, ""),
  second_step(Servers, File, ?NUM_OF_TITLES div length(Servers)).

second_step([], File, _) -> file:close(File);
second_step(Servers, File, LinePerServer) ->
  ok = sendLines(hd(Servers), File, LinePerServer, step1),
  second_step(tl(Servers), File, LinePerServer).

third_step(Servers, NamePID) ->
  {ok, File} = file:open(?PRINCIPALS_FILENAME, [read]),
  _headers = io:get_line(File),
  third_step(Servers, File, NamePID).

third_step([], File, _) -> file:close(File);
third_step(Servers, File, NamePID) ->
  ok = gen_server:cast({serverpid, hd(Servers)}, {namePID, NamePID}),
  Res = sendLines(hd(Servers), File, false, step2),
  case Res of
    ok -> ignore;
    Line -> ok = gen_server:cast({serverpid, tl(hd(Servers))}, {step2, Line})
  end,
  third_step(tl(Servers), File, NamePID).

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

sendLines(_, _, 0, _) -> ok;
sendLines(ServerNode, File, N, step1) ->
  case io:get_line(File, "") of
    eof -> ok;
    Line ->
      ok = gen_server:cast({serverpid, ServerNode}, {step1, Line}),
      case N =:= 1 of
        true -> put(ServerNode, hd(string:split(Line, "\t")));
        false -> ignore
      end,
      sendLines(ServerNode, File, N-1, step1)
  end;

sendLines(ServerNode, File, CurrStop, step2) ->
  case io:get_line(File, "") of
    eof -> ok;
    Line ->
      ok = gen_server:cast({serverpid, ServerNode}, {step2, Line}),
      case get(ServerNode) =:= hd(string:split(Line, "\t")) of
        true -> Stop = true;
        false -> Stop = false
      end,
      case Stop of
        false when CurrStop =:= true -> Line;
        Other -> sendLines(ServerNode, File, Other, step2)
      end
  end.
