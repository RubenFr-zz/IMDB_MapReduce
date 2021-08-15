%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Aug 2021 9:40 PM
%%%-------------------------------------------------------------------
-module(master).
-author("Ruben").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-import(graph, [generate_graph/1, generate_graph/3]).

-define(SERVER, master).
-define(SERVERS_FILENAME, "InputFiles/servers.txt").

-record(master_state, {servers, namePID, upPID, downPID}).
-record(request, {name, type, level}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  {ok, PID} = gen_server:start_link({global, ?SERVER}, ?MODULE, [], []),
  register(master, PID),
  gen_server:cast({master, node()}, start),
  {ok, PID}.

%%%===================================================================
%%% gen_server callbacks - init
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #master_state{}} | {ok, State :: #master_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).

init([]) ->
  io:format("Master started.~n"),
  {ok, #master_state{}}.

%%%===================================================================
%%% gen_server callbacks - handle_call
%%%===================================================================

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #master_state{}) ->
  {reply, Reply :: term(), NewState :: #master_state{}} |
  {reply, Reply :: term(), NewState :: #master_state{}, timeout() | hibernate} |
  {noreply, NewState :: #master_state{}} |
  {noreply, NewState :: #master_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #master_state{}} |
  {stop, Reason :: term(), NewState :: #master_state{}}).

handle_call({name, NameID}, From, State = #master_state{}) ->
  case State#master_state.namePID of
    undefined -> {reply, not_available, State};
    PID -> 
      spawn_monitor(fun() -> gen_server:reply(From, fetch_name(NameID, PID)) end),
      {noreply, State}
  end;

%% Type = cast | title (movie/show/short)
handle_call({request, Request = #request{}}, From, State = #master_state{}) ->
  spawn_monitor(fun() -> process_request(Request, From, State#master_state.servers) end),
  {noreply, State};

handle_call({request, Name, Type, Level}, From, State = #master_state{}) ->
  Request = #request{name = Name, type = Type, level = Level},
  spawn_monitor(fun() -> process_request(Request, From, State#master_state.servers) end),
  {noreply, State};

handle_call(request, From, State = #master_state{}) ->
  Request = #request{name = "Carmencita", type = title, level = 3},
  spawn_monitor(fun() -> process_request(Request, From, State#master_state.servers) end),
  {noreply, State};

handle_call(_Request, _From, State = #master_state{}) ->
  {reply, ok, State}.

%%%===================================================================
%%% gen_server callbacks - handle_cast
%%%===================================================================

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #master_state{}) ->
  {noreply, NewState :: #master_state{}} |
  {noreply, NewState :: #master_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #master_state{}}).

handle_cast(start, State = #master_state{}) ->
  Start = os:timestamp(),

  {NodesAlive, NamePID} = dataInit:start_distribution(),
    
  broadcast(NodesAlive, cast, stop_init),
  UpPID = spawn(fun() -> check_server_up([], dataInit:servers() -- NodesAlive) end),
  DownPID = spawn(fun() -> check_server_down([], NodesAlive) end),
  
  io:format("Distributed data to ~p servers in ~p ms.~n", [length(NodesAlive), round(timer:now_diff(os:timestamp(), Start) / 1000)]),
  {noreply, State#master_state{servers = NodesAlive, namePID = NamePID, upPID = UpPID, downPID = DownPID}};

handle_cast({distribute, Node, FileName}, State = #master_state{}) ->
  Reply = pmap:map(
    fun(S) -> 
      {ok, Bin} = file:read_file(FileName),
      ok = rpc:call(S, file, write_file, [FileName, Bin])
      % ok = file:delete(FileName) 
    end,
    State#master_state.servers -- [Node]),
  io:format("Distributed data from ~p to other servers -> ~p~n", [Node, Reply]),
  {noreply, State};

handle_cast({server_up, Node},  State = #master_state{}) ->
  NewList = [Node | State#master_state.servers],

  State#master_state.downPID ! {new_node, Node}, % Update the list of servers to check if down
  broadcast(NewList, pcall, reset),           % Tell to all the servers to discard the data they had until now
  dataInit:distribute(NewList),               % Distribute the data to the servers in NewList
  broadcast(NewList, cast, stop_init),     % Tell to all the servers the init is finished

  io:format("~nNew Server is UP: ~p\t~p Server(s) Running!~n~n", [Node, length(NewList)]),
  {noreply, State#master_state{servers = NewList}};

handle_cast({server_down, Node},  State = #master_state{}) ->
  NewList = State#master_state.servers -- [Node],

  State#master_state.upPID ! {new_node, Node}, % Update the list of servers to check if up
  dataInit:redistribute(NewList, Node),     % Tell to one of the servers running to handle the data of the server down
  
  io:format("~nServer ~p is DOWN\t~p Server(s) Running!~n~n", [Node, length(NewList)]),
  {noreply, State#master_state{servers = NewList}};

handle_cast(_Request, State = #master_state{}) ->
  {noreply, State}.

%%%===================================================================
%%% gen_server callbacks - handle_info
%%%===================================================================

%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #master_state{}) ->
  {noreply, NewState :: #master_state{}} |
  {noreply, NewState :: #master_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #master_state{}}).

handle_info(_Info, State = #master_state{}) ->
  {noreply, State}.

%%%===================================================================
%%% gen_server callbacks - terminate
%%%===================================================================

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #master_state{}) -> term()).
terminate(_Reason, _State = #master_state{}) ->
  ok.

%%%===================================================================
%%% gen_server callbacks - code_change
%%%===================================================================

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #master_state{},
    Extra :: term()) ->
  {ok, NewState :: #master_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #master_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

fetch_name(NameID, DB) ->
  % io:format("Receive name request (from ~p): ~p~n", [From, NameID]),
  DB ! {self(), NameID},
  receive
    Name -> Name
  end.

broadcast([], _, _) -> ok;
broadcast(Servers, Type, Message) ->
  io:format("Sending Message to ~p (~p servers).~n", [Servers, length(Servers)]),
  case Type of
    cast -> lists:foreach(fun(S) -> gen_server:cast({server, S}, Message) end, Servers);
    call -> lists:map(fun(S) -> gen_server:call({server, S}, Message) end, Servers);
    pcall -> pmap:map(fun(S) -> gen_server:call({server, S}, Message, 10000) end, Servers);
    _ -> io:format("ERROR")
  end.


process_request(Request = #request{}, From, Servers) ->
  io:format("Received a new Request: Name = ~p\tType = ~p\tLevel = ~p~n", [Request#request.name, Request#request.type, Request#request.level]),
  G = digraph:new(),
  Root = digraph:add_vertex(G, Request#request.name, Request#request.name),
  Set = sets:new(),
  process_request(G, Root, sets:add_element(Request#request.name, Set), Request, 1, Servers),
  gen_server:reply(From, {length(digraph:edges(G)), length(digraph:vertices(G))}),
  FileName = generate_graph(G, Root, Request#request.type),
  % FileName = generate_graph(G),
  Reply = os:cmd(io_lib:format("xdg-open ~s", [FileName])),
  io:format("Opening file ~s: ~p~n", [FileName, Reply]).

process_request(_, _, _, Request = #request{}, Level, _) when Level =:= Request#request.level -> ok;

process_request(G, V, Set, Request = #request{}, Level, Servers) ->
  Children = parse_reply(broadcast(Servers, pcall, {request, Request#request{name = V, level = Level}})),
  {AddedVertices, UpdatedSet} = update_graph(G, V, Set, Children, []),
  io:format("Level ~p: ~s~n", [Level, string:join(AddedVertices, ", ")]),
  lists:foreach(fun(Vertex) -> process_request(G, Vertex, UpdatedSet, Request, Level + 1, Servers) end, AddedVertices).


update_graph(_, _, Set, [], V) -> {V, Set};
update_graph(G, V, Set, [Name | Next], Vertices) ->
  case sets:is_element(Name, Set) of
    true -> update_graph(G, V, Set, Next, Vertices);
    false ->
      V1 = digraph:add_vertex(G, Name, Name),
      digraph:add_edge(G, V, V1),
      update_graph(G, V, sets:add_element(Name, Set), Next, [V1 | Vertices])
  end.


parse_reply(List) -> parse_reply(List, []).
parse_reply([], Reply) -> sets:to_list(sets:from_list(Reply));
parse_reply([badarg | _], _) -> badarg;
parse_reply([notfound | Next], Reply) -> parse_reply(Next, Reply);
parse_reply([R | Next], Reply) -> parse_reply(Next, R ++ Reply). 


check_server_up([], []) ->
  receive
    NewNode when is_atom(NewNode) -> check_server_up(NewNode, [])
  end;

check_server_up([], Servers) -> check_server_up(Servers, []);

check_server_up([ToCheck | Next], Servers) ->
  case net_kernel:connect_node(ToCheck) of
    true -> 
      % io:format("Server ~p is UP!~n", [ToCheck]),
      gen_server:cast({master, node()}, {server_up, ToCheck}),
      check_server_up(Next, Servers);
    false -> 
      receive
        {new_node, NewNode} -> 
          % io:format("Up Checker: New Node to check ~p~n", [NewNode]),
          check_server_up(Next, [ToCheck, NewNode | Servers])
      after
        1000 -> check_server_up(Next, [ToCheck | Servers])
      end
  end.

check_server_down([], []) ->
  receive
    {new_node, NewNode} -> check_server_down(NewNode, [])
  end;

check_server_down([], Servers) -> check_server_down(Servers, []);

check_server_down([ToCheck | Next], Servers) ->
  case net_kernel:connect_node(ToCheck) of
    false -> 
      % io:format("Server ~p is DOWN!~n", [ToCheck]),
      gen_server:cast({master, node()}, {server_down, ToCheck}),
      check_server_down(Next, Servers);
    true -> 
      receive
        {new_node, NewNode} -> 
          % io:format("Down Checker: New Node to check ~p~n", [NewNode]),
          check_server_down(Next, [ToCheck, NewNode | Servers])
      after
        1000 -> check_server_down(Next, [ToCheck | Servers])
      end
  end.