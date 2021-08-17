-module(graphviz).
-export([digraph/1, graph/1, delete/0, add_node/1, add_edge/2, graph_server/1, to_dot/1, to_file/2]).

% -- Constructor
digraph(Id) ->
  register(graph_server, spawn(?MODULE, graph_server, [{Id, {digraph, "->"}, [], [], []}])).

graph(Id) ->
  register(graph_server, spawn(?MODULE, graph_server, [{Id, {graph, "--"}, [], [], []}])).

% -- Destructor
delete() ->
  graph_server ! stop.

% -- Server/Dispatcher
graph_server(Graph) ->
  receive
    {add_node, Id} ->
      graph_server(add_node(Graph, Id));

    {add_edge, NodeOne, NodeTwo} ->
      graph_server(add_edge(Graph, NodeOne, NodeTwo));

    {to_dot, File} ->
      to_dot(Graph, File),
      graph_server(Graph);

    {to_file, File, Format, PID} ->
      to_file(Graph, File, Format),
      PID ! ok,
      graph_server(Graph);

    {value, Pid} ->
      Pid ! {value, Graph},
      graph_server(Graph);

    stop -> true
  end.

% -- Methods

add_node(Id) -> graph_server ! {add_node, Id}.
add_edge(NodeOne, NodeTwo) -> graph_server ! {add_edge, NodeOne, NodeTwo}.
to_dot(File) -> graph_server ! {to_dot, File}.
to_file(File, Format) ->
  graph_server ! {to_file, File, Format, self()},
  receive
    ok -> done
  end.

% -- Implementation

add_node(Graph, Id) ->
  {GraphId, Type, GraphOptions, Nodes, Edges} = Graph,
  io:format("Add node ~s to graph ~s !~n", [Id, GraphId]),
  {GraphId, Type, GraphOptions, Nodes ++ [Id], Edges}.

add_edge(Graph, NodeOne, NodeTwo) ->
  {GraphId, Type, GraphOptions, Nodes, Edges} = Graph,
  io:format("Add edge ~s -> ~s to graph ~s !~n", [NodeOne, NodeTwo, GraphId]),
  {GraphId, Type, GraphOptions, Nodes, Edges ++ [{NodeOne, NodeTwo}]}.

to_dot(Graph, File) ->
  {GraphId, Type, _GraphOptions, Nodes, Edges} = Graph,
  {GraphType, EdgeType} = Type,

  % open file
  {ok, IODevice} = file:open(File, [write]),

  % print graph
  io:format(IODevice, "~s ~s {~n", [GraphType, GraphId]),

  % print nodes
  lists:foreach(
    fun(Node) ->
      io:format(IODevice, "  ~s;~n", [Node])
    end,
    Nodes
  ),

  % print edges
  lists:foreach(
    fun(Edge) ->
      {NodeOne, NodeTwo} = Edge,
      io:format(IODevice, "  ~s ~s ~s;~n", [NodeOne, EdgeType, NodeTwo])
    end,
    Edges
  ),

  % close file
  io:format(IODevice, "}~n", []),
  file:close(IODevice).

to_file(Graph, File, Format) ->
  ID = erlang:unique_integer([positive]),
  DotFile = lists:concat([File, ".dot-", ID]),
  to_dot(Graph, DotFile),
  DotCommand = lists:concat(["dot -T", Format, " -Nshape=rect -o", File, " ", DotFile]),
  os:cmd(DotCommand),
  file:delete(DotFile).

