%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Aug 2021 5:41 PM
%%%-------------------------------------------------------------------
-module(graph).
-author("Ruben").

%% API
-export([generate_graph/1, generate_graph/3]).

generate_graph(G) ->
  ID = erlang:unique_integer([positive]),  % unique id

  graphviz:digraph(io_lib:format("G_~p", [ID])),  % Create graph

  lists:map(
    fun(E) ->
      {_, N1, N2, _} = digraph:edge(G, E),
      graphviz:add_edge(re:replace(N1, "[.' ]", "_",[{return,list},global]), re:replace(N2, "[.' ]", "_",[{return,list},global]))
    end, digraph:edges(G)
  ),

  FileName = io_lib:format("Tree_~p.png", [ID]),
  graphviz:to_file(FileName, "png"),
  io:format("New Graph available at: ~s~n", [FileName]),
  graphviz:delete(),
  FileName.

generate_graph(G, Root, Type) ->
  ID = erlang:unique_integer([positive]),  % unique id

  graphviz:digraph(io_lib:format("G_~p", [ID])),  % Create graph
  graphviz:add_node(re:replace(Root, "[.' ]", "_",[{return,list},global])),

  add_neighbours(G, Root, Type),
  FileName = io_lib:format("Tree_~p.png", [ID]),
  graphviz:to_file(FileName, "png"),
  io:format("New Graph available at: ~s~n", [FileName]),
  graphviz:delete(),
  FileName.

add_neighbours(G, Root, Type) ->
  case found_children(G, Root, Type) of
    [] -> ok;
    Neighbours ->
      RootJ = re:replace(Root, "[.' ]", "_",[{return,list},global]),
      lists:map(
        fun(V) ->
          Vertex = re:replace(V, "[.' ]", "_",[{return,list},global]),
          graphviz:add_node(Vertex),
          graphviz:add_edge(RootJ, Vertex),
          % graphviz:add_edge(Root, V),
          add_neighbours(G, V, Type)
        end, Neighbours)
  end.

found_children(G, Root, Type) ->
  OutNeighbours = digraph:out_neighbours(G, Root),
  delete_edges(G, Root),
  case Type of
    title -> lists:sort(OutNeighbours);
    cast -> lists:sort(
        fun(X1, X2) -> 
          S1 = lists:last(string:split(X1, " ", trailing)),
          S2 = lists:last(string:split(X2, " ", trailing)),
          S1 < S2
        end, OutNeighbours);
    _ -> []
  end.

delete_edges(G, Root) -> digraph:del_edges(G, digraph:out_edges(G, Root)).
