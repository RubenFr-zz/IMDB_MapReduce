%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, BGU
%%% @doc
%%%
%%% @end
%%% Created : 15. Aug 2021 5:41 PM
%%%-------------------------------------------------------------------
-module(graph).
-author("Ruben").

%% API
-export([generate_graph/1, generate_graph/3]).

%%%===================================================================
%%% Internal functions - generate_graph/1
%%%===================================================================

%% @doc Create a graphviz based on the edges in the Graph G. Return the Name of the File created
-spec generate_graph(G :: digraph:graph()) -> FileName :: string().

generate_graph(G) ->
  ID = erlang:unique_integer([positive]),  % unique id

  graphviz:digraph(io_lib:format("G_~p", [ID])),  % Create graph

  lists:map(
    fun(E) ->
      {_, N1, N2, _} = digraph:edge(G, E),
      graphviz:add_edge(re:replace(N1, "[^A-Za-z1-9]", "_", [global, {return, list}, global]), re:replace(N2, "[^A-Za-z1-9]", "_", [global, {return, list}, global]))
    end, digraph:edges(G)
  ),

  FileName = io_lib:format("Tree_~p.png", [ID]),
  graphviz:to_file(FileName, "png"),
  io:format("New Graph available at: ~s~n", [FileName]),
  graphviz:delete(),
  FileName.

%%%===================================================================
%%% Internal functions - generate_graph/3
%%%===================================================================

%% @doc Create a graphviz based on the edges in the Graph G in graphical order. Return the Name of the File created
-spec generate_graph(G :: digraph:graph(), Root :: digraph:vertex(), Type :: movie | actor) -> FileName :: string().

generate_graph(G, Root, Type) ->
  ID = erlang:unique_integer([positive]),  % unique id

  graphviz:digraph(io_lib:format("G_~p", [ID])),  % Create graph
  graphviz:add_node(re:replace(Root, "[^A-Za-z1-9]", "_", [global, {return, list}, global])),

  add_neighbours(G, Root, Type),
  FileName = io_lib:format("Tree_~p.png", [ID]),
  graphviz:to_file(FileName, "png"),
  io:format("New Graph available at: ~s~n", [FileName]),
  graphviz:delete(),
  FileName.

%%%===================================================================
%%% Internal functions - add_neighbours/3
%%%===================================================================

%% @doc Add to the graphviz all the children of Root
-spec add_neighbours(G :: digraph:graph(), Root :: digraph:vertex(), Type :: movie | actor) -> ok.

add_neighbours(G, Root, Type) ->
  case found_children(G, Root, Type) of
    [] -> ok;
    Neighbours ->
      RootJ = re:replace(Root, "[^A-Za-z1-9]", "_", [global, {return, list}, global]),
      lists:foreach(
        fun(V) ->
          Vertex = re:replace(V, "[^A-Za-z1-9]", "_", [global, {return, list}, global]),
          graphviz:add_node(Vertex),
          graphviz:add_edge(RootJ, Vertex),
          add_neighbours(G, V, Type)
        end, Neighbours)
  end.

%%%===================================================================
%%% Internal functions - found_children/3
%%%===================================================================

%% @doc Find all the children of Root in G and sort them in graphical order
-spec found_children(G :: digraph:graph(), Root :: digraph:vertex(), Type :: movie | actor) -> [V :: digraph:vertex()].

found_children(G, Root, Type) ->
  OutNeighbours = digraph:out_neighbours(G, Root),
  delete_edges(G, Root),
  case Type of
    movie -> lists:sort(OutNeighbours);
    actor -> lists:sort(
      fun(X1, X2) ->
        S1 = lists:last(string:split(X1, " ", trailing)),
        S2 = lists:last(string:split(X2, " ", trailing)),
        S1 < S2
      end, OutNeighbours);
    _ -> []
  end.

%%%===================================================================
%%% Internal functions - delete_edges/2
%%%===================================================================

%% @doc Delete all the out edges of Root in G
-spec delete_edges(G :: digraph:graph(), Root :: digraph:vertex()) -> true.

delete_edges(G, Root) -> digraph:del_edges(G, digraph:out_edges(G, Root)).
