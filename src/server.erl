%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Aug 2021 10:04 PM
%%%-------------------------------------------------------------------
-module(server).
-author("Ruben").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(MASTER_NODE, 'master@132.72.104.125').

-record(server_state, {imdb, namePid}).
-record(title, {id, title, type, genres, cast}).
-record(request, {name, type, level}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  {ok, PID} = gen_server:start_link({global, node()}, ?MODULE, [], []),
  register(server, PID),
  {ok, PID}.

%%%===================================================================
%%% gen_server callbacks - init
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #server_state{}} | {ok, State :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).

init([]) ->
  io:format("Server started.~n"),
  {ok, #server_state{imdb = ets:new(imdb, [ordered_set, public, {read_concurrency, true}])}}.

%%%===================================================================
%%% gen_server callbacks - handle_call
%%%===================================================================

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #server_state{}) ->
  {reply, Reply :: term(), NewState :: #server_state{}} |
  {reply, Reply :: term(), NewState :: #server_state{}, timeout() | hibernate} |
  {noreply, NewState :: #server_state{}} |
  {noreply, NewState :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #server_state{}} |
  {stop, Reason :: term(), NewState :: #server_state{}}).

handle_call({request, Request = #request{}}, From, State = #server_state{}) ->
  spawn(fun() -> process_request(Request, From, State#server_state.imdb) end),
  {noreply, State};

handle_call(_Request, _From, State = #server_state{}) ->
  {reply, ok, State}.

%%%===================================================================
%%% gen_server callbacks - handle_cast
%%%===================================================================

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #server_state{}) ->
  {noreply, NewState :: #server_state{}} |
  {noreply, NewState :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #server_state{}}).

handle_cast({step1, Line}, State = #server_state{}) ->
  Data = string:split(Line, "\t", all),
  Title = parseTitle(Data),
  ets:insert_new(State#server_state.imdb, {Title#title.id, Title}),
  {noreply, State};

handle_cast({step2, Line}, State = #server_state{}) ->
  Data = string:split(Line, "\t", all),
  {ID, NameID} = parsePrincipal(Data),
  case fetch_name(NameID) of
    not_found -> pass;
    Name -> 
      case ets:lookup(State#server_state.imdb, ID) of
        [] -> io:format("ID = ~p NOT FOUND!", [ID]);
        [{_, Title = #title{}}] -> 
          ets:insert(State#server_state.imdb, {ID, Title#title{cast = [Name | Title#title.cast]}})
      end
  end,
  {noreply, State};

handle_cast(stop_init, State = #server_state{}) ->
  FileName = "table_" ++ hd(string:split(atom_to_list(node()), "@")),
  
  % Save the table in two forms
  print_to_file(State#server_state.imdb, FileName ++ [".txt"]),
  ets:tab2file(State#server_state.imdb, FileName),

  %% Send a copy of the table to the master to distribute it to other severs for backup
  {ok, Bin} = file:read_file(FileName), % Send tab2file
  % {ok, Bin} = file:read_file(FileName ++ [".tsv"]), % Send text file
  Reply = rpc:call(?MASTER_NODE, file, write_file, [FileName, Bin]),
  gen_server:cast({master, ?MASTER_NODE}, {distribute, node(), FileName}),
  io:format("~nTable sent to master: ~p~n", [Reply]),

  io:format("Table available at ~s~n~n", [FileName ++ [".txt"]]),
  {noreply, State};

handle_cast(_Request, State = #server_state{}) ->
  {noreply, State}.

%%%===================================================================
%%% gen_server callbacks - handle_info
%%%===================================================================

%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #server_state{}) ->
  {noreply, NewState :: #server_state{}} |
  {noreply, NewState :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #server_state{}}).

handle_info(_Info, State = #server_state{}) ->
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
    State :: #server_state{}) -> term()).

terminate(_Reason, _State = #server_state{}) ->
  ok.

%%%===================================================================
%%% gen_server callbacks - code_change
%%%===================================================================

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #server_state{},
    Extra :: term()) ->
  {ok, NewState :: #server_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #server_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

parseTitle(Title) ->
  #title{
    id = element(1, string:to_integer(string:sub_string(lists:nth(1, Title), 3))),
    title = lists:nth(4, Title),
    type = lists:nth(2, Title),
    genres = string:trim(lists:nth(9, Title)),
    cast = []
  }.

parsePrincipal(Principal) ->
  { 
    element(1, string:to_integer(string:sub_string(lists:nth(1, Principal), 3))), 
    lists:nth(3, Principal)
  }.

fetch_name(NameID) ->
  gen_server:call({master, ?MASTER_NODE}, {name, NameID}).

process_request(Request = #request{}, From, TableRef) ->
  io:format("Received a new Request: Name = ~p\tType = ~p~n", [Request#request.name, Request#request.type]),
  % {ok, TableRef} = ets:file2tab(TableName),
  case Request#request.type of
    cast -> Reply = cast_request(Request, TableRef);
    title -> Reply = title_request(Request, TableRef);
    _other -> Reply = badarg
  end,
  gen_server:reply(From, Reply).

cast_request(Request = #request{}, TableRef) ->
  cast_request(Request#request.name, sets:new(), TableRef, ets:first(TableRef)).

title_request(Request = #request{}, TableRef) ->
  case find_cast(Request#request.name, TableRef) of
    notfound -> notfound;
    Cast -> title_request(Request#request.name, Cast, sets:new(), TableRef, ets:first(TableRef))
  end.

cast_request(Name, Set, _, '$end_of_table') -> sets:to_list(sets:del_element(Name, Set));
cast_request(Name, Set, TableRef, Key) ->
  Title = ets:lookup_element(TableRef, Key, 2),
  % io:format("Looking for: ~p\tCast: ~p~n", [Name, Title#title.cast]),
  case lists:member(Name,Title#title.cast) of
    false -> cast_request(Name, Set, TableRef, ets:next(TableRef, Key));
    true ->  
      io:format("Looking for: ~p\tFound: ~p~n", [Name, Title#title.cast]),
      cast_request(Name, sets:union(Set, sets:from_list(Title#title.cast)), TableRef, ets:next(TableRef, Key))
  end.

title_request(Name, _, Set, _, '$end_of_table') -> sets:to_list(sets:del_element(Name, Set));
title_request(Name, Cast, Set, TableRef, Key) ->
  Title = ets:lookup_element(TableRef, Key, 2),
  case lists:foldr(fun(X, Acc) -> Acc or sets:is_element(X, Cast) end, false, Title#title.cast) of
    false -> title_request(Name, Cast, Set, TableRef, ets:next(TableRef, Key));
    true ->  title_request(Name, Cast, sets:add_element(Title#title.title, Set), TableRef, ets:next(TableRef, Key))
  end.

find_cast(Title, TableRef) ->
  find_cast(Title, TableRef, ets:first(TableRef)).

find_cast(_, _, '$end_of_table') -> notfound;
find_cast(Name, TableRef, Key) ->
  Title = ets:lookup_element(TableRef, Key, 2),
  
  case Name == Title#title.title of
    false -> find_cast(Name, TableRef, ets:next(TableRef, Key));
    true -> lists:foldl(
              fun(X, S) -> sets:add_element(X, S) end, 
              sets:new(), Title#title.cast)
  end.

print_to_file(TableRef, FileName) -> 
  try
    file:delete(FileName)
  catch
    error: _Error -> {os:system_time(), error, io_lib:format("File ~p doesn't exist", [FileName])}
  end,
  print_to_file(FileName, TableRef, ets:first(TableRef)).

print_to_file(_, _, '$end_of_table') -> ok;
print_to_file(FileName, TableRef, Key) ->
  Title = ets:lookup_element(TableRef, Key, 2),
  ok = file:write_file(FileName, io_lib:format("~p\t~s\t~s\t~s\t~s~n", [Title#title.id, Title#title.title, Title#title.type, Title#title.genres, string:join(Title#title.cast, ",")]), [write, append]),
  print_to_file(FileName, TableRef, ets:next(TableRef, Key)).