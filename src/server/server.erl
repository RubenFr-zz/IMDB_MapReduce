%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, BGU
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

-import(mapreduce, [mapreduce/3]).

-define(SERVER, ?MODULE).
%%-define(MASTER_NODE, 'master@132.72.104.125').
-define(MASTER_NODE, 'master@ubuntu').

-record(server_state, {titles_db, actors_db}).
-record(title, {id, title, type, genres, cast}).
-record(request, {name, type, level}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the global name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  {ok, Pid} = gen_server:start_link({global, node()}, ?MODULE, [], []),
  register(server, Pid),
  {ok, Pid}.

%%%===================================================================
%%% gen_server callbacks - init
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #server_state{}} | {ok, State :: #server_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).

init([]) ->
  delete_all_backups(),
  io:format("Server started.~n"),
  {ok, #server_state{
    titles_db = ets:new(title_db, [ordered_set, public, {read_concurrency, true}]),
    actors_db = ets:new(name_db, [ordered_set, public, {read_concurrency, true}])}}.

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

%% Job request -> Request = #request{}
handle_call({request, Request = #request{}}, From, State = #server_state{}) ->
  spawn(fun() -> process_request(Request, From, State) end),
  {noreply, State};

%% Reset request -> delete all backups and db on the node
handle_call(reset, _From, State = #server_state{}) ->
  io:format("~n~nRESET REQUESTED!~n~n"),
  delete_all_backups(),
  ets:delete(State#server_state.titles_db),
  ets:delete(State#server_state.actors_db),
  {reply, ack, #server_state{
    titles_db = ets:new(title_db, [ordered_set, public, {read_concurrency, true}]),
    actors_db = ets:new(name_db, [ordered_set, public, {read_concurrency, true}])}};

%% Merge request -> merge the current db with the backup from Node
handle_call({merge, Node}, _From, State = #server_state{}) ->
  io:format("~nMerging with data base of ~p~n~n", [Node]),
  Reply = merge_db(State, Node),
  {reply, Reply, State};

%% Other request -> not handled
handle_call(_Request, _From, State = #server_state{}) ->
  {reply, unknown_request, State}.

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
  Title = parse_title(Data),
  ets:insert_new(State#server_state.titles_db, {Title#title.id, Title}),
  {noreply, State};

handle_cast({step2, Line}, State = #server_state{titles_db = MoviesTable, actors_db = ActorsTable}) ->
  Data = string:split(Line, "\t", all),
  {ID, NameID, Role} = parse_principal(Data),
  if
    Role =:= "actor" orelse Role =:= "actress" ->
      case fetch_name(NameID) of
        not_found -> pass;
        Name ->
          case {ets:lookup(MoviesTable, ID), ets:lookup(ActorsTable, Name)} of
            {[], _} -> io:format("ID = ~p NOT FOUND!", [ID]);
            {[{ID, Title = #title{cast = Cast}}], []} ->
              ets:insert(MoviesTable, {ID, Title#title{cast = [Name | Cast]}}),
              ets:insert_new(ActorsTable, {Name, [Title#title.title]});
            {[{ID, Title = #title{cast = Cast}}], [{Name, Movies}]} ->
              ets:insert(MoviesTable, {ID, Title#title{cast = [Name | Cast]}}),
              ets:insert(ActorsTable, {Name, [Title#title.title | Movies]})
          end
      end;
    true -> ok
  end,
  {noreply, State};

handle_cast(stop_init, State = #server_state{titles_db = TitlesTable, actors_db = ActorsTable}) ->
  io:format("Init Terminated.~n"),
  ID = hd(string:split(atom_to_list(node()), "@")),
  NewTitlesTable = change_key(TitlesTable),

  % Save the movies table in two forms
  io:format("Saving Movies Table -> ~p elements~n", [ets:info(NewTitlesTable, size)]),
  tab2file(NewTitlesTable, "table_movies_" ++ ID ++ ".txt"),
  ets:tab2file(NewTitlesTable, "table_movies_" ++ ID),
  send_file_to_master("table_movies_" ++ ID),

  % Save the actors table in two forms
  io:format("Saving Actors Table -> ~p elements~n", [ets:info(ActorsTable, size)]),
  tab2file(ActorsTable, "table_actors_" ++ ID ++ ".txt"),
  ets:tab2file(ActorsTable, "table_actors_" ++ ID),
  send_file_to_master("table_actors_" ++ ID),

  {noreply, State#server_state{titles_db = NewTitlesTable}};

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

terminate(Reason, _State = #server_state{}) ->
  gen_server:cast({master, ?MASTER_NODE}, {server_down, node(), Reason}).

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
%%% Internal functions - parse_title
%%%===================================================================

%% @doc Get a line from the basic.tsv and return a title record with the info
-spec(parse_title(Title :: string()) -> #title{}).
parse_title(Title) ->
  #title{
    id = element(1, string:to_integer(string:sub_string(lists:nth(1, Title), 3))),
    title = lists:nth(4, Title),
    type = lists:nth(2, Title),
    genres = string:trim(lists:nth(9, Title)),
    cast = []
  }.

%%%===================================================================
%%% Internal functions - parse_principal
%%%===================================================================

%% @doc Get a line from the principals.tsv and return a tuple with some info
-spec(parse_principal(Principal :: string()) ->
  {ID :: integer(), NameID :: string(), Role :: string()}).
parse_principal(Principal) ->
  {
    element(1, string:to_integer(string:sub_string(lists:nth(1, Principal), 3))),
    lists:nth(3, Principal),
    lists:nth(4, Principal)
  }.

%%%===================================================================
%%% Internal functions - fetch_name
%%%===================================================================

%% @doc Send a request to master to get the associated name
-spec(fetch_name(NameID :: string()) -> Name :: string()).
fetch_name(NameID) ->
  gen_server:call({master, ?MASTER_NODE}, {name, NameID}).

%%%===================================================================
%%% Internal functions - process_request
%%%===================================================================

%% @doc Process a request sent from master
-spec(process_request(Request :: #request{}, From :: {pid(), Tag :: term()}, State :: #server_state{}) -> ok).
process_request(Request = #request{}, From, #server_state{actors_db = ActorsTable, titles_db = MoviesTable}) ->
  ID = erlang:unique_integer([positive]),
  io:format("Request ~p: Name = ~p\tType = ~p~n", [ID, Request#request.name, Request#request.type]),

  Start = os:timestamp(),
  case Request#request.type of
    cast -> Reply = process_request(Request, ActorsTable, MoviesTable);
    title -> Reply = process_request(Request, MoviesTable, ActorsTable);
    _other -> Reply = bad_arg
  end,
  io:format("Result for Request ~p: ~p~n", [ID, Reply]),
  io:format("Processing the request (~p) took ~p ms.~n", [ID, round(timer:now_diff(os:timestamp(), Start) / 1000)]),
  gen_server:reply(From, Reply);


process_request(Request = #request{}, First, Second) ->
  Name = Request#request.name,
  case ets:lookup(First, Name) of
    [] -> [];
    [{Name, L1}] ->
      lists:foldr(
        fun(E, Acc) ->
          case ets:lookup(Second, E) of
            [] -> [];
            [{E, L2}] -> L2 ++ Acc
          end
        end, [], L1)
  end.

%%%===================================================================
%%% Internal functions - merge_db
%%%===================================================================

%% @doc Merge the current table of the server with the one of a 'dead node'
-spec(merge_db(State :: #server_state{}, DeadNode :: atom()) ->
  ack |
  {error, Reason :: term()}).
merge_db(State = #server_state{}, DeadNode) ->
  ID = hd(string:split(atom_to_list(DeadNode), "@")),
  case ets:file2tab("table_movies_" ++ ID) of
    {error, Reason} -> {error, Reason};
    {ok, ETS} ->
      file:delete("table_movies_" ++ ID),
      merge_db(State#server_state.titles_db, ETS),
      case ets:file2tab("table_actors_" ++ ID) of
        {error, Reason} -> {error, Reason};
        {ok, ETS} ->
          file:delete("table_actors_" ++ ID),
          merge_db(State#server_state.actors_db, ETS)
      end
  end;

merge_db(TableRef, ToMerge) ->
  lists:foreach(fun(X) -> ets:insert_new(TableRef, X) end, ets:tab2list(ToMerge)),
  ets:delete(ToMerge),
  ack.

%%%===================================================================
%%% Internal functions - send_file_to_master
%%%===================================================================

%% @doc Start a daemon process for the master to download the file
-spec send_file_to_master(FileName :: string()) -> ok.
send_file_to_master(FileName) ->
  Start = os:timestamp(),

  {ok, Bin} = file:read_file(FileName),
  rpc:call(?MASTER_NODE, file, write_file, [FileName, Bin]),

  Reply = gen_server:call({master, ?MASTER_NODE}, {distribute_files, node(), FileName}),
  io:format("~n~p sent to master in ~p ms~nReply from master: ~p~n",
    [FileName, round(timer:now_diff(os:timestamp(), Start) / 1000), Reply]).

%%%===================================================================
%%% Internal functions - tab2file
%%%===================================================================

%% @doc Print a Table to a file (in strings and not table)
-spec tab2file(TableRef :: term(), Filename :: string()) -> ok | {error, Reason} when
  Reason :: badarg | terminated | system_limit.

tab2file(TableRef, FileName) ->
  try
    file:delete(FileName)
  catch
    error: _Error -> {os:system_time(), error, io_lib:format("File ~p doesn't exist", [FileName])}
  end,
  {ok, Fd} = file:open(FileName, [write, append]),
  lists:foreach(fun({X, Y}) ->
    file:write(Fd, io_lib:format("~s\t~s~n", [X, string:join(Y, ", ")])) end, ets:tab2list(TableRef)),
  file:close(Fd),
  io:format("New File available: ~p~n", [FileName]).

%%%===================================================================
%%% Internal functions - delete_all_backups
%%%===================================================================

%% @doc Delete all the files in the working directory that start with table
-spec delete_all_backups() -> ok.
delete_all_backups() ->
  {ok, Dir} = file:get_cwd(),
  {ok, Files} = file:list_dir(Dir),
  lists:foreach(
    fun(File) ->
      case hd(string:split(File, "_")) == "table" of
        true -> file:delete(File);
        false -> ok
      end
    end,
    Files).

%%%===================================================================
%%% Internal functions - change_key
%%%===================================================================

%% @doc Change the key for the titles_db -> ID to Title
-spec change_key(TableRef :: term()) -> ok.
change_key(TableRef) ->
  ets:match_delete(TableRef, {'_', #title{id = '_', title = '_', type = '_', genres = '_', cast = []}}),

  NewTable = ets:new(title_db, [ordered_set, public, {read_concurrency, true}]),
  lists:foreach(fun({_, #title{title = Title, cast = Cast}}) ->
    ets:insert_new(NewTable, {Title, Cast}) end, ets:tab2list(TableRef)),

  ets:delete(TableRef),
  NewTable.
