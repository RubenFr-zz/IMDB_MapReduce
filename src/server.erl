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

-record(server_state, {imdb, namePid}).
-record(title, {id, title, type, genres, cast}).

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
  {ok, #server_state{imdb = ets:new(imdb, [set, public, {read_concurrency, true}])}}.

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

handle_call(test, {From, _Tag}, State = #server_state{}) ->
  From ! received,
  {reply, ok, State};

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
  io:format("Received step1.~n"),
  Data = string:split(Line, "\t", all),
  Title = parseTitle(Data),
  ets:insert_new(State#server_state.imdb, {Title#title.id, Title}),
  {noreply, State};

handle_cast({namePID, NamePID}, State = #server_state{}) ->
  io:format("Received namePID.~n"),
  {noreply, State#server_state{namePid = NamePID}};

handle_cast({step2, Line}, State = #server_state{}) ->
  io:format("Received step2.~n"),
  Data = string:split(Line, "\t", all),
  {ID, NameID} = parsePrincipal(Data),
  Name = fetch_name(NameID),
  [Title = #title{}] = ets:lookup(State#server_state.imdb, ID),
  ets:update_element(State#server_state.imdb, ID, {2, [Name | Title#title.cast]}),
  {noreply, State};

handle_cast(test, State = #server_state{}) ->
  io:format("Message received!~n"),
  {noreply, State};

handle_cast(_Request, State = #server_state{}) ->
  {noreply, State}.

%%%===================================================================
%%% gen_server callbacks - init
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
%%% gen_server callbacks - init
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
%%% gen_server callbacks - init
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
    id = list_to_atom(lists:nth(1, Title)),
    title = lists:nth(4, Title),
    type = lists:nth(2, Title),
    genres = lists:nth(9, Title),
    cast = []
  }.

parsePrincipal(Principal) ->
  { list_to_atom(lists:nth(1, Principal)), list_to_atom(lists:nth(3, Principal)) }.

fetch_name(NameID) ->
  gen_server:call({master, 'master@ubuntu'}, {name, NameID}),
  receive
    not_available -> timer:sleep(100), fetch_name(NameID);
    Name -> Name
  end.