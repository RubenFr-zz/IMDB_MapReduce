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

-define(SERVER, master).

-record(master_state, {servers, namePID}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  {ok, PID} = gen_server:start_link({global, ?SERVER}, ?MODULE, [], []),
  register(master, PID),
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
  Start = os:timestamp(),
  Servers = dataInit:start_distribution(),
  io:format("Distributed data to ~p servers in ~p ms.~n",
    [length(Servers), round(timer:now_diff(os:timestamp(), Start) / 1000)]),
  {ok, #master_state{servers = Servers}}.

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

handle_call({name, NameID}, {From, _Tag}, State = #master_state{}) ->
  case State#master_state.namePID of
    undefined -> From ! not_available;
    PID -> spawn(fun() -> fetch_name(NameID, PID, From) end)
  end,
    {reply, ok, State};
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

handle_cast(test, State = #master_state{}) ->
  io:format("Test casting received!~n"),
  dataInit:test_step(),
  {noreply, State};

handle_cast(_Request, State = #master_state{}) ->
  {noreply, State}.

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

fetch_name(NameID, DB, From) ->
  DB ! {self(), NameID},
  receive
    Name -> From ! Name
  end.