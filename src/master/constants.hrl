%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, BGU
%%% @doc
%%%
%%% @end
%%% Created : 18. Aug 2021 12:07 PM
%%%-------------------------------------------------------------------
-author("Ruben").

%% Data Init
-define(BASIC_FILENAME, "InputFiles/basic1000.tsv").
-define(PRINCIPALS_FILENAME, "InputFiles/principals1000.tsv").
-define(NAMES_FILENAME, "InputFiles/names1000.tsv").

%% Servers
-define(SERVERS,
  [
    'server1@ubuntu',
    'server2@ubuntu',
    'server3@ubuntu',
    'server4@ubuntu'
  ]).
