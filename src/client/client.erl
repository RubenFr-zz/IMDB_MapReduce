%%%-------------------------------------------------------------------
%%% @author Ruben
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 5. Aug 2021 11:59 PM
%%%-------------------------------------------------------------------
-module(client).
-author("Ruben").
%---------------------------------------------------------------------
-export([start/0]).
-include_lib("wx/include/wx.hrl").

-define(SERVER, ?MODULE).
% -define(MASTER_NODE, 'master@132.72.104.125').
-define(MASTER_NODE, 'master@ubuntu').

-record(request, {name, type, level}).

start() ->

  %%--------------------- Creating Components -------------------------------------------------------------------
  WX = wx:new(),
  Frame = wxFrame:new(WX, ?wxID_ANY, "IMBD map-reduce ",[{pos,{500,200}},{size,{400,470}}]),   %% build and layout the GUI components
  wxWindow:setBackgroundColour(Frame, {244,196,5}),

  Headline = wxStaticText:new(Frame, ?wxID_ANY, "Please enter a name of a actor or a movie", [{style, ?wxALIGN_CENTRE_HORIZONTAL}]),
  wxStaticText:wrap(Headline,5000),

  RadioBox = wxRadioBox:new(Frame, ?wxID_ANY, "Type:", {0,0}, {250,75}, ["Movie or Tv-Show or Short", "Actor or Actress"]),
  RadioBox2 = wxRadioBox:new(Frame, ?wxID_ANY, "Level:", {0,0}, {250,45}, [" 1        "," 2        "," 3        "," 4"], [{majorDim, ?wxHORIZONTAL}]),

  ButtonSearch = wxButton:new(Frame, ?wxID_ANY, [{label, "Search"}, {size,{200,30}}]),    %new button with text
  ButtonInfo = wxButton:new(Frame, ?wxID_ANY, [{label, "Information"}, {size,{200,30}}]),    %for more information

  Image = wxImage:scale(wxImage:new("IMBD.png", []), 300, 100, [{quality, ?wxIMAGE_QUALITY_HIGH}]),
  ResizedImage = wxImage:resize(Image, {300, 80}, {0, -6}),
  Bitmap = wxBitmap:new(ResizedImage),
  StaticBitmap = wxStaticBitmap:new(Frame, ?wxID_ANY, Bitmap),

  TextCtrl = wxTextCtrl:new(Frame, ?wxID_ANY, [{value, ""}, {style, ?wxTE_LEFT}]),   %text box value and align
  % wxTextCtrl:setToolTip(TextCtrl, "Enter your search value here"),
  Font = wxFont:new(14, ?wxFONTFAMILY_DEFAULT, ?wxFONTSTYLE_NORMAL, ?wxFONTWEIGHT_NORMAL),    %font size and design
  wxTextCtrl:setFont(TextCtrl, Font),

  %%--------------------- Inserting Components -------------------------------------------------------------------
  TextBoxSizer = wxBoxSizer:new(?wxVERTICAL),
  wxSizer:add(TextBoxSizer, Headline, [{flag, ?wxALIGN_CENTRE}, {border, 10}]),
  wxSizer:add(TextBoxSizer, TextCtrl, [{proportion,13},{flag, ?wxEXPAND bor ?wxALL}, {border, 15}]),

  MainSizer = wxBoxSizer:new(?wxVERTICAL),
  wxSizer:add(MainSizer, StaticBitmap, [{flag, ?wxALL bor ?wxEXPAND}, {border, 25}]),
  wxSizer:add(MainSizer, TextBoxSizer, [{flag, ?wxALIGN_CENTER},{border,100}]),
  wxSizer:add(MainSizer, RadioBox, [{flag, ?wxALL bor ?wxALIGN_CENTER},{border,20}]),
  wxSizer:add(MainSizer, RadioBox2, [{flag, ?wxBOTTOM bor ?wxALIGN_CENTER},{border,20}]),
  wxSizer:add(MainSizer, ButtonSearch, [{flag, ?wxALIGN_CENTER},{border,80}]),
  wxSizer:add(MainSizer, ButtonInfo, [{flag, ?wxALIGN_CENTER},{border,150}]),
  wxWindow:setSizer(Frame, MainSizer),

  wxButton:connect(ButtonSearch, command_button_clicked, [{callback, fun search_mouse_click/2},
    {userData, {wx:get_env(), TextCtrl, RadioBox, RadioBox2}}]),   % Connect the button to function
  wxEvtHandler:connect(ButtonInfo, command_button_clicked, [{callback, fun info_mouse_click/2},
    {userData, {wx:get_env(), ButtonInfo}}]),

  wxFrame:show(Frame).

%--------------------------------------------------------------------------------
%--------------------------------------------------------------------------------
search_mouse_click(A = #wx{}, _B) ->
  {Env, Text, Type, Level} = A#wx.userData,
  wx:set_env(Env),
  case is_valid(wxTextCtrl:getValue(Text)) of
    false ->
      wxTextCtrl:setForegroundColour(Text, ?wxRED),
      wxTextCtrl:setLabel(Text, "The input is invalid");
    true ->
      Request = #request{
        name = wxTextCtrl:getValue(Text),
        type = parse_type(wxRadioBox:getSelection(Type)),
        level = wxRadioBox:getSelection(Level) + 1
      },
      gen_server:call({master, ?MASTER_NODE}, {request, Request})
  end.

info_mouse_click(_A = #wx{}, _B) ->
  WX = wx:new(),
  Frame = wxFrame:new(WX, ?wxID_ANY, "IMBD",[{pos,{700,300}}, {size,{400,400}}]),
  wxWindow:setBackgroundColour(Frame, {244,196,5}),

  Text =
    "What is IMBb?

  Internet Movie Database (IMDb) is an online database of information related to films, television programs, home videos, video games, and streaming content online – including cast, production crew and personal biographies, plot summaries, trivia, ratings, and fan and critical reviews.
    
How to use?

  Simply enter the name of an actor or a movie or tv-show or short, select the according option and click on 'Search'.",

  Headline = wxStaticText:new(Frame, ?wxID_ANY, Text, [{style, ?wxALIGN_LEFT}]),
  MainSizer = wxBoxSizer:new(?wxVERTICAL),
  wxSizer:add(MainSizer, Headline, [{flag, ?wxALL bor ?wxALIGN_CENTRE}, {border, 10}]),

  wxFrame:show(Frame).

parse_type(String) ->
  case String of
    0 -> title;
    1 -> cast;
    Other -> Other
  end.

is_valid(String) -> length(String) > 0.

