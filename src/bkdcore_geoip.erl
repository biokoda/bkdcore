% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_geoip).
-behaviour(gen_server).
-export([start/0, stop/0, reload/0, register/0,print_info/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([update_db/1,lookup/1,is_country/1,country_langs/1]).
-compile(export_all).
-include_lib("kernel/include/file.hrl").
-include_lib("../include/bkdcore.hrl").
-compile({parse_transform,lager_transform}).

-record(ci,{country, languages = [], continent}).

% ISO 3166-1
-define(COUNTRIES, {<<"AF">>,<<"AL">>,<<"DZ">>,<<"AS">>,<<"AD">>,<<"AO">>,<<"AI">>,<<"AQ">>,<<"AG">>,<<"AR">>,
					<<"AM">>,<<"AW">>,<<"AU">>,<<"AT">>,<<"AZ">>,<<"BS">>,<<"BH">>,<<"BD">>,<<"BB">>,<<"BY">>,
					<<"BE">>,<<"BZ">>,<<"BJ">>,<<"BM">>,<<"BT">>,<<"BO">>,<<"BA">>,<<"BW">>,<<"BV">>,<<"BR">>,
					<<"IO">>,<<"VG">>,<<"BN">>,<<"BG">>,<<"BF">>,<<"03">>,<<"BI">>,<<"KH">>,<<"CM">>,<<"CA">>,
					<<"CV">>,<<"01">>,<<"KY">>,<<"CF">>,<<"TD">>,<<"CL">>,<<"CN">>,<<"CX">>,<<"CC">>,<<"CO">>,
					<<"KM">>,<<"CG">>,<<"CD">>,<<"CK">>,<<"CR">>,<<"CI">>,<<"HR">>,<<"CU">>,<<"CY">>,<<"CZ">>,
					<<"DK">>,<<"DJ">>,<<"DM">>,<<"DO">>,<<"TP">>,<<"EC">>,<<"EG">>,<<"SV">>,<<"GQ">>,<<"ER">>,
					<<"EE">>,<<"ET">>,<<"FK">>,<<"FO">>,<<"FJ">>,<<"FI">>,<<"FR">>,<<"FX">>,<<"GF">>,<<"PF">>,
					<<"TF">>,<<"GA">>,<<"GM">>,<<"GE">>,<<"DE">>,<<"GH">>,<<"GI">>,<<"GR">>,<<"GL">>,<<"GD">>,
					<<"GP">>,<<"GU">>,<<"GT">>,<<"GN">>,<<"GW">>,<<"GY">>,<<"HT">>,<<"HM">>,<<"HN">>,<<"00">>,
					<<"HK">>,<<"HU">>,<<"IS">>,<<"IN">>,<<"ID">>,<<"IR">>,<<"IQ">>,<<"IE">>,<<"IL">>,<<"IT">>,
					<<"JM">>,<<"JP">>,<<"JO">>,<<"KZ">>,<<"KE">>,<<"KI">>,<<"KR">>,<<"KP">>,<<"KW">>,<<"KG">>,
					<<"LA">>,<<"LV">>,<<"LB">>,<<"LS">>,<<"LR">>,<<"LY">>,<<"LI">>,<<"LT">>,<<"LU">>,<<"MO">>,
					<<"MK">>,<<"MG">>,<<"MW">>,<<"MY">>,<<"MV">>,<<"ML">>,<<"MT">>,<<"MH">>,<<"MQ">>,<<"MR">>,
					<<"MU">>,<<"YT">>,<<"MX">>,<<"FM">>,<<"MD">>,<<"MC">>,<<"MN">>,<<"MS">>,<<"MA">>,<<"MZ">>,
					<<"MM">>,<<"NA">>,<<"NR">>,<<"NP">>,<<"NL">>,<<"AN">>,<<"NC">>,<<"NZ">>,<<"NI">>,<<"NE">>,
					<<"NG">>,<<"NU">>,<<"NF">>,<<"MP">>,<<"NO">>,<<"OM">>,<<"PK">>,<<"PW">>,<<"PA">>,<<"PG">>,
					<<"PY">>,<<"PE">>,<<"PH">>,<<"PN">>,<<"PL">>,<<"PT">>,<<"PR">>,<<"QA">>,<<"RE">>,<<"RO">>,
					<<"RU">>,<<"RW">>,<<"KN">>,<<"LC">>,<<"VC">>,<<"WS">>,<<"SM">>,<<"ST">>,<<"SA">>,<<"SN">>,
					<<"SC">>,<<"SL">>,<<"SG">>,<<"SK">>,<<"SI">>,<<"SB">>,<<"SO">>,<<"ZA">>,<<"GS">>,<<"ES">>,
					<<"LK">>,<<"SH">>,<<"PM">>,<<"SD">>,<<"SR">>,<<"SJ">>,<<"SZ">>,<<"SE">>,<<"CH">>,<<"SY">>,
					<<"TW">>,<<"TJ">>,<<"TZ">>,<<"TH">>,<<"TG">>,<<"TK">>,<<"TO">>,<<"TT">>,<<"TN">>,<<"TR">>,
					<<"TM">>,<<"TC">>,<<"TV">>,<<"UG">>,<<"UA">>,<<"AE">>,<<"GB">>,<<"US">>,<<"UM">>,<<"UY">>,
					<<"UZ">>,<<"VU">>,<<"VA">>,<<"VE">>,<<"VN">>,<<"WF">>,<<"EH">>,<<"YE">>,<<"YU">>,<<"02">>,
					<<"ZM">>,<<"ZW">>,<<"VI">>,<<"CS">>,<<"ME">>,<<"PS">>,<<"RS">>,<<"TL">>,<<"JE">>}).

% ISO 639
-define(LANGUAGES,	[{<<"SL">>,[<<"sl">>]},{<<"GR">>,[<<"el">>]},{<<"KR">>,[<<"ko">>]},{<<"LT">>,[<<"lt">>]},{<<"TR">>,[<<"tr">>]},
					 {<<"CH">>,[<<"de">>,<<"fr">>,<<"it">>]},{<<"SK">>,[<<"sk">>]},{<<"EO">>,[<<"eo">>]},{<<"GB">>,[<<"en">>]},{<<"HE">>,[<<"he">>]},
					 {<<"HR">>,[<<"hr">>]},{<<"DE">>,[<<"de">>]},{<<"ET">>,[<<"am">>]},{<<"EL">>,[<<"el">>]},{<<"HK">>,[<<"zh">>]},
					 {<<"IE">>,[<<"en">>]},{<<"CS">>,[<<"cs">>]},{<<"BG">>,[<<"bg">>]},{<<"IS">>,[<<"is">>]},{<<"BY">>,[<<"be">>]},{<<"KZ">>,[<<"kk">>]},
					 {<<"JP">>,[<<"ja">>]},{<<"EU">>,[<<"eu">>]},{<<"NL">>,[<<"nl">>]},{<<"US">>,[<<"en">>]},{<<"IN">>,[<<"hi">>]},{<<"LN">>,[<<"la">>]},
					 {<<"SV">>,[<<"sv">>]},{<<"KO">>,[<<"ko">>]},{<<"PL">>,[<<"pl">>]},{<<"SI">>,[<<"sl">>]},{<<"CN">>,[<<"zh">>]},{<<"TW">>,[<<"zh">>]},
					 {<<"IT">>,[<<"it">>]},{<<"FR">>,[<<"fr">>]},{<<"HU">>,[<<"hu">>]},{<<"BE">>,[<<"fr">>,<<"nl">>]},{<<"BR">>,[<<"pt">>]},
					 {<<"SE">>,[<<"sv">>]},{<<"ES">>,[<<"ca">>,<<"es">>,<<"eu">>]},{<<"YU">>,[<<"sr">>]},{<<"ZA">>,[<<"af">>]},{<<"FI">>,[<<"fi">>]},
					 {<<"DK">>,[<<"da">>]},{<<"RU">>,[<<"ru">>]},{<<"NZ">>,[<<"en">>]},{<<"RO">>,[<<"ro">>]},{<<"EE">>,[<<"et">>]},{<<"JA">>,[<<"ja">>]},
					 {<<"AT">>,[<<"de">>]},{<<"DA">>,[<<"da">>]},{<<"NB">>,[<<"nb">>]},{<<"PT">>,[<<"pt">>]},{<<"AM">>,[<<"hy">>]},{<<"UA">>,[<<"uk">>]},
					 {<<"ZH">>,[<<"zh">>]},{<<"CZ">>,[<<"cs">>]},{<<"IL">>,[<<"he">>]},{<<"NO">>,[<<"no">>]},{<<"CA">>,[<<"ca">>,<<"en">>,<<"fr">>]},
					 {<<"AU">>,[<<"en">>]},{<<"AL">>, [<<"sq">>]}]).
					

is_country(C) ->
	gen_server:call(?MODULE,{is_country,ctry_format(C)}).
	
ctry_format([A,B]) ->
	ctry_format(<<A,B>>);
ctry_format(<<A,B>>) when A >= $a ->
	ctry_format(<<(A-32),(B-32)>>);
ctry_format(C) ->
	C.

update_db(Path) ->
	gen_server:cast(?MODULE,{update, Path}).

lookup(IP) ->
	case gen_server:call(?MODULE,{lookup,butil:ip_to_int(IP)}) of
		false ->
			false;
		Index ->
			element(Index, ?COUNTRIES)
	end.

country_langs(C) ->
	gen_server:call(?MODULE, {ctry_langs,ctry_format(C)}).
localized_lang(C) ->
	gen_server:call(?MODULE,{localized_lang,string:to_lower(butil:tolist(C))}).

ipinfo(IP) ->
	case gen_server:call(?MODULE,{ipinfo,butil:ip_to_int(IP)}) of
		[L] ->
			L;
		_ ->
			undefined
	end.


-record(gip, {tree,loclangs, countries, ipv4time = 0,ipv6time = 0}).

handle_call({ipinfo,IP}, _, P) ->
	case find_ip(P#gip.tree,IP) of
		false ->
			{reply, undefined, P};
		C ->
			{reply, ets:lookup(P#gip.countries,element(C,?COUNTRIES)), P}
	end;
handle_call({ctry_langs, Ctry}, _, P) ->
	case ets:lookup(P#gip.countries, Ctry) of
		[#ci{} = L] ->
			{reply, L#ci.languages,P};
		_ ->
			{reply, [], P}
	end;
handle_call({localized_lang,Code}, _, P) ->
	case dict:find(Code,P#gip.loclangs) of
		{ok,Val} ->
			{reply, Val, P};
		_ ->
			{reply, "", P}
	end;
handle_call({lookup,IP},_,P) ->
	{reply, find_ip(P#gip.tree,IP), P};
handle_call({is_country,<<"tv">>}, _, P) ->
	{reply, false, P};
handle_call({is_country,C}, _, P) ->
	{reply, ets:member(P#gip.countries,C),P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call(_, _, P) ->
	{reply, ok, P}.

handle_cast({save_tree, T}, P) ->
	lager:info("bkdweb_geoip saving geoip tree"),
	gen_server:cast(access,{save_tree,T}),
	{noreply, P#gip{tree = T}};
handle_cast({print_info}, P) ->
	io:format("treesize ~p~n~p~n", [tuple_size(P#gip.tree), ets:tab2list(P#gip.countries)]),
	{noreply, P};
handle_cast(_X, P) ->
	{noreply, P}.

handle_info({check_file}, P) ->
	handle_info({check_file,0}, P);
handle_info({check_file,N}, P) ->
	CSV = butil:project_rootpath() ++ "/priv/geoip/geoip.csv",
	Ipv6 = butil:project_rootpath() ++ "/priv/geoip/ipv6.csv",
	erlang:send_after(60000,self(),{check_file,N+1}),
	case true of
		true when N rem 60*24 == 0 ->
			spawn(fun() -> check_update_csv() end);
		_ ->
			ok
	end,
	IsFileNew = fun(Path,Time) ->
								{ok,ICsv} = file:read_file_info(Path),
								CStime = butil:sec(ICsv#file_info.mtime),
								{CStime > Time,CStime}
							end,
	% 
	case catch IsFileNew(CSV,P#gip.ipv4time) of
		{true,I4Time} ->
			ok;
		_ ->
			I4Time = P#gip.ipv4time
	end,
	case catch IsFileNew(Ipv6) of
		{true,I6Time} ->
			ok;
		_ ->
			I6Time = P#gip.ipv6time
	end,
	case P#gip.ipv4time /= I4Time orelse I6Time /= P#gip.ipv6time of
		true ->
			spawn(fun() -> update_geoipdb(CSV,Ipv6) end);
		false ->
			ok
	end,
	{noreply,P#gip{ipv4time = I4Time, ipv6time = I6Time}};
handle_info(_, State) -> 
	{noreply, State}.

terminate(_, _) ->
	ok.
code_change(_, State, _) ->
	{ok, State}.
init(_) ->
	% filelib:ensure_dir(butil:project_rootpath()++"/priv/geoip/"),
	C = ets:new(countries,[set,private,{keypos,#ci.country}]),
	% [ets:insert(C,{Ctry}) || Ctry <- tuple_to_list(?COUNTRIES)],
	% [ets:insert(C,{{langs, Ctry},L}) || {Ctry,L} <- ?LANGUAGES],
	ets:insert(C,consci(europe(), [])),
	ets:insert(C,consci(asia(), [])),
	ets:insert(C,consci(america(), [])),
	Loclangs = dict:from_list(loclangs()),
	case filelib:is_dir(butil:project_rootpath()++"/priv/geoip/") of
		true ->
			self() ! {check_file,0};
		_ ->
			ok
	end,
	{ok, #gip{countries = C,loclangs = Loclangs}}.

check_update_csv() ->
	case application:get_env(bkdcore,geoipv4) of
		undefined ->
			Url = "";
		{ok,Url} ->
			ok
	end,	
	case Url of
		"" ->
			ok;
		_ ->
			{ok, "200", Heads, _} = butil:http(Url,[],head),
			{_,Date} = lists:keyfind('Last-Modified',1,Heads),
			Lastmod = butil:sec(httpd_util:convert_request_date(butil:tolist(Date))),
			case file:read_file_info(butil:project_rootpath() ++ "/priv/geoip/geoip.csv") of
				{ok,I} ->
					lager:info("Update csv Diff ~p~n~p", [{I#file_info.mtime,butil:sec(I#file_info.mtime)}, {Date,Lastmod}]),
					case butil:sec(I#file_info.mtime) < Lastmod of
						true ->
							update_csv();
						false ->
							uptodate
					end;
				_ ->
					update_csv()
			end
	end.
update_csv() ->
	case application:get_env(bkdcore,geoipv4) of
		undefined ->
			Url = "";
		{ok,Url} ->
			ok
	end,		
	case length(Url) > 0 of
		true ->
			{ok, "200", _, Body} = butil:http(Url),
			{ok,L} = zip:extract(Body,[memory]),
			{_,Csv} = butil:findtrue(fun({Name,_B}) -> 
										string:str(Name,".csv") > 0
									end,L),
			filelib:ensure_dir(butil:project_rootpath()++"/priv/geoip/"),
			file:write_file(butil:project_rootpath() ++ "/priv/geoip/geoip.csv",Csv);
		_ ->
			ok
	end.

consci([HL|CL],L) ->
	H = butil:tobin(HL),
	put(c,undefined),
	case lists:member(HL,asia()) of
		true ->	put(c,asia);
		_ -> ok
	end,
	case lists:member(HL,europe()) of
		true ->	put(c,europe);
		_ -> ok
	end,	
	case lists:member(HL,america()) of
		true ->	put(c,america);
		_ -> ok
	end,
	case proplists:get_value(H,?LANGUAGES) of
		undefined -> Lang = [];
		Lang -> ok
	end,
	consci(CL,[#ci{country = H, continent = get(c),languages = Lang}|L]);
consci([], L) ->
	L.

find_ip({Min, Max, Country, _Left, _Right}, IP) when IP > Min, IP < Max ->
	Country;
find_ip({_Min, Max, _Country, _Left, Right}, IP) when IP >= Max ->
	find_ip(Right, IP);
find_ip({Min, _Max, _Country, Left, _Right}, IP) when IP =< Min ->
	find_ip(Left, IP);	
find_ip(undefined, _) ->
	false;
find_ip(_, _) ->
	false.


update_geoipdb(I4,I6) ->
	{ok, File} = file:read_file(I4),
	case file:read_file(I6) of
		{ok,I6b} ->
			Bin = <<File/binary,I6b/binary>>;
		_ ->
			Bin = File
	end,
	AllRanges = butil:sparsemap(fun(Line) ->
					case [Item || Item <- binary:split(Line,[<<"\"">>,<<",">>,<<" ">>],[global]),Item /= <<>>] of
						[_,_,From,To,Ctry|_] ->
							case catch {butil:toint(From),butil:toint(To)} of
								{Fromi,Toi} when is_integer(Fromi), is_integer(Toi) ->
									{Fromi,Toi,Ctry};
								_ ->
									undefined
							end;
						_ ->
							undefined
					end
			 end,
			 binary:split(Bin,<<"\n">>,[global])),
	Sorted = lists:keysort(1, AllRanges),
	{CountryTList, _} = lists:mapfoldl(fun(Country, I) -> {{Country, I+1}, I+1} end,0, tuple_to_list(?COUNTRIES)),
	Ets = ets:new(countryset, [set, private]),
	ets:insert(Ets, CountryTList),
	SortedNoname = lists:map(fun({Min, Max, Country}) -> 
								% io:format("~p~n", [Country]),
								case ets:lookup(Ets, Country) of
									[{_, Index}] ->
										{Min, Max, Index};
									_ ->
										{Min, Max, Country}
								end
		                     end, Sorted),
	ets:delete(Ets),
	% WithoutUnknowns = lists:filter(fun({_Min, _Max, Country}) -> is_integer(Country)  end, SortedNoname),
	Tree = cons_tree(SortedNoname),
	gen_server:cast(?MODULE, {save_tree, Tree}).

cons_tree([]) ->
	undefined;
cons_tree(L) ->
	{Left, Right} = lists:split(round(length(L)/2), L),
	[{Min, Max, Country}|LeftSide] = lists:reverse(Left),
	{Min, Max, Country, cons_tree(lists:reverse(LeftSide)), cons_tree(Right)}.

reload() ->
	code:purge(?MODULE),
	code:load_file(?MODULE).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

register() ->
	supervisor:start_child(bkdcore_sup, {?MODULE, {?MODULE, start, []}, permanent, 100, worker, [?MODULE]}).

print_info() ->
	gen_server:cast(?MODULE, {print_info}).
	

-define(EUROPE,
	["DZ","AO","BJ","BW","BF","BI","CM","CV","CF","TD","KM","CG","CD","CI","DJ",
	 "EG","GQ","ER","ET","GA","GM","GH","GN","GW","KE","LS","LR","LY","MG","MW",
	 "ML","MR","MU","YT","MA","MZ","NA","NE","NG","RE","RW","SH","ST","SN","SC",
	 "SL","SO","ZA","SD","SZ","TZ","TG","TN","UG","EH","ZM","ZW","AX","AL","AD",
	 "AT","BY","BE","BA","BG","HR","CZ","DK","EE","EU","FO","FI","FR","DE","GI",
	 "GR","GG","VA","HU","IS","IE","IM","IT","JE","CS","LV","LI","LT","LU","MK",
	 "MT","MD","MC","ME","NL","NO","PL","PT","RO","RU","SM","RS","SK","SI","ES",
	 "SJ","SE","CH","UA","GB"]).
-define(AMERICA,
	["AI","AG","AW","BS","BB","BZ","BM","CA","KY","CR","CU","DM","DO","SV","GL",
	 "GD","GP","GT","HT","HN","JM","MQ","MX","MS","AN","NI","PA","PR","KN","LC",
	 "PM","VC","TT","TC","US","UM","VG","VI","AR","BO","BR","CL","CO","EC","FK",
	 "GF","GY","PY","PE","SR","UY","VE"]).
-define(ASIA,
	["AF","AM","AZ","BH","BD","BT","IO","BN","KH","CN","CX","CC","CY","GE","HK",
	 "IN","ID","IR","IQ","IL","JP","JO","KZ","KP","KR","KW","KG","LA","LB","MO",
	 "MY","MV","MN","MM","NP","OM","PK","PS","PH","QA","SA","SG","LK","SY","TW",
	 "TJ","TH","TL","TR","TM","AE","UZ","VN","YE","AS","AP","AU","CK","FJ","PF",
	 "GU","KI","MH","FM","NR","NC","NZ","NU","NF","MP","PW","PG","PN","WS","SB",
	 "TK","TO","TV","VU","WF"]).
europe() ->
	?EUROPE.
asia() ->
	?ASIA.
america() ->
	?AMERICA.


-define(LOC_LANGS,[	{[122,117],<<105,115,105,90,117,108,117>>},
	{[122,104],<<228,184,173,230,150,135>>},
	{[121,111],<<195,136,100,195,168,32,89,111,114,195,185,98,195,161>>},
	{[120,111,103],<<79,108,117,115,111,103,97>>},
	{[120,104],<<105,115,105,88,104,111,115,97>>},
	{[119,97,108],<<225,139,136,225,136,139,225,139,173,225,137,179,225,137,177>>},
	{[118,117,110],<<75,121,105,118,117,110,106,111>>},
	{[118,105],<<84,105,225,186,191,110,103,32,86,105,225,187,135,116>>},
	{[117,122],<<208,142,208,183,208,177,208,181,208,186>>},
	{[117,114],<<216,167,216,177,216,175,217,136>>},
	{[117,107],<<209,131,208,186,209,128,208,176,209,151,208,189,209,129,209,140,208,186,208,176>>},
	{[116,122,109],<<84,97,109,97,122,105,201,163,116>>},
	{[116,115],<<88,105,116,115,111,110,103,97>>},
	{[116,114],<<84,195,188,114,107,195,167,101>>},
	{[116,111],<<108,101,97,32,102,97,107,97,116,111,110,103,97>>},
	{[116,110],<<83,101,116,115,119,97,110,97>>},
	{[116,105,103],<<225,137,181,225,140,141,225,136,168>>},
	{[116,105],<<225,137,181,225,140,141,225,136,173,225,138,155>>},
	{[116,104],<<224,185,132,224,184,151,224,184,162>>},
	{[116,101,111],<<75,105,116,101,115,111>>},
	{[116,101],<<224,176,164,224,177,134,224,176,178,224,177,129,224,176,151,224,177,129>>},
	{[116,97],<<224,174,164,224,174,174,224,174,191,224,174,180,224,175,141>>},
	{[115,119],<<75,105,115,119,97,104,105,108,105>>},
	{[115,118],<<115,118,101,110,115,107,97>>},
	{[115,116],<<83,101,115,111,116,104,111>>},
	{[115,114],<<208,161,209,128,208,191,209,129,208,186,208,184>>},
	{[115,113],<<115,104,113,105,112,101>>},
	{[115,111],<<83,111,111,109,97,97,108,105>>},
	{[115,110],<<99,104,105,83,104,111,110,97>>},
	{[115,108],<<115,108,111,118,101,110,197,161,196,141,105,110,97>>},
	{[115,107],<<115,108,111,118,101,110,196,141,105,110,97>>},
	{[115,104,105],<<116,97,109,97,122,105,103,104,116>>},
	{[115,103],<<83,195,164,110,103,195,182>>},
	{[115,101,115],<<75,111,121,114,97,98,111,114,111,32,115,101,110,110,105>>},
	{[115,101,104],<<115,101,110,97>>},
	{[115,101],<<100,97,118,118,105,115,195,161,109,101,103,105,101,108,108,97>>},
	{[115,97,113],<<75,105,115,97,109,112,117,114>>},
	{[115,97],<<224,164,184,224,164,130,224,164,184,224,165,141,224,164,149,224,165,131,224,164,164,32,224,164,173,224,164,190,224,164,183,224,164,190>>},
	{[114,119,107],<<75,105,114,117,119,97>>},
	{[114,119],<<75,105,110,121,97,114,119,97,110,100,97>>},
	{[114,117],<<209,128,209,131,209,129,209,129,208,186,208,184,208,185>>},
	{[114,111,102],<<75,105,104,111,114,111,109,98,111>>},
	{[114,111],<<114,111,109,195,162,110,196,131>>},
	{[114,109],<<114,117,109,97,110,116,115,99,104>>},
	{[112,116],<<112,111,114,116,117,103,117,195,170,115>>},
	{[112,115],<<217,190,218,154,216,170,217,136>>},
	{[112,108],<<112,111,108,115,107,105>>},
	{[112,97],<<224,168,170,224,169,176,224,168,156,224,168,190,224,168,172,224,169,128>>},
	{[111,114],<<224,172,147,224,172,161,224,172,188,224,172,191,224,172,134>>},
	{[111,109],<<79,114,111,109,111,111>>},
	{[111,99],<<111,99,99,105,116,97,110>>},
	{[110,121,110],<<82,117,110,121,97,110,107,111,114,101>>},
	{[110,110],<<110,121,110,111,114,115,107>>},
	{[110,108],<<78,101,100,101,114,108,97,110,100,115>>},
	{[110,101],<<224,164,168,224,165,135,224,164,170,224,164,190,224,164,178,224,165,128>>},
	{[110,100,115],<<80,108,97,116,116,100,195,188,195,188,116,115,99,104>>},
	{[110,100],<<105,115,105,78,100,101,98,101,108,101>>},
	{[110,98],<<110,111,114,115,107,32,98,111,107,109,195,165,108>>},
	{[110,97,113],<<75,104,111,101,107,104,111,101,103,111,119,97,98>>},
	{[109,121],<<225,128,151,225,128,153,225,128,172>>},
	{[109,116],<<77,97,108,116,105>>},
	{[109,115],<<66,97,104,97,115,97,32,77,101,108,97,121,117>>},
	{[109,114],<<224,164,174,224,164,176,224,164,190,224,164,160,224,165,128>>},
	{[109,110],<<208,188,208,190,208,189,208,179,208,190,208,187>>},
	{[109,108],<<224,180,174,224,180,178,224,180,175,224,180,190,224,180,179,224,180,130>>},
	{[109,107],<<208,188,208,176,208,186,208,181,208,180,208,190,208,189,209,129,208,186,208,184>>},
	{[109,103],<<77,97,108,97,103,97,115,121>>},
	{[109,102,101],<<107,114,101,111,108,32,109,111,114,105,115,105,101,110>>},
	{[109,101,114],<<75,196,169,109,196,169,114,197,169>>},
	{[109,97,115],<<77,97,97>>},
	{[108,118],<<108,97,116,118,105,101,197,161,117>>},
	{[108,117,121],<<76,117,108,117,104,105,97>>},
	{[108,117,111],<<68,104,111,108,117,111>>},
	{[108,116],<<108,105,101,116,117,118,105,197,179>>},
	{[108,111],<<224,186,165,224,186,178,224,186,167>>},
	{[108,110],<<108,105,110,103,195,161,108,97>>},
	{[108,103],<<76,117,103,97,110,100,97>>},
	{[108,97,103],<<75,201,168,108,97,97,110,103,105>>},
	{[107,121],<<208,154,209,139,209,128,208,179,209,139,208,183>>},
	{[107,119],<<107,101,114,110,101,119,101,107>>},
	{[107,117],<<217,131,217,136,216,177,216,175,219,140>>},
	{[107,115,98],<<75,105,115,104,97,109,98,97,97>>},
	{[107,111,107],<<224,164,149,224,165,139,224,164,130,224,164,149,224,164,163,224,165,128>>},
	{[107,111],<<237,149,156,234,181,173,236,150,180>>},
	{[107,110],<<224,178,149,224,178,168,224,179,141,224,178,168,224,178,161>>},
	{[107,109],<<225,158,151,225,158,182,225,158,159,225,158,182,225,158,129,225,159,146,225,158,152,225,159,130,225,158,154>>},
	{[107,108,110],<<75,97,108,101,110,106,105,110>>},
	{[107,108],<<107,97,108,97,97,108,108,105,115,117,116>>},
	{[107,107],<<210,154,208,176,208,183,208,176,210,155>>},
	{[107,105],<<71,105,107,117,121,117>>},
	{[107,104,113],<<75,111,121,114,97,32,99,105,105,110,105>>},
	{[107,101,97],<<107,97,98,117,118,101,114,100,105,97,110,117>>},
	{[107,100,101],<<67,104,105,109,97,107,111,110,100,101>>},
	{[107,97,109],<<75,105,107,97,109,98,97>>},
	{[107,97,98],<<84,97,113,98,97,121,108,105,116>>},
	{[107,97],<<225,131,165,225,131,144,225,131,160,225,131,151,225,131,163,225,131,154,225,131,152>>},
	{[106,109,99],<<75,105,109,97,99,104,97,109,101>>},
	{[106,97],<<230,151,165,230,156,172,232,170,158>>},
	{[105,116],<<105,116,97,108,105,97,110,111>>},
	{[105,115],<<195,173,115,108,101,110,115,107,97>>},
	{[105,105],<<234,134,136,234,140,160,234,137,153>>},
	{[105,103],<<73,103,98,111>>},
	{[105,100],<<66,97,104,97,115,97,32,73,110,100,111,110,101,115,105,97>>},
	{[105,97],<<105,110,116,101,114,108,105,110,103,117,97>>},
	{[104,121],<<213,128,213,161,213,181,213,165,214,128,213,167,213,182>>},
	{[104,117],<<109,97,103,121,97,114>>},
	{[104,114],<<104,114,118,97,116,115,107,105>>},
	{[104,105],<<224,164,185,224,164,191,224,164,168,224,165,141,224,164,166,224,165,128>>},
	{[104,101],<<215,162,215,145,215,168,215,153,215,170>>},
	{[104,97,119],<<202,187,197,141,108,101,108,111,32,72,97,119,97,105,202,187,105>>},
	{[104,97],<<72,97,117,115,97>>},
	{[103,117,122],<<69,107,101,103,117,115,105,105>>},
	{[103,117],<<224,170,151,224,171,129,224,170,156,224,170,176,224,170,190,224,170,164,224,171,128>>},
	{[103,115,119],<<83,99,104,119,105,105,122,101,114,116,195,188,195,188,116,115,99,104>>},
	{[103,108],<<103,97,108,101,103,111>>},
	{[103,101,122],<<225,140,141,225,139,149,225,139,157,225,138,155>>},
	{[103,97],<<71,97,101,105,108,103,101>>},
	{[102,117,114],<<102,117,114,108,97,110>>},
	{[102,114],<<102,114,97,110,195,167,97,105,115>>},
	{[102,111],<<102,195,184,114,111,121,115,107,116>>},
	{[102,105,108],<<70,105,108,105,112,105,110,111>>},
	{[102,105],<<115,117,111,109,105>>},
	{[102,102],<<80,117,108,97,97,114>>},
	{[102,97],<<217,129,216,167,216,177,216,179,219,140>>},
	{[101,117],<<101,117,115,107,97,114,97>>},
	{[101,116],<<101,101,115,116,105>>},
	{[101,115],<<101,115,112,97,195,177,111,108>>},
	{[101,111],<<101,115,112,101,114,97,110,116,111>>},
	{[101,110],<<69,110,103,108,105,115,104>>},
	{[101,108],<<206,149,206,187,206,187,206,183,206,189,206,185,206,186,206,172>>},
	{[101,101],<<69,202,139,101,103,98,101>>},
	{[101,98,117],<<75,196,169,101,109,98,117>>},
	{[100,122],<<224,189,162,224,190,171,224,189,188,224,189,132,224,188,139,224,189,129>>},
	{[100,101],<<68,101,117,116,115,99,104>>},
	{[100,97,118],<<75,105,116,97,105,116,97>>},
	{[100,97],<<100,97,110,115,107>>},
	{[99,121],<<67,121,109,114,97,101,103>>},
	{[99,115],<<196,141,101,197,161,116,105,110,97>>},
	{[99,104,114],<<225,143,163,225,142,179,225,142,169>>},
	{[99,103,103],<<82,117,107,105,103,97>>},
	{[99,97],<<99,97,116,97,108,195,160>>},
	{[98,121,110],<<225,137,165,225,136,138,225,138,149>>},
	{[98,115],<<98,111,115,97,110,115,107,105>>},
	{[98,111],<<224,189,148,224,189,188,224,189,145,224,188,139,224,189,166,224,190,144,224,189,145,224,188,139>>},
	{[98,110],<<224,166,172,224,166,190,224,166,130,224,166,178,224,166,190>>},
	{[98,109],<<98,97,109,97,110,97,107,97,110>>},
	{[98,103],<<208,177,209,138,208,187,208,179,208,176,209,128,209,129,208,186,208,184>>},
	{[98,101,122],<<72,105,98,101,110,97>>},
	{[98,101,109],<<73,99,104,105,98,101,109,98,97>>},
	{[98,101],<<208,177,208,181,208,187,208,176,209,128,209,131,209,129,208,186,208,176,209,143>>},
	{[97,122],<<97,122,201,153,114,98,97,121,99,97,110,99,97>>},
	{[97,115,97],<<75,105,112,97,114,101>>},
	{[97,115],<<224,166,133,224,166,184,224,166,174,224,167,128,224,167,159,224,166,190>>},
	{[97,114],<<216,167,217,132,216,185,216,177,216,168,217,138,216,169>>},
	{[97,109],<<225,138,160,225,136,155,225,136,173,225,138,155>>},
	{[97,107],<<65,107,97,110>>},
	{[97,102],<<65,102,114,105,107,97,97,110,115>>},
	{[97,97],<<81,97,102,97,114>>}]).

loclangs() ->
	?LOC_LANGS.
% localized_lang(X) ->
% 	element(2,lists:keyfind(X,1,?LOC_LANGS)).

% Creates a list of xml files with localized names of languages (sl.xml, en.xml, ..)
% langnames() ->
% 	Path = butil:project_rootpath() ++ "/priv/doc/common/main/",
% 	{ok,L} = file:list_dir(Path),
% 	F = fun(Langf) ->
% 			try Short = hd(string:tokens(Langf,".")),
% 			[_,_] = Short,
% 			{ok,Bin} = file:read_file(Path ++ Langf),
% 			{ldml,_,Langl} = butil:parsexml(Bin,[{xmlbase,Path}]),
% 			[LocDisp] = butil:xmlvals(Langl,[localeDisplayNames]),
% 			[Langsrw] = butil:xmlvals(LocDisp,[languages]),
% 			case is_tuple(Langsrw) of
% 				true ->
% 					Langs = Langsrw;
% 				false ->
% 					Langs = {languages,filterlang(Langsrw,[])}
% 			end,
% 			file:write_file("/Users/sergej/tmp/lang/" ++ Short ++ ".xml",butil:toxmlbin(Langs))
% 			of
% 			X ->
% 				io:format("~w~n", [X])
% 			catch
% 				% exit:_ ->
% 				% 	[];
% 				error:_ ->
% 					[]
% 			end
% 		end,
% 	[F(Lang)|| Lang <- lists:filter(fun(X) -> lists:member($_,X) == false end,L)].
% 
% filterlang([{_,L,[Name]} = H|T],Lang) ->
% 	case proplists:get_value(type,L) of
% 		[_,_] ->
% 			filterlang(T,[H|Lang]);
% 		_ ->
% 			filterlang(T,Lang)
% 	end;
% filterlang([],L) ->
% 	L.

% Create LOC_LANGS
% langs() ->
% 	Path = butil:project_rootpath() ++ "/priv/doc/common/main/",
% 	{ok,L} = file:list_dir(Path),
% 	F = fun(Langf) ->
% 			try Short = hd(string:tokens(Langf,".")),
% 			{ok,Bin} = file:read_file(Path ++ Langf),
% 			{ldml,_,Langl} = butil:parsexml(Bin,[{xmlbase,Path}]),
% 			[LocDisp] = butil:xmlvals(Langl,[localeDisplayNames]),
% 			[Langs] = butil:xmlvals(LocDisp,[languages]),
% 			{Short,findlang(Langs,Short)}
% 			of
% 			X ->
% 				io:format("~w~n", [X])
% 			catch
% 				% exit:_ ->
% 				% 	[];
% 				error:_ ->
% 					[]
% 			end
% 		end,
% 	[F(Lang)|| Lang <- lists:filter(fun(X) -> lists:member($_,X) == false end,L)].
% 	% [Lang <- Langl]
% 	% .
% findlang([{_,L,[Name]}|T],Lang) ->
% 	case proplists:get_value(type,L) of
% 		Lang ->
% 			unicode:characters_to_binary(Name);
% 		_ ->
% 			findlang(T,Lang)
% 	end;
% findlang([],_) ->
% 	false.

% print_continents() ->
% 	Africa = continent_countries("Africa"),
% 	Europe = continent_countries("Europe"),
% 	Asia = continent_countries("Asia"),
% 	Oceania = continent_countries("Oceania"),
% 	NAmerica = continent_countries("North_America"),
% 	SAmerica = continent_countries("South_America"),
% 	A1 = Africa ++ Europe,
% 	A2 = NAmerica ++ SAmerica,
% 	A3 = Asia ++ Oceania,
% 	io:format("A1~n~p~n", [A1]),
% 	io:format("A2~n~p~n", [A2]),
% 	io:format("A3~n~p~n", [A3]).
% 		
% continent_countries(Continent) ->
% 	{ok, "200", _, Bin} = ibrowse:send_req("http://www.countryipblocks.net/e_country_data/" ++ Continent ++ "_netmask.txt",[],get),
% 	Lines = string:tokens(Bin,"\n"),
% 	F = fun("# ISO Code: " ++ _) ->
% 				% io:format("~p~n", [Code]),
% 				true;
% 			(_) ->
% 				false
% 		end,
% 	Len = length("# ISO Code: ")+1,
% 	[string:sub_string(Line,Len) || Line <- lists:filter(F,Lines)].

% use locale to create a list of {COUNTRY,[language list]}
% langs() ->
% 	{ok, L} = file:list_dir("/usr/share/locale"),
% 	erase(),
% 	loopl(L).
% loopl([H|T]) ->
% 	[Useful|_] = string:tokens(H,"."),
% 	case string:tokens(Useful,"_") of
% 		[Lang,Ctry] ->
% 			setl(Lang,Ctry);
% 		[X] when length(X) == 2 ->
% 			setl(X,X);
% 		_ ->
% 			true
% 	end,
% 	loopl(T);
% loopl([]) ->
% 	[io:format("_~p_",[K]) || K <- get()].
% 
% setl(LA,CT) ->
% 	Lang = list_to_binary(string:to_lower(LA)),
% 	Ctry = list_to_binary(string:to_upper(CT)),
% 	case get(Ctry) of
% 		undefined ->
% 			put(Ctry,[Lang]);
% 		L ->
% 			case lists:member(Lang,L) of
% 				true ->
% 					true;
% 				false ->
% 					put(Ctry,[Lang|L])
% 			end
% 	end.