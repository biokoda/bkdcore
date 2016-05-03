% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_geoip).
-behaviour(gen_server).
-export([start/0, stop/0, reload/0, register/0,print_info/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([lookup/1, update_db/1]).
-include_lib("kernel/include/file.hrl").
-include_lib("../include/bkdcore.hrl").
-compile({parse_transform,lager_transform}).
-compile(export_all).

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

update_db(Path) ->
	gen_server:cast(?MODULE,{update, Path}).

lookup(IP) when is_integer(IP) ->
	case gen_server:call(?MODULE,{lookup,IP}) of
		false ->
			false;
		Index ->
			element(Index, ?COUNTRIES)
	end;
lookup(IP) when is_list(IP); is_binary(IP) ->
	case lookup(butil:ip_to_int(IP)) of
		false ->
			case string:tokens(butil:tolist(IP),":") of
				["2002",[A1,A2,B1,B2],[C1,C2,D1,D2]|_] ->
					<<A>> = butil:hex2dec([A1,A2]),
					<<B>> = butil:hex2dec([B1,B2]),
					<<C>> = butil:hex2dec([C1,C2]),
					<<D>> = butil:hex2dec([D1,D2]),
					<<N:32/integer-unsigned>> = <<A,B,C,D>>,
					lookup(N);
				_ ->
					false
			end;
		R ->
			R
	end.

-record(gip, {tree, countries, ipv4time = 0,ipv6time = 0}).


handle_call({lookup,IP},_,P) ->
	{reply, find_ip(P#gip.tree,IP), P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call(_, _, P) ->
	{reply, ok, P}.

handle_cast({save_tree, T}, P) ->
	lager:info("bkdweb_geoip saving geoip tree"),
	gen_server:cast(access,{save_tree,T}),
	butil:savetermfile(butil:project_rootpath()++"/priv/geoip/tree.bin", T),
	{noreply, P#gip{tree = T}};
handle_cast({print_info}, P) ->
	io:format("treesize ~p~n", [tuple_size(P#gip.tree)]),
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
	case filelib:is_dir(butil:project_rootpath()++"/priv/geoip/") of
		true ->
			self() ! {check_file,0};
		_ ->
			ok
	end,
	case butil:readtermfile(butil:project_rootpath()++"/priv/geoip/tree.bin") of
		undefined ->
			Tree = undefined;
		Tree ->
			ok
	end,
	{ok, #gip{tree = Tree}}.

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
							update_csv(Url);
						false ->
							uptodate
					end;
				_ ->
					update_csv(Url)
			end
	end.
update_csv(Url) ->
	filelib:ensure_dir(butil:project_rootpath()++"/priv/geoip/"),
	case butil:http("http://download.geonames.org/export/dump/countryInfo.txt") of
		{ok, "200", _, BodyCI} ->
			file:write_file(butil:project_rootpath() ++ "/priv/geoip/countryInfo.txt",BodyCI);
		_ ->
			ok
	end,
	{ok, "200", _, Body} = butil:http(Url),
	{ok,L} = zip:extract(Body,[memory]),
	[begin
		case filename:split(Name) of
			[_,"GeoIP2-Country-Blocks-IPv4.csv"] ->
				Fn = "geoip.csv";
			[_,"GeoIP2-Country-Blocks-IPv6.csv"] ->
				Fn = "ipv6.csv";
			_ ->
				Fn = undefined
		end,
		case Fn of
			undefined ->
				ok;
			_ ->
				file:write_file(butil:project_rootpath() ++ "/priv/geoip/"++Fn,Bin)
		end
	end || {Name,Bin} <- L].


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
	case Bin of
		<<"network,geoname_id",_/binary>> ->
			AllRanges = split_geoipdb2(Bin);
		_ ->
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
			binary:split(Bin,<<"\n">>,[global]))
	end,
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

split_geoipdb2(GeoBin) ->
	Ets = country_ets(),
	Split = binary:split(GeoBin,<<"\n">>,[global]),
	R = butil:sparsemap(fun(Line) ->
		case [Item || Item <- binary:split(Line,[<<",">>,<<" ">>],[global]), Item /= <<>> andalso binary:first(Line) =< $9] of
			[Range,Ctry|_] ->
				{From,To} = cidr_expand(Range),
				{From,To,butil:ds_val(Ctry,Ets)};
			_ ->
				undefined
		end
	end,
	tl(Split)),
	ets:delete(Ets),
	R.

cidr_expand(Range) ->
	[IP,Mask1] = binary:split(butil:tobin(Range),<<"/">>),
	From = butil:ip_to_int(IP),
	case ok of
		_ when From =< 16#ffffffff ->
			MaskLimit = 32;
		_ ->
			MaskLimit = 128
	end,
	Mask = butil:toint(Mask1),
	To = From + (1 bsl (MaskLimit-Mask)) -1,
	{From,To}.

country_ets() ->
	{ok,CIBin} = file:read_file(butil:project_rootpath() ++ "/priv/geoip/countryInfo.txt"),

	Countries = [Bin || <<F,_/binary>> = Bin <- binary:split(CIBin,<<"\n">>,[global]), F /= $#],
	Ets = ets:new(countries,[set,private]),
	[begin
		case binary:split(Line,<<"\t">>,[global]) of
			[Code, _Code3, _IsoCode, _Fips, _Country, _Capital,_Area, _Population, _Continent, _tld, _CurrencyCode, _CurrencyName, _Phone, _PostalCodeFormat, _PostalCodeRegex, _Languages,Geonameid|_] ->
				butil:ds_add(Geonameid,Code,Ets);
			_ ->
				ok
		end
	end || Line <- Countries],
	Ets.

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

