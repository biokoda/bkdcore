% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_mochi).
-behaviour(gen_server).
-export([register/0, register/1, print_info/0, start/0, stop/0, init/1, handle_call/3, 
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([http_req/1,http_statreq/1]).

-compile([{parse_transform, lager_transform}]).

-record(bm,{}).

handle_call({print_info}, _, P) ->
	io:format("~p~n", [P]),
	{reply, ok, P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast({start_mochiweb,L},P) ->
	F = fun(Name,Val) ->
		case Val of
			undefined ->
				[];
			_ ->
				[{Name,Val}]
		end
	end,
	[begin
		[Port,Ssl,CaCert,Cert,Key] = butil:ds_vals([port,ssl,cacert,cert,key],Info,[8080,false,undefined,undefined,undefined]),
		% case Ssl of
		% 	true ->
				mochiweb_http:start([{port, Port}, {name,list_to_atom("mochi"++butil:tolist(Port))},{ssl,Ssl}, {loop,{?MODULE,http_req}}]++
					F(cacertfile,CaCert)++F(certfile,Cert)++F(keyfile,Key))
		% 	false ->
		% 		mochiweb_http:start([{port, Port}, {name,list_to_atom("mochi"++butil:tolist(Port))}, {loop,{?MODULE,ReqHandler}}])
		% end
	end || Info <- L],
	{noreply,P};
handle_cast({start_mochiweb},P) ->
	{ok,Port1} = application:get_env(bkdcore,webport),
	case application:get_env(bkdcore,stathandler) of
		{ok,true} ->
			ReqHandler = http_statreq;
		_ ->
			ReqHandler = http_req
	end,
	Port = butil:toint(Port1),
	lager:info("mochiweb startup on port ~p~n",[Port]),
	mochiweb_http:start([{port, Port}, {name,bkdweb_mochiweb}, {loop,{?MODULE,ReqHandler}}]),

	% Detect if any crt files in etc
	case application:get_env(bkdcore,etc) of
		undefined ->
			Etc = butil:project_rootpath() ++ "/etc";
		{ok,[$\~|Etcrem]} ->
			Etc = butil:project_rootpath()++Etcrem;
		{ok,Etc} ->
			ok
	end,
	% io:format("Checking for https ~p ~p~n", [Etc++"/*.key",filelib:wildcard(Etc++"/*.key")]),
	case Etc of
		none ->
			ok;
		_ ->
			[begin
				Rootname = filename:rootname(Keyfile), % without .key
				case filelib:is_file(Rootname++".crt") of
					true ->
						Certfile = Rootname++".crt",
						case filelib:is_file(Rootname++".pem") of
							true ->
								Cacert = [{cacertfile,Rootname++".pem"}];
							false ->
								Cacert = []
						end,
						mochiweb_http:start([{port, 6443}, {name,butil:toatom("ssl"++filename:basename(Rootname))},{ssl,true},
																 {ssl_opts,[{certfile,Certfile},{keyfile,Keyfile}|Cacert]},{loop,{?MODULE,http_req}}]);
					false ->
						ok
				end
			 end
			 || Keyfile <- filelib:wildcard(Etc++"/*.key")]
	end,
	{noreply, P};
handle_cast({stop_mochiweb},P) ->
	mochiweb_http:stop(bkdweb_mochiweb),
	{noreply, P};
handle_cast(_, P) ->
	{noreply, P}.

handle_info(_, P) -> 
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	gen_server:cast(?MODULE,{start_mochiweb}),
	{ok, #bm{}};
init(Servers) ->
	gen_server:cast(?MODULE,{start_mochiweb,Servers}),
	{ok,#bm{}}.

http_statreq(Req) ->
	case catch http_req(Req:get_header_value("host"),Req) of
		ok ->
			ok;
		{'EXIT',normal} ->
			exit(normal);
		_X ->
			io:format("Http req ~p invalid response ~p~n", [{Req:get_header_value("host"),Req:get(path)},_X]),
			Req:not_found()
	end.	

http_req(Req) ->
	%io:format("mochireq ~p~n", [Req:get_header_value("host")++Req:get(path)]),
	case catch http_req(Req:get_header_value("host"),Req) of
		ok ->			
			ok;
		{'EXIT',normal} ->
			exit(normal);
		_X ->
			io:format("Http req ~p invalid response ~p~n", [{Req:get_header_value("host"),Req:get(path)},_X]),
			Req:not_found()
	end.
 
http_req(Host, Req) ->
	case catch apply(bkdcore_mochi_conf,mod,[Host]) of
		{'EXIT',Err} ->
			lager:error("trying to access domain ~p but got error ~p instead",[Host,Err]),
			Req:respond({404, [{"Content-Type", "text/plain"}],
           		 <<"Unknown host">>});
		ExMod ->
			apply(ExMod,out,[Req]),
			ok
	end.

register(Servers) ->
	supervisor:start_child(bkdcore_sup, {?MODULE, {?MODULE, start, Servers}, permanent, 100, worker, [?MODULE]}).
register() ->
	supervisor:start_child(bkdcore_sup, {?MODULE, {?MODULE, start, []}, permanent, 100, worker, [?MODULE]}).
start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop() ->
	gen_server:call(?MODULE, stop).
print_info() ->
	gen_server:call(?MODULE, {print_info}).