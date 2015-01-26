% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_app).
-behavior(application).
-export([start/2, stop/1]).
-compile(export_all).

% Environment variables: 
%  - etc = path to etc folder
%  - name = name of node
%  - docompile = turn of compilation of erl and dtl files

init() ->
	application:start(bkdcore,permanent).

start(_Type, _Args) ->
	application:start(asn1),
	application:start(distreg),
	application:start(yamerl),
	butil:wait_for_app(yamerl),
	Params = [begin
							case application:get_env(bkdcore,K) of
								{ok,V} ->
									{K,V};
								_ ->
									{K,undefined}
							end
						end || K <- [statepath,webport,rpcport,name,key,pem,crt]],
	case butil:ds_val(name,Params) of
		undefined ->
			[Name|_] = string:tokens(butil:tolist(node()),"@"),
			application:set_env(bkdcore,name,butil:tobin(Name));
		Name ->
			ok
	end,
	% Name should be binary
	case application:get_env(bkdcore,name) of
		{ok,[_|_] = SN} ->
			application:set_env(bkdcore,name,butil:tobin(SN));
		_ ->
			ok
	end,
	bkdcore:rpccookie(),
	[begin
		Val = butil:tolist(Val1),
		case Key of
		% statepath when hd(Val) /= $~ andalso hd(Val) /= $/ andalso Val1 /= undefined ->
		% 	application:set_env(bkdcore,Key,butil:expand_path([$~,$/|butil:tolist(Val)]));
		_ when Val1 == undefined ->
			ok;
		_ ->
			application:set_env(bkdcore,Key,butil:expand_path(butil:tolist(Val)))
		end 
	end || {Key,Val1} <- Params, lists:member(Key,[key,crt,pem,statepath])],

	% io:format("Application params ~p~n",[application:get_all_env(bkdcore)]),
	
	application:set_env(bkdcore,starttime,os:timestamp()),
	application:set_env(bkdcore,randnum,erlang:phash2([Name,now()])),

	bkdcore_changecheck:startup_node(),
	{ok,SupPid} = bkdcore_sup:start_link(),
		
	case application:get_env(bkdcore,rpcport) of
		undefined ->
			ok;
		{ok,RpcPort} ->
			case node() of
				'nonode@nohost' ->
					IP = {127,0,0,1};
				_ ->
					case string:tokens(butil:tolist(node()),"@") of
						[_,IP1] ->
							IP = butil:ip_to_tuple(IP1);
						_ ->
							IP = {127,0,0,1}
					end
			end,
			case gen_tcp:connect(IP,RpcPort,[],100) of
				{error,_} ->
					ok;
				{ok,_S} ->
					error_logger:format("Local RPC address already taken ~p:~p~n",[butil:ip_to_list(IP),RpcPort]),
					inet:stop()
			end,
			application:start(ranch),
			{ok, _} = ranch:start_listener(bkdcore_in, 10,
		    ranch_tcp, [{port, RpcPort}, {max_connections, infinity}, {ip,IP}],bkdcore_rpc, [])
	end,
	% bkdcore:startup_node(),
	{ok,SupPid}.


stop(_State) ->
	ok.
