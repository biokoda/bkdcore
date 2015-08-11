% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_rpc).
-behaviour(gen_server).
-include("bkdcore.hrl").
-compile([{parse_transform, lager_transform}]).
% API
-export([call/2,cast/2,async_call/3,multicall/2,is_connected/1,isolate/1]).
% gen_server
-export([start/0,start/1,start/2, stop/1,stop/0, init/1, handle_call/3,
		  handle_cast/2, handle_info/2, terminate/2, code_change/3,t/0]).
-export([start_link/4,init/4]).
% -compile([export_all]).
-define(CHUNKSIZE,16834).

% RPC between bkdcore nodes
% Large calls are supported. Every call is split into 16kB chunks.
%  So sending multimegabyte data over RPC is fine and will not block other smaller calls for longer than it takes to send a 16KB chunk.

isolate(Bool) ->
	case catch ranch_server:get_connections_sup(bkdcore_in) of
		Cons when is_pid(Cons) ->
			application:set_env(bkdcore,isolated,Bool),
			L = supervisor:which_children(Cons),
			[Pid ! {isolate,Bool} || {bkdcore_rpc,Pid,worker,[bkdcore_rpc]} <- L],
			[butil:safesend(distreg:whereis({bkdcore,Nd}),{isolated,Bool}) || Nd <- bkdcore:nodelist()],
			ok;
		_ ->
			ok
	end.
	% gen_server:call(Pid,{isolated,Bool}).

call(Node,Msg) ->
	% ?INF("rpc to ~p ~p",[Node,bkdcore:nodelist()]),
	case getpid(Node) of
		Pid when is_pid(Pid) ->
			case catch gen_server:call(Pid,{call,Msg},infinity) of
				{'EXIT',{noproc,_}} ->
					erlang:yield(),
					call(Node,Msg);
				{'EXIT',{normal,_}} ->
					{error,econnrefused};
				{'EXIT',{invalidnode,_}} ->
					{error,invalidnode};
				normal ->
					{error,econnrefused};
				invalidnode ->
					{error,invalidnode};
				X ->
					X
			end;
		Res ->
			Res
	end.

is_connected(Node) ->
	case getpid(Node) of
		Pid when is_pid(Pid) ->
			Res = (catch gen_server:call(Pid,is_connected));
		_ ->
			Res = error
	end,
	Res == true.


multicall(Nodes,Msg) when is_list(Nodes) ->
	Ref = make_ref(),
	NumCalls = lists:foldl(fun(Nd,Count) ->
		async_call({self(),{Ref,Nd}},Nd,Msg),
		Count+1
	 end,0,Nodes),
	multicall(NumCalls,Ref,[],[]).
multicall(0,_,Results,BadNodes) ->
	{Results,BadNodes};
multicall(Count,Ref,Results,Bad) ->
	receive
		{{Ref,Nd},{error,econnrefused}} ->
			multicall(Count-1,Ref,Results,[Nd|Bad]);
		{{Ref,_Nd},Res} ->
			multicall(Count-1,Ref,[Res|Results],Bad)
	end.


async_call(From,Node,Msg) ->
	% ?INF("arpc to ~p ~p",[Node,bkdcore:nodelist()]),
	case getpid(Node) of
		Pid when is_pid(Pid) ->
			gen_server:cast(Pid,{call,From,Msg});
		Err ->
			Err
	end.

cast(Node,Msg) ->
	% ?INF("rpc cast to ~p ~p",[Node,bkdcore:nodelist()]),
	case getpid(Node) of
		Pid when is_pid(Pid) ->
			gen_server:cast(Pid,{cast,Msg});
		Err ->
			Err
	end.


start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
start(Node) when is_binary(Node) ->
	Ref = make_ref(),
	case gen_server:start(?MODULE, [{self(),Ref},Node], []) of
		{ok,Pid} ->
			{ok,Pid};
		{error,normal} ->
			receive
				{Ref,name_exists} ->
					{error,name_exists};
				{Ref,Err} ->
					{error,Err}
			after 0 ->
				{error,normal}
			end;
		E ->
			E
	end.
start(Id,Info) ->
	gen_server:start_link(?MODULE, [Id,Info], []).

stop() ->
	gen_server:call(?MODULE, stop).
stop(Node) when is_pid(Node) ->
	gen_server:call(Node, stop);
stop(undefined) ->
	ok;
stop(Node) ->
	stop(distreg:whereis({bkdcore,Node})).

getpid(Node) ->
	case distreg:whereis({bkdcore,Node}) of
		undefined ->
			Pid =
			case start(Node) of
				{error,name_exists} ->
					getpid(Node);
				{error,normal} ->
					{error,econnrefused};
				{error,E} ->
					E;
				{ok,Pid1} ->
					Pid1
			end;
		Pid ->
			ok
	end,
	Pid.

-record(dp,{sock,sendproc,calln = 0,callsininterval = 0, callcount = 0,
respawn_timer = 0, iteration = 0,
permanent = false, direction,transport, tunnelstate,
isinit = false, connected_to,tunnelmod, reconnecter, isolated = false}).

handle_call(_Msg,_,#dp{direction = sender, sock = undefined} = P) ->
	{reply,{error,econnrefused},P};
handle_call(_Msg,_,#dp{direction = sender, isolated = true} = P) ->
	{reply,{error,econnrefused},P};
handle_call({call,permanent},_,P) ->
	{reply,ok,P#dp{permanent = true}};
handle_call({call,Msg},From,P) ->
	Bin = term_to_binary({From,Msg},[compressed,{minor_version,1}]),
	handle_call({sendbin,Bin},From,P#dp{callcount = P#dp.callcount + 1});
handle_call({reply,Bin},From,P) ->
	handle_call({sendbin,Bin},From,P#dp{callcount = P#dp.callcount - 1});
handle_call({sendbin,Bin},_,P) ->
	case Bin of
		<<First:?CHUNKSIZE/binary,_/binary>> ->
			put({sendbin,P#dp.calln},{?CHUNKSIZE,Bin}),
			self() ! {continue,P#dp.calln};
		First ->
			ok
	end,
	Packet = [<<(P#dp.calln):24/unsigned,(byte_size(Bin)):32/unsigned>>,First],
	ok = gen_tcp:send(P#dp.sock,Packet),
	{noreply,P#dp{calln = P#dp.calln + 1,
				callsininterval = P#dp.callsininterval + 1}};
handle_call(is_connected,_,P) ->
	{reply,true,P};
handle_call({print_info}, _, P) ->
	io:format("~p~n", [P]),
	{reply, ok, P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast({call,From,Msg},P) ->
	Bin = term_to_binary({From,Msg},[compressed,{minor_version,1}]),
	case handle_call({sendbin,Bin},undefined,P#dp{callcount = P#dp.callcount + 1}) of
		{reply,Err,NP} ->
			gen_server:reply(From,Err),
			{noreply,NP};
		{noreply,NP} ->
			{noreply,NP}
	end;
handle_cast({cast,Msg},P) ->
	Bin = term_to_binary({undefined,Msg},[compressed,{minor_version,1}]),
	case handle_call({sendbin,Bin},undefined,P#dp{callcount = P#dp.callcount + 1}) of
		{reply,_,NP} ->
			{noreply,NP};
		{noreply,NP} ->
			{noreply,NP}
	end;
handle_cast(decr_callcount,P) ->
	{noreply,P#dp{callcount = P#dp.callcount - 1}};
handle_cast(_, P) ->
	{noreply, P}.

handle_info({tcp,S,<<Key:40/binary,Rem/binary>>},#dp{direction = receiver,isinit = false} = P) ->
	Key = bkdcore:rpccookie(),
	case Rem of
		<<>> ->
			inet:setopts(P#dp.sock,[{active, once}]),
			{noreply,P#dp{isinit = true}};
		<<"tunnel,",Mod1/binary>> ->
			inet:setopts(P#dp.sock,[{active, 32}]),
			[From,Mod] = butil:split_first(Mod1,<<",">>),
			ok = gen_tcp:send(S,<<"ok">>),
			lager:debug("Started tunnel from ~p",[From]),
			{noreply,P#dp{direction = tunnel, isinit = true, permanent = true, connected_to = From,
						  tunnelmod = binary_to_existing_atom(Mod,latin1)}}
	end;
handle_info({tcp,_S,_Bin},#dp{direction = tunnel, isolated = true} = P) ->
	{noreply,P};
handle_info({tcp,_S,Bin},#dp{direction = tunnel} = P) ->
	case catch apply(P#dp.tunnelmod,tunnel_bin,[P#dp.tunnelstate,Bin]) of
		{'EXIT',_Err}  ->
			State = P#dp.tunnelstate;
		State ->
			ok
	end,
	{noreply,P#dp{tunnelstate = State, respawn_timer = P#dp.respawn_timer}};
handle_info({tcp_passive, _Socket},P) ->
	% lager:info("tcp_passive ~p",[{P#dp.connected_to,P#dp.iteration}]),
	case P#dp.respawn_timer > 3000 of
		true ->
			handle_info(restart,P);
		false ->
			inet:setopts(P#dp.sock,[{active, 32}]),
			{noreply,P}
	end;
handle_info({isolated,I},P) ->
	{noreply,P#dp{isolated = I}};
handle_info(restart,P) ->
	% lager:info("Restarting tunnel process ~p",[{P#dp.connected_to,P#dp.iteration}]),
	NewId = {?MODULE,P#dp.connected_to,P#dp.iteration+1},
	Spec = {NewId,
		{?MODULE,start,[NewId,P]}, %mfa
		transient,
		100,
		worker,
		[?MODULE]
	},
	{ok,Pid} = supervisor:start_child(bkdcore_sup,Spec),
	% Transfer ownership to new process
	ok = gen_udp:controlling_process(P#dp.sock,Pid),
	% Wake that process up
	Pid ! {tcp_passive,P#dp.sock},
	spawn(fun() -> timer:sleep(1000), supervisor:delete_child(bkdcore_sup,{?MODULE,P#dp.connected_to,P#dp.iteration}) end),
	{stop,normal,P};
handle_info({tcp,_,<<Id:24/unsigned,SizeAndBody/binary>>},P) ->
	case get(Id) of
		undefined ->
			case P#dp.direction of
				receiver ->
					CallCount = P#dp.callcount + 1;
				_ ->
					CallCount = P#dp.callcount
			end,
			CallsInInt = P#dp.callsininterval + 1,
			Home = self(),
			<<Size:32/unsigned,Body/binary>> = SizeAndBody,
			case Size == byte_size(Body) of
				true ->
					{ProcPid,_} = spawn_monitor(fun() -> exec(P#dp.isolated,Home,Body) end);
				false ->
					{ProcPid,_} = spawn_monitor(fun() -> exec_gather(P#dp.isolated,Home,Body) end)
			end,
			put(Id,{Size - byte_size(Body),ProcPid}),
			put(ProcPid,Id);
		{SizeRem,Pid} ->
			CallCount = P#dp.callcount,
			CallsInInt = P#dp.callsininterval,
			case SizeRem - byte_size(SizeAndBody) =< 0 of
				true ->
					Pid ! {done,SizeAndBody};
				false ->
					Pid ! {chunk,SizeAndBody},
					put(Id,{SizeRem-byte_size(SizeAndBody),Pid})
			end
	end,
	inet:setopts(P#dp.sock,[{active, once}]),
	{noreply,P#dp{calln = P#dp.calln + 1, callcount = CallCount,
					callsininterval = CallsInInt}};
handle_info({continue,N},P) ->
	case get({sendbin,N}) of
		{Skip, Bin} when byte_size(Bin) > ?CHUNKSIZE+Skip ->
			<<_:Skip/binary,First:?CHUNKSIZE/binary,_/binary>> = Bin,
			put({sendbin,N},{Skip+?CHUNKSIZE,Bin}),
			self() ! {continue,N};
		{Skip,Bin} ->
			<<_:Skip/binary,First/binary>> = Bin,
			erase({sendbin,N})
	end,
	ok = gen_tcp:send(P#dp.sock,[<<N:24/unsigned>>,First]),
	{noreply,P};
handle_info({'DOWN',_Monitor,_,PID,Reason}, #dp{reconnecter = PID} = P) ->
	case Reason of
		false ->
			{noreply, P#dp{reconnecter = undefined}};
		invalidnode ->
			{stop,invalidnode,P};
		Socket ->
			?INF("Reconnected to ~p",[P#dp.connected_to]),
			erlang:send_after(5000,self(),timeout),
			inet:setopts(Socket,[{active, once}]),
			{noreply, P#dp{sock = Socket, reconnecter = undefined}}
	end;
handle_info({'DOWN',_Monitor,_,Pid,_Reason},P) ->
	case get(Pid) of
		undefined ->
			{noreply,P};
		Id ->
			erase(Pid),
			erase(Id),
			{noreply,P}
	end;
handle_info({tcp_closed,_},#dp{direction = receiver} = P) ->
	[exit(Pid,tcp_closed) || {_Id,{_,Pid}} <- get(), is_pid(Pid)],
	{stop,normal,P};
handle_info({tcp_closed,_},P) ->
	lager:debug("Connection closed type ~p",[P#dp.direction]),
	case P#dp.direction of
		tunnel ->
			spawn(fun() -> timer:sleep(1000), supervisor:delete_child(bkdcore_sup,{?MODULE,P#dp.connected_to,P#dp.iteration}) end);
		_ ->
			ok
	end,
	{stop,normal,P};
handle_info(timeout,P) ->
	case P#dp.callsininterval of
		0 when P#dp.permanent == false, P#dp.callcount == 0, P#dp.direction == sender, P#dp.isolated == false ->
			% If nothing is going on, first unreg,
			%  then in next timeout die off. To prevent any race conditions.
			case P#dp.connected_to of
				undefined ->
					{stop,normal,P};
				_ ->
					distreg:unreg({bkdcore,P#dp.connected_to}),
					erlang:send_after(5000,self(),timeout),
					{noreply,P#dp{connected_to = undefined}}
			end;
		_ ->
			garbage_collect(),
			erlang:send_after(5000,self(),timeout),
			{noreply,P#dp{callsininterval = 0}}
	end;
handle_info(reconnect,P) ->
	erlang:send_after(1000,self(),reconnect),
	case P#dp.reconnecter of
		undefined when P#dp.sock == undefined ->
			Me = self(),
			{Pid,_} = spawn_monitor(fun() -> connect_to(Me,P#dp.connected_to) end),
			{noreply,P#dp{reconnecter = Pid}};
		_ ->
			{noreply,P}
	end;
handle_info(_Msg, P) ->
	io:format("bkdcoreout invalid msg ~p~n",[_Msg]),
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.

% Ranch
start_link(Ref, Socket, Transport, Opts) ->
	proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

init(Ref, Socket, Transport, _Opts) ->
	ok = proc_lib:init_ack({ok, self()}),
	ok = ranch:accept_ack(Ref),
	ok = Transport:setopts(Socket, [{active, once},{packet,4},{keepalive,true},{send_timeout,10000},{nodelay,true}]),
	erlang:send_after(5000,self(),timeout),
	gen_server:enter_loop(?MODULE, [], #dp{direction = receiver,sock = Socket, transport = Transport}).

init([]) ->
	{ok,#dp{direction = receiver}};
init([{?MODULE,_ConnectedTo,Iteration},P]) ->
	{ok,P#dp{iteration = Iteration, respawn_timer = 0}};
init([{From,FromRef},Node]) ->
	case application:get_env(bkdcore,isolated) of
		{ok,Isolated} ->
			ok;
		_ ->
			Isolated = false
	end,
	case distreg:reg({bkdcore,Node}) of
		ok ->
			case bkdcore:node_address(Node) of
				{IP,Port} ->
					%{ip,butil:ip_to_tuple(element(1,bkdcore:node_address()))}
					case gen_tcp:connect(IP,Port,[{packet,4},{keepalive,true},binary,{active,once},
							{send_timeout,2000},{nodelay,true}],2000) of
						{ok,S} ->
							ok = gen_tcp:send(S,bkdcore:rpccookie(Node)),
							erlang:send_after(5000,self(),timeout),
							{ok, #dp{sock = S, direction = sender, connected_to = Node, isolated = Isolated}};
						_Err ->
							lager:error("Unable to connect to node=~p, addr=~p, err=~p",[Node,{IP,Port},_Err]),
							erlang:send_after(1000,self(),reconnect),
							{ok,#dp{direction = sender, connected_to = Node, isolated = Isolated}}
					end;
				undefined ->
					lager:error("Node address does not exist ~p",[Node]),
					{stop,invalidnode}
			end;
		name_exists ->
			From ! {FromRef,name_exists},
			{stop,normal}
	end.

connect_to(Home,Node) ->
	case (catch bkdcore:node_address(Node)) of
		{IP,Port} when is_list(IP), is_integer(Port) ->
			case gen_tcp:connect(IP,Port,[{packet,4},{keepalive,true},binary,{active,false},
					{send_timeout,2000},{nodelay,true}],2000) of
				{ok,S} ->
					ok = gen_tcp:send(S,bkdcore:rpccookie(Node)),
					gen_tcp:controlling_process(S,Home),
					exit(S);
				_Err ->
					exit(false)
			end;
		_ ->
			exit(invalidnode)
	end.

exec_gather(Isolated,Home,Bin) ->
	erlang:monitor(process,Home),
	exec_sum(Isolated,Home,Bin).
exec_sum(I,Home,Bin) ->
	receive
		{chunk,C} ->
			exec_sum(I,Home,<<Bin/binary,C/binary>>);
		{'DOWN',_Monitor,_,Home,_Reason} ->
			ok;
		{done,C} ->
			exec(I,Home,<<Bin/binary,C/binary>>)
	end.

exec(true,Home,Msg) ->
	case binary_to_term(Msg) of
		{rpcreply,{From,_X}} ->
			?ERR("Replying erorr closed! on ~p",[From]),
			gen_server:reply(From,{error,econnrefused}),
			gen_server:cast(Home,decr_callcount);
		{undefined,_} ->
			ok;
		{From,_} ->
			?ERR("Replying erorr closed! on ~p",[From]),
			gen_server:call(Home,{reply,term_to_binary({rpcreply,{From,{error,econnrefused}}},[compressed,{minor_version,1}])})
	end;
exec(_,Home,Msg) ->
	case binary_to_term(Msg) of
		{rpcreply,{From,X}} ->
			gen_server:reply(From,X),
			gen_server:cast(Home,decr_callcount);
		{From,{Mod,Func,Param}} when Mod /= file, Mod /= filelib, Mod /= init,
				Mod /= io, Mod /= os, Mod /= erlang, Mod /= code ->
			% Start = os:timestamp(),
			case catch apply(Mod,Func,Param) of
				X when From /= undefined ->
					% Stop = os:timestamp(),
					% Diff = timer:now_diff(Stop,Start),
					% case Diff > 10000 of
					% 	true ->
					% 		?ERR("High call time ~p ~p",[Diff,{Func,Param}]);
					% 	_ ->
					% 		ok
					% end,
					gen_server:call(Home,{reply,term_to_binary({rpcreply,{From,X}},[compressed,{minor_version,1}])});
				_ ->
					ok
			end;
		{From,ping} ->
			gen_server:call(Home,{reply,term_to_binary({rpcreply,{From,pong}},[compressed,{minor_version,1}])});
		{From,What} ->
			gen_server:call(Home,{reply,term_to_binary({rpcreply,{From,{module_not_alowed,What}}},[compressed,{minor_version,1}])})
	end.









t() ->
	Bin = mkbin(<<>>),
	io:format("Starting ~p ~p~n",[os:timestamp(),byte_size(Bin)]),
	spawn(fun() -> Res = bkdcore:rpc("node3",{erlang,byte_size,[Bin]}),io:format("Bytesize response ~p ~p~n",[Res,os:timestamp()]) end),
	spawn(fun() -> Res = bkdcore:rpc("node3",ping),io:format("Ping response ~p ~p~n",[Res,os:timestamp()]) end).

mkbin(Bin) when byte_size(Bin) > 1024*1024 ->
	Bin;
mkbin(Bin) ->
	mkbin(<<Bin/binary,(butil:flatnow()):64>>).
