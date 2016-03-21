% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_rpc).
% -behaviour(gen_server).
-include("bkdcore.hrl").
-compile([{parse_transform, lager_transform}]).
% API
-export([call/2,cast/2,async_call/3,multicall/2,multicall/3,is_connected/1,isolate/1, isolate_from/2,stop/1]).
% gen_server
% -export([start/0,start/1,start/2, stop/1,stop/0, init/1, handle_call/3,
% 		  handle_cast/2, handle_info/2, terminate/2, code_change/3,t/0]).
-export([start_link/4,init/4, init/1, exec_gather1/1]).
% -compile([export_all]).
-define(CHUNKSIZE,16834).

% RPC between nodes. Uses proc_lib directly instead of gen_server. 
% This gives us two important optimisations:
% - We can batch together multiple calls in a single gen_tcp:send. 
% - We can do term_to_binary on the caller process.
% Made possible by using receive ... after 0 to combine calls.
% And by manually putting together {self(),make_ref()}.

% Large calls are supported. Every call is split into 16kB chunks.
%  So sending multimegabyte data over RPC is fine and will not block other smaller calls 
%  for too long.

call(Node,Msg) ->
	% ?INF("rpc to ~p ~p",[Node,bkdcore:nodelist()]),
	Ref = make_ref(),
	case getpid(Node) of
		Pid when is_pid(Pid) ->
			MonRef = erlang:monitor(process, Pid),
			From = {self(), Ref},
			Pid ! {call,From, term_to_binary({From, Msg},[{minor_version,1}])},
			Answer = recv_answer(MonRef,Ref, infinity),
			erlang:demonitor(MonRef,[flush]),
			case Answer of
				{'EXIT',{noproc,_}} ->
					erlang:yield(),
					call(Node,Msg);
				{'EXIT',{normal,_}} ->
					{error,econnrefused};
				{invalidnode,_} ->
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

recv_answer(Mon,Ref,Time) ->
	receive
		{Ref,Resp} ->
			Resp;
		{'DOWN',Mon,_,_Pid,Reason} ->
			Reason
	after Time ->
		{error,timeout}
	end.

is_connected(Node) ->
	Ref = make_ref(),
	case getpid(Node) of
		Pid when is_pid(Pid) ->
			% Res = (catch gen_server:call(Pid,is_connected));
			MonRef = erlang:monitor(process, Pid),
			Pid ! {{self(),Ref},is_connected},
			Res = recv_answer(MonRef,Ref,5000),
			erlang:demonitor(MonRef,[flush]);
		_ ->
			Res = error
	end,
	Res == true.

multicall(Nodes,Msg) ->
	multicall(Nodes,Msg,1000*60*5).
multicall(Nodes,Msg,MaxTime) when is_list(Nodes) ->
	Ref = make_ref(),
	{NumCalls,Errs} = lists:foldl(fun(Nd,{Count,Errors}) ->
		case async_call(Ref,Nd,Msg) of
			{error,_} ->
				{Count,[Nd|Errors]};
			_Ret ->
				{Count+1,Errors}
		end
	end,{0,[]},Nodes),
	StartTime = erlang:monotonic_time(),
	% erlang:convert_time_unit(AfterT-BeforeT, native, nano_seconds)
	multicall(NumCalls,Ref,[],Errs,StartTime,MaxTime).

multicall(0,_,Results,BadNodes,_StartTime,_MaxTime) ->
	{Results,BadNodes};
multicall(Count,Ref,Results,Bad,StartTime,MaxTime) ->
	Now = erlang:monotonic_time(),
	case erlang:convert_time_unit(Now-StartTime, native, milli_seconds) of
		Diff when Diff >= MaxTime ->
			{Results,Bad};
		_ ->
			receive
				{{Ref,MonRef,Nd},{error,econnrefused}} ->
					erlang:demonitor(MonRef,[flush]),
					multicall(Count-1,Ref,Results,[Nd|Bad],StartTime,MaxTime);
				{{Ref,MonRef,_Nd},Res} ->
					erlang:demonitor(MonRef,[flush]),
					multicall(Count-1,Ref,[Res|Results],Bad,StartTime,MaxTime)
			after 20 ->
				multicall(Count,Ref,Results,Bad,StartTime,MaxTime)
			end
	end.


async_call(Ref,Node,Msg) ->
	% ?INF("arpc to ~p ~p",[Node,bkdcore:nodelist()]),
	case getpid(Node) of
		Pid when is_pid(Pid) ->
			% gen_server:cast(Pid,{call,From,Msg});
			MonRef = erlang:monitor(process,Pid),
			Ret = {Ref,MonRef,Node},
			From = {self(),Ret},
			Pid ! {call,From,term_to_binary({From,Msg},[{minor_version,1}])},
			Ret;
		Err ->
			Err
	end.

cast(Node,Msg) ->
	case getpid(Node) of
		Pid when is_pid(Pid) ->
			Pid ! {call,undefined,term_to_binary({undefined,Msg},[{minor_version,1}])},
			ok;
			% gen_server:cast(Pid,{cast,Msg});
		Err ->
			Err
	end.

% For testing network splits.
isolate(Bool) ->
	case catch ranch_server:get_connections_sup(bkdcore_in) of
		Cons when is_pid(Cons) ->
			application:set_env(bkdcore,isolated,Bool),
			L = supervisor:which_children(Cons),
			lager:info("Isolating to pids=~p",[L]),
			% Receiver connections
			[Pid ! {isolated,Bool} || {bkdcore_rpc,Pid,worker,[bkdcore_rpc]} <- L],
			% Sender connections
			[butil:safesend(distreg:whereis({bkdcore,Nd}),{isolated,Bool}) || Nd <- bkdcore:nodelist()],
			ok;
		_ ->
			ok
	end.

isolate_from(Nd,Bool) ->
	case catch ranch_server:get_connections_sup(bkdcore_in) of
		Cons when is_pid(Cons) ->
			L = supervisor:which_children(Cons),
			lager:info("Isolate from=~p, cons=~p",[Nd,L]),
			[Pid ! {isolated,Nd,Bool} || {bkdcore_rpc,Pid,worker,[bkdcore_rpc]} <- L];
		_ ->
			ok
	end.


start(Node) when is_binary(Node) ->
	% Ref = make_ref(),
	% case gen_server:start(?MODULE, [{self(),Ref},Node], []) of
	case proc_lib:start(?MODULE, init, [[self(),Node]]) of
		{ok,Pid} ->
			{ok,Pid};
		{error,E} ->
			{error,E}
	end.
% start(Id,Info) ->
% 	gen_server:start_link(?MODULE, [Id,Info], []).

% stop() ->
% 	gen_server:call(?MODULE, stop).
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
					{error,E};
				{ok,Pid1} ->
					Pid1
			end;
		Pid ->
			ok
	end,
	Pid.

-record(dp,{sock,sendproc,calln = 0, callcount = 0,
respawn_timer = 0, iteration = 0, canbatch = false,
permanent = false, direction,transport, tunnelstate,
isinit = false, connected_to,tunnelmod, reconnecter,
isolated = false, executors = []}).

-record(executor,{pid, mon, runs = 0, callsize = 0, callid, home, isolated = false}).
% Ranch
start_link(Ref, Socket, Transport, Opts) ->
	proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

init(Ref, Socket, Transport, _Opts) ->
	ok = proc_lib:init_ack({ok, self()}),
	ok = ranch:accept_ack(Ref),
	ok = Transport:setopts(Socket, [{active, once},{packet,4},{keepalive,true},{send_timeout,10000},{nodelay,true}]),
	% erlang:send_after(5000,self(),timeout),
	loop(#dp{direction = receiver,sock = Socket, transport = Transport}).
	% gen_server:enter_loop(?MODULE, [], #dp{direction = receiver,sock = Socket, transport = Transport}).

init([{?MODULE,_ConnectedTo,Iteration},P]) ->
	{ok,P#dp{iteration = Iteration, respawn_timer = 0}};
init([From,Node]) ->
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
					case gen_tcp:connect(IP,Port,[{packet,4},{keepalive,true},binary,{active,once},
							{send_timeout,2000},{nodelay,true}],2000) of
						{ok,S} ->
							case gen_tcp:send(S,bkdcore:rpccookie(Node)) of
								ok ->
									proc_lib:init_ack(From,{ok,self()}),
									self() ! {call,{self(),ok}, term_to_binary({{self(),ok}, canbatch},[{minor_version,1}])},
									loop(#dp{sock = S, direction = sender, connected_to = Node, isolated = Isolated});
								_ ->
									proc_lib:init_ack(From,{ok,self()}),
									erlang:send_after(1000,self(),reconnect),
									gen_tcp:close(S),
									loop(#dp{direction = sender, connected_to = Node, isolated = Isolated})
							end;
						_Err ->
							lager:error("Unable to connect to node=~p, addr=~p, err=~p",[Node,{IP,Port},_Err]),
							proc_lib:init_ack(From,{ok,self()}),
							erlang:send_after(1000,self(),reconnect),
							loop(#dp{direction = sender, connected_to = Node, isolated = Isolated})
					end;
				undefined ->
					lager:error("Node address does not exist ~p",[Node]),
					proc_lib:init_ack(From,{error,invalidnode})
			end;
		name_exists ->
			proc_lib:init_ack(From,{error,name_exists})
	end.



-define(TIMEOUT,5000).
% tunnel loop
loop(#dp{direction = receiver, isinit = false} = P) ->
	Key = bkdcore:rpccookie(),
	receive
		{tcp,S,<<Key:40/binary,Rem/binary>>} ->
			case Rem of
				<<>> ->
					inet:setopts(P#dp.sock,[{active, once}]),
					loop(P#dp{isinit = true});
				<<"tunnel,",Mod1/binary>> ->
					inet:setopts(P#dp.sock,[{active, 32}]),
					[From,Mod] = butil:split_first(Mod1,<<",">>),
					ok = gen_tcp:send(S,<<"ok">>),
					lager:debug("Started tunnel from ~p",[From]),
					loop(P#dp{direction = tunnel, isinit = true, permanent = true, connected_to = From,
						tunnelmod = binary_to_existing_atom(Mod,latin1)})
			end;
		{tcp,_,_} ->
			ok
		after 1000 ->
			timeout
	end;
loop(#dp{direction = tunnel, isolated = true} = P) ->
	receive
		{tcp,_S,_Bin} ->
			loop(P);
		{isolated,Nd,I} when P#dp.connected_to == Nd ->
			loop(P#dp{isolated = I});
		{isolated,_,_} ->
			loop(P);
		{isolated,I} ->
			loop(P#dp{isolated = I})
	end;
loop(#dp{direction = tunnel} = P) ->
	receive
		{tcp,_S,Bin} ->
			case catch apply(P#dp.tunnelmod,tunnel_bin,[P#dp.tunnelstate,Bin]) of
				{'EXIT',_Err}  ->
					loop(P);
				State ->
					loop(P#dp{tunnelstate = State})
			end;
		{tcp_passive, _Socket} ->
			% case P#dp.respawn_timer > 3000 of
			% 	true ->
			% 		handle_info(restart,P);
			% 	false ->
					inet:setopts(P#dp.sock,[{active, 32}]),
					loop(P);
			% end;
		{tcp_closed,_} ->
			ok;
		{isolated,Nd,I} when P#dp.connected_to == Nd ->
			lager:info("Isolation=~p, for con=~p, direction=~p",[I,P#dp.connected_to,P#dp.direction]),
			loop(P#dp{isolated = I});
		{isolated,_,_} ->
			loop(P);
		{isolated,I} ->
			lager:info("Isolation=~p, for con=~p, direction=~p",
				[I,P#dp.connected_to,P#dp.direction]),
			loop(P#dp{isolated = I});
		X ->
			lager:error("Received invalid msg ~p",[X])
			% spawn(fun() -> timer:sleep(1000), 
			% 	supervisor:delete_child(bkdcore_sup,{?MODULE,P#dp.connected_to,P#dp.iteration}) 
			% end)
	end;
loop(P) ->
	loop(P,[],[],?TIMEOUT).

reply({Pid,Ref},Msg) ->
	Pid ! {Ref,Msg};
reply(_,_) ->
	ok.

% RPC loop
loop(P, ToSend, HaveSent, Timeout) ->
	receive
	{tcp,_Sock,<<(16#ffffff):24/unsigned,Bin/binary>>} ->
		loop(readcombined(P,Bin), ToSend, HaveSent, 0);
	{tcp,_Sock,Bin} ->
		loop(readsingle(P,Bin), ToSend, HaveSent, 0);
	{_,canbatch} ->
		loop(P#dp{canbatch = true}, ToSend, HaveSent, 0);
	{call,From,Bin} ->
		case P#dp.sock of
			undefined ->
				reply(From, {error,econnrefused}),
				loop(P, ToSend, HaveSent, 0);
			_ when P#dp.isolated ->
				reply(From,{error,econnrefused}),
				loop(P, ToSend, HaveSent, 0);
			_ ->
				loop(P#dp{calln = P#dp.calln + 1, callcount = P#dp.callcount+1}, 
					[{P#dp.calln,init, Bin}|ToSend],
					HaveSent, 0)
		end;
	{reply,E,Bin} ->
		loop(P#dp{calln = P#dp.calln + 1, callcount = P#dp.callcount-1, 
				executors = store_ex(E,P#dp.executors)}, 
			[{P#dp.calln,init, Bin}|ToSend],
			HaveSent, 0);
	{decr_callcount,E} ->
		loop(P#dp{callcount = P#dp.callcount - 1, executors = store_ex(E,P#dp.executors)}, ToSend, HaveSent, 0);
	{From,is_connected} ->
		reply(From,is_port(P#dp.sock)),
		loop(P, ToSend, HaveSent, 0);
	{'DOWN',_Monitor,_,Pid,Reason} ->
		case get(Pid) of
			undefined when Pid == P#dp.reconnecter ->
				case Reason of
					false ->
						loop(P#dp{reconnecter = undefined},ToSend, HaveSent, 0);
					invalidnode ->
						exit({error,invalidnode});
					Socket ->
						?INF("Reconnected to ~p",[P#dp.connected_to]),
						inet:setopts(Socket,[{active, once}]),
						loop(P#dp{sock = Socket, reconnecter = undefined},ToSend, HaveSent, 0)
				end;
			undefined ->
				case lists:keyfind(Pid,#executor.pid,P#dp.executors) of
					false ->
						loop(P, ToSend, HaveSent, 0);
					_Ex ->
						?ERR("Executor died ~p",[Reason]),
						loop(P#dp{executors = lists:keydelete(Pid,#executor.pid,P#dp.executors)}, ToSend, HaveSent, 0)
				end;
			Id ->
				erase(Id),
				loop(P, ToSend, HaveSent, 0)
		end;
	{tcp_closed,_} ->
		[exit(Pid,tcp_closed) || {_Id,{_,Pid}} <- get(), is_pid(Pid)];
	reconnect ->
		erlang:send_after(1000,self(),reconnect),
		case P#dp.reconnecter of
			undefined when P#dp.sock == undefined ->
				Me = self(),
				{Pid,_} = spawn_monitor(fun() -> connect_to(Me,P#dp.connected_to) end),
				loop(P#dp{reconnecter = Pid}, ToSend, HaveSent, 0);
			_ ->
				loop(P, ToSend, HaveSent, 0)
		end;
	{isolated,Nd,I} when Nd == P#dp.connected_to ->
		lager:info("Isolation=~p, for con=~p, direction=~p",[I,P#dp.connected_to,P#dp.direction]),
		loop(P#dp{isolated = I}, ToSend, HaveSent, 0);
	{isolated,_,_} ->
		loop(P, ToSend, HaveSent, 0);
	{isolated,I} ->
		lager:info("Isolation=~p, for con=~p, direction=~p",
			[I,P#dp.connected_to,P#dp.direction]),
		loop(P#dp{isolated = I}, ToSend, HaveSent, 0);
	{tcp_error,_,_} ->
		self() ! {tcp_closed,P#dp.sock},
		loop(P, ToSend, HaveSent, 0);
	X ->
		lager:error("Ignored rpc ~p",[X]),
		loop(P, ToSend, HaveSent, 0)
	after Timeout ->
		case ok of
			_ when ToSend == [], HaveSent == [], Timeout == 0 ->
				loop(P, [], [], ?TIMEOUT);
			_ when ToSend == [], HaveSent == [] ->
				case ok of
					_ when P#dp.permanent == false, P#dp.callcount == 0, 
							P#dp.direction == sender, P#dp.isolated == false ->
						case P#dp.connected_to of
							undefined ->
								ok;
							_ ->
								distreg:unreg({bkdcore,P#dp.connected_to}),
								% erlang:send_after(5000,self(),timeout),
								loop(P#dp{connected_to = undefined}, [],[],?TIMEOUT)
						end;
					_ ->
						loop(P, [], [], ?TIMEOUT)
				end;
			_ when ToSend == [] ->
				loop(P, HaveSent, [], 0);
			_ when P#dp.canbatch == false ->
				[{Id,init,Bin}|_] = ToSend,
				Packet = [<<(Id):24/unsigned,(byte_size(Bin)):32/unsigned>>,Bin],
				ok = gen_tcp:send(P#dp.sock,Packet),
				loop(P,tl(ToSend),HaveSent, 0);
			_ ->
				{SendIo, ToSend2} = combinereqs(ToSend,[],[]),
				ok = gen_tcp:send(P#dp.sock, [<<(16#ffffff):24/unsigned>>,SendIo]),
				loop(P, ToSend2, HaveSent, 0)
		end
	end.

store_ex(E,L) ->
	erase(E#executor.callid),
	Ex = E#executor{runs = E#executor.runs + 1},
	case Ex#executor.runs > 100 of
		true ->
			Ex#executor.pid ! stop,
			L;
		false ->
			[Ex|L]
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

readcombined(P,<<Id:24/unsigned, SizeAndBody/binary>>) ->
	case get(Id) of
		undefined ->
			case P#dp.direction of
				receiver ->
					CallCount = P#dp.callcount + 1;
				_ ->
					CallCount = P#dp.callcount
			end,
			<<EntireSize:32/unsigned, ChunkSize:16/unsigned,Chunk:ChunkSize/binary,Next/binary>> = SizeAndBody,
			readcombined(pick_exec(P#dp{callcount = CallCount}, Id, EntireSize, Chunk), Next);
		Pid ->
			CallCount = P#dp.callcount,
			<<ChunkSize:16/unsigned,Chunk:ChunkSize/binary,Next/binary>> = SizeAndBody,
			Pid ! {chunk,Chunk},
			readcombined(P#dp{callcount = CallCount}, Next)
	end;
readcombined(P,<<>>) ->
	inet:setopts(P#dp.sock,[{active, once}]),
	P.

readsingle(P,<<Id:24/unsigned, SizeAndBody/binary>>) ->
	inet:setopts(P#dp.sock,[{active, once}]),
	case get(Id) of
		undefined ->
			case P#dp.direction of
				receiver ->
					CallCount = P#dp.callcount + 1;
				_ ->
					CallCount = P#dp.callcount
			end,
			<<EntireSize:32/unsigned, Chunk/binary>> = SizeAndBody,
			pick_exec(P#dp{callcount = CallCount}, Id, EntireSize, Chunk);
		Pid ->
			CallCount = P#dp.callcount,
			Pid ! {chunk,SizeAndBody},
			P#dp{callcount = CallCount}
	end.

pick_exec(P,Id,EntireSize,Chunk) ->
	Home = self(),
	case P#dp.executors of
		[] ->
			{ProcPid,ProcMon} = spawn_monitor(fun() -> exec_gather(Home) end),
			Ex = #executor{pid = ProcPid, mon = ProcMon, home = self()},
			T = [];
		[Ex|T] ->
			ok
	end,
	Ex#executor.pid ! {start,Ex#executor{callsize = EntireSize, isolated = P#dp.isolated, callid = Id}, Chunk},
	put(Id,Ex#executor.pid),
	P#dp{executors = T}.

% Combines all packets into a single iolist.
% At most from every buffer it takes ?CHUNKSIZE bytes.
combinereqs([{Id,init,Bin}|T], Out, OutRem) when byte_size(Bin) =< ?CHUNKSIZE ->
	Packet = [<<(Id):24/unsigned,(byte_size(Bin)):32/unsigned,(byte_size(Bin)):16/unsigned>>,Bin],
	combinereqs(T, [Packet|Out], OutRem);
combinereqs([{Id,init,<<From:(?CHUNKSIZE)/binary,Rem/binary>> = Bin}|T], Out, OutRem) ->
	Packet = [<<(Id):24/unsigned,(byte_size(Bin)):32/unsigned, (?CHUNKSIZE):16/unsigned>>,From],
	combinereqs(T, [Packet|Out], [{Id,Rem}|OutRem]);
combinereqs([{Id,Rem}|T], Out, OutRem) when byte_size(Rem) =< ?CHUNKSIZE ->
	Packet = [<<(Id):24/unsigned,(byte_size(Rem)):16/unsigned>>,Rem],
	combinereqs(T, [Packet|Out], OutRem);
combinereqs([{Id,<<Chunk:(?CHUNKSIZE)/binary, Rem/binary>>}|T], Out, OutRem) ->
	Packet = [<<(Id):24/unsigned,(?CHUNKSIZE):16/unsigned>>,Chunk],
	combinereqs(T, [Packet|Out], [{Id,Rem}|OutRem]);
combinereqs([],Out,Rem) ->
	{Out,Rem}.

exec_gather(Home) when is_pid(Home) ->
	erlang:monitor(process,Home),
	exec_gather1(Home).

exec_gather1(Home) ->
	receive
		stop ->
			ok;
		{start,Ex,Chunk} ->
			case Ex#executor.callsize =< byte_size(Chunk) of
				true ->
					exec(Ex, Chunk),
					exec_gather1(Home);
				false ->
					exec_gather1(Ex,byte_size(Chunk),[Chunk])
			end;
		{'DOWN',_Monitor,_,Home,_Reason} ->
			ok;
		_ ->
			% exec might produce messages. We no longer care about them.
			exec_gather1(Home)
	after 1000 ->
		erlang:hibernate(?MODULE, exec_gather1,[Home])
	end.
exec_gather1(E,Sz,L) when Sz < E#executor.callsize ->
	receive
		{chunk,Chunk} ->
			exec_gather1(E, Sz + byte_size(Chunk), [Chunk|L]);
		{'DOWN',_Monitor,_,Home,_Reason} when Home == E#executor.home ->
			ok
	end;
exec_gather1(Ex,_,L) ->
	exec(Ex, iolist_to_binary(lists:reverse(L))),
	exec_gather1(Ex#executor.home).

% We are isolated, always return econnrefused.
exec(#executor{isolated = true} = E,Msg) ->
	case binary_to_term(Msg) of
		{rpcreply,{From,_X}} ->
			?ERR("Replying erorr closed!",[]),
			reply(From, {error,econnrefused}),
			E#executor.home ! {decr_callcount,E};
		{undefined,_} ->
			ok;
		{From,_} ->
			?ERR("Replying erorr closed! on ~p",[From]),
			E#executor.home ! {reply,E,term_to_binary({rpcreply,{From,{error,econnrefused}}},[{minor_version,1}])}
	end;
exec(E,Msg) ->
	case binary_to_term(Msg) of
		{rpcreply,{From,X}} ->
			reply(From,X),
			E#executor.home ! {decr_callcount,E};
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
					E#executor.home ! {reply,E,term_to_binary({rpcreply,{From,X}},[{minor_version,1}])};
				_ ->
					E#executor.home ! {decr_callcount,E}
			end;
		{From,ping} ->
			E#executor.home ! {reply,E,term_to_binary({rpcreply,{From,pong}},[{minor_version,1}])};
		{From,canbatch} ->
			E#executor.home ! {ok,canbatch},
			E#executor.home ! {reply,E,term_to_binary({rpcreply,{From,canbatch}},[{minor_version,1}])};
		{From,What} ->
			E#executor.home ! {reply,E,term_to_binary({rpcreply,{From,{module_not_alowed,What}}},[{minor_version,1}])}
	end.









% t() ->
% 	Bin = mkbin(<<>>),
% 	io:format("Starting ~p ~p~n",[os:timestamp(),byte_size(Bin)]),
% 	spawn(fun() -> Res = bkdcore:rpc("node3",{erlang,byte_size,[Bin]}),io:format("Bytesize response ~p ~p~n",[Res,os:timestamp()]) end),
% 	spawn(fun() -> Res = bkdcore:rpc("node3",ping),io:format("Ping response ~p ~p~n",[Res,os:timestamp()]) end).

% mkbin(Bin) when byte_size(Bin) > 1024*1024 ->
% 	Bin;
% mkbin(Bin) ->
% 	mkbin(<<Bin/binary,(butil:flatnow()):64>>).
