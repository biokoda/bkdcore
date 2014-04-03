% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_sharedstate).
-behaviour(gen_server).
-include("bkdcore.hrl").
-define(LAGERDBG,true).
-define(GLOBAL_MASTER_GROUP_SIZE,7).
-export([start/0, stop/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([print_info/0,reload/0,deser_prop/1]).
-export([am_i_global_master/0,am_i_cluster_master/0,whois_global_master/0,whois_cluster_master/0, 
				subscribe_changes/1, register_app/2,unregister_app/1, app_vote_done/2,node_online/1]).
-export([set_cluster_state/1, set_cluster_state/3,set_global_state/1, set_global_state/3,get_cluster_state/2,
			get_global_state/2,is_ok/0]).
-export([savetermfile/2,readtermfile/1]).
% For internal RPC
-export([clusterstate/0,globalstate/0,ping/1,ping/2,rec_global_broadcast/4,get_global_state/0,find_global_state/2,ping/3]).
% -compile(export_all).
% Purpose:
% - Maintains cluster and global state. Apps can use state for data that needs to be replicated to every node. 
% 		State is a property list of {AppName,Value}
% - Maintains a master node for local cluster and global network.
% - Registration of apps that have a vote for when a node becomes active. This way apps can progress from 
% 		initializing to active state.
%   Apps are also notified whenever global/cluster state changes.

% Once a node becomes master, it will remain a master until it becomes unavailable. If remaining nodes
%  have a majority of the cluster, they will decide which one is new master.
% ?GLOBAL_MASTER_GROUP_SIZE determines how many nodes out of all nodes can become a global master candidate. 
%   Not all nodes are elligible
%  to become global masters. Nodes in this group constantly ping each other.
% Global/Cluster state changes are not executed if there is not a majority. It uses a two-phase commit.

% Module uses a public ETS table:
% {NodeName,true/false/init}  -> node is online, offline or initializing
% {{nodenum,NodeName},Integer} -> bkdcore:nodenum() of node. Used to detect when nodes restart and determines their vote.
% {apps,[{Name,{Mod,Func,Arg}}]} -> registered apps
% {subscribers,[{Mod,Func,Args},...]} -> modules that get nofitications on clusterchange or state change
% {{lastseen,NodeName},os:timestamp()} -> last time nodes pinged each other
% {clusterstate,[{Appname,[{_,_},...]},
% 							 {Appname1,[....]}]}
% {clusterversion,Number},
% {globalstate,[{Appname,[{_,_},..]},
% 						  {Appname1,[...]}]}
% {globalversion,Number}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 										API
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
% Find out state of node in local cluster. true (online), false (offline) or init (starting up)
node_online(Nd) ->
	butil:ds_val(butil:tobin(Nd),?MODULE,false).

% Registered apps participate in decision when node_online goes from init to true for every node in cluster.
% {M,F,A} - function that will return Pid or {ok,Pid} of process that participates in voting.
% Pid will receive messages:
% {bkdcore_sharedstate,NodeName,offline/init/reconnect/online}
% {bkdcore_sharedstate,cluster_state_change}
% {bkdcore_sharedstate,global_state_change}
% {bkdcore_sharedstate,cluster_connected}
% For init and reconnect it must eventually call app_vote_done/2 from Pid.
% State change messages are sent whenever global state is changed.
register_app(Name,{M,F,A}) ->
	case butil:safecall({M,F,A}) of
		{ok,Pid} when is_pid(Pid) ->
			ok;
		Pid when is_pid(Pid) ->
			ok
	end,
	gen_server:call(?MODULE,{register_app,Name,Pid,{M,F,A}},infinity).

% Mod - registered module (can be pid as well)
subscribe_changes(Mod) ->
	gen_server:call(?MODULE,{subscribe,Mod},infinity).

unregister_app(Name) ->
	gen_server:call(?MODULE,{unregister_app,Name},infinity).

app_vote_done(App,Node) ->
	gen_server:cast(?MODULE,{app_vote_done,self(),App,Node}).

is_ok() ->
	gen_server:call(?MODULE,is_ok).

am_i_cluster_master() ->
	% gen_server:call(?MODULE,am_i_cluster_master).
	State = butil:ds_val(clusterstate,?MODULE),
	bkdcore:node_name() == state_val(bkdcore,master,State).
am_i_global_master() ->
	State = butil:ds_val(globalstate,?MODULE),
	bkdcore:node_name() == state_val(bkdcore,master,State).

whois_global_master() ->
	State = butil:ds_val(globalstate,?MODULE),
	state_val(bkdcore,master,State).
whois_cluster_master() ->
	State = butil:ds_val(clusterstate,?MODULE),
	state_val(bkdcore,master,State).
% Set app state.
% Return value: ok | nomajority 
%  In case of nomajority state was not commited because a majority of cluster nodes are not up.
set_cluster_state([{_App,_Key,_Val}|_] = L) ->
	case gen_server:call(?MODULE,{set_clusterstate,L},infinity) of
		{notmaster,undefined} ->
			nomajority;
		{notmaster,Node} ->
			rpc:call(bkdcore:dist_name(Node),?MODULE,set_cluster_state,[L]);
		Res ->
			Res
	end.
set_cluster_state(App,Key,Val)  ->
	set_cluster_state([{App,Key,Val}]).
% Set global app state.
% Return value: ok | nomajority 
%  In case of nomajority state was not commited because a majority of master_group nodes are not up.
set_global_state(L) ->
	case gen_server:call(?MODULE,{set_globalstate,L},infinity) of
		{notmaster,undefined} ->
			nomajority;
		{notmaster,Node} when is_binary(Node) ->
			bkdcore:rpc(Node,{?MODULE,set_global_state,[L]});
		Res ->
			Res
	end.
set_global_state(App,Key,Val)  ->
	set_global_state([{App,Key,Val}]).

% Result: nostate | Term
% nostate = initial state has not been established yet. 
get_cluster_state(App,Key) ->
	gen_server:call(?MODULE,{clusterstate,App,Key},infinity).
get_global_state(App,Key) ->
	gen_server:call(?MODULE,{globalstate,App,Key},infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
% 										Implementation
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
get_global_state() ->
	gen_server:call(?MODULE,globalstate).


start() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:cast(?MODULE,{print_info}).
reload() ->
	gen_server:call(?MODULE, {reload}).


% bkdcore_sharedstate relies on nodeup,nodedown messages from net_kernel.
% after a received net_kernel message it goes to:
% handle_info(figureout_master,_) -> exchange_state/0 -> handle_cast({figureout_master,StateFromOtherNodes},_) -> 
% 	figureout_cluster_master/2 -> i_am_master -> (to every node)
%	handle_call({clusterstate_unconfirmed,Nd,V,S},_,_)  -> (if majority replied ok)
% 			to every node ->  handle_call({clusterstate_confirm,Nd,V},_,P) ->

% Global uses bkdcore:rpc, sorts nodes by name picks ?GLOBAL_MASTER_GROUP_SIZE number of nodes from the top and those become
%    master group. Global master is only picked between them.
% The ?GLOBAL_MASTER_GROUP_SIZE nodes will ping each other every second and also work off of nodeup, nodedown messages.


% localstate,globalstate: [{Name,[{Key,Value},..]},...]
% voters: [{Pid,AppName,{Mod,Func,Args}},...] 
% 				There is at least one voter (this module itself).
% votings: [{Node,VotesYes = [Pid1,Pid2,..]}]
% concluded_votings: [{Node1,PidVerify1},{Node2,PidVerify2},..] 
% 			once all local apps voted yes, move node name to concluded. It will be set to online and removed
% 			once all other nodes have also concluded.
% 			PidVerify is spawned process that checks slave nodes if they also concluded voting
% master_group: group of max 10 nodes which can become global master. If local node is member of this group
% 	it will periodically ping other nodes in group to check if they are alive and do the same type of master voting
%    like inside cluster. This activity is independent of cluster state.
-record(dp,{voters = [], votings = [], concluded_votings = [],
						globalversion = 0,
						globalstate = [], 
						globalhash,
						globalunconfirmed,
						global_commiting,
						global_commiting_from = [],
						global_precommiting = [],
						global_precommiting_from = [],
						global_update_pid,
						global_ping_nodes,
						global_ping_pos = 0,
						global_do_exchange = false,
						exchange_global_state_pid,
						find_global_state_pid,
						clusterversion = 0,
						clusterstate = [],
						clusterhash, 
						clusterunconfirmed,
						cluster_commiting, % Cluster state changes being commited to cluster
						cluster_commiting_from = [], % list handle_call calls to set_clusterstsate
						cluster_precommiting = [], % For statechanges while some are already being commited
						cluster_precommiting_from = [],
						cluster_update_pid,
						exchange_state_pid,
						master_group = [],
						pingref,pingnum = 0,
						connected = false
						}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, dp))).
-define(P2R(Prop), butil:prop2rec(Prop, dp, #dp{}, record_info(fields, dp))).	

apply_states(L,State) ->
	lists:foldl(fun({App,Key,Val},S) -> 
						save_state_val(App,Key,Val,S);
					({{App,Key},Val},S) ->
						save_state_val(App,Key,Val,S)
					end,State,L).

handle_call(is_ok,_,P) ->
	{reply,P#dp.clusterversion > 0 andalso P#dp.globalversion > 0 andalso majority_up(),P};
handle_call({set_clusterstate,L},From,P) ->
	?DBG("set_clusterstate ~p",[L]),
	NS = apply_states(L,P#dp.clusterstate),
	% Apply states to check for master, because this statechange might be for changing masters.
	case am_i_cluster_master(P#dp{clusterstate = NS}) of
		true ->
			case majority_up() of
				% Nothing is in process of commiting so start with this change immediately
				true when P#dp.cluster_commiting == undefined ->
					NV = P#dp.clusterversion + 1,
					handle_call(cluster_state_update,ok,P#dp{cluster_commiting = {NV,NS}, cluster_commiting_from = [From]});
				% Queue up the change to state
				true ->
					{noreply,P#dp{cluster_precommiting = L ++ P#dp.cluster_precommiting, 
												 cluster_precommiting_from = [From|P#dp.cluster_precommiting_from]}};
				false ->
					{reply,nomajority,P}
			end;
		false ->
			{reply,{notmaster,state_val(bkdcore,master,P#dp.clusterstate)},P}
	end;
handle_call(cluster_state_update,_,P) ->
	?DBG("cluster_state_update"),
	case ok of
		_ when P#dp.cluster_commiting == undefined, P#dp.cluster_precommiting /= [] ->
			State = apply_states(lists:reverse(P#dp.cluster_precommiting),P#dp.clusterstate),
			handle_call(cluster_state_update,ok,P#dp{cluster_commiting = {P#dp.clusterversion+1, State}, 
												cluster_commiting_from = P#dp.cluster_precommiting_from,
												cluster_precommiting = [], cluster_precommiting_from = []});
		_ when P#dp.cluster_commiting /= undefined ->
			{NV,NS} = P#dp.cluster_commiting,
			case P#dp.clusterversion of
				0 ->
					[gen_server:reply(From,ok) || From <- P#dp.cluster_commiting_from],
					handle_call({master_update,NV,NS,erlang:phash2(NS)},ok,
									P#dp{cluster_commiting = undefined, cluster_commiting_from = undefined});
				_ when P#dp.cluster_update_pid /= undefined ->
					{noreply,P};
				_ ->
					{Pid,_} = spawn_monitor(fun() -> spread_cluster_state(NV,NS) end),
					{noreply,P#dp{cluster_update_pid = Pid}}
			end;
		_ ->
			{reply,ok,P}
	end;
handle_call({set_globalstate,L},From,P) ->
	?DBG("set_globalstate ~p",[L]),
	NS = apply_states(L,P#dp.globalstate),
	% Apply states to check for master, because this statechange might be for changing masters.
	case am_i_global_master(P#dp{globalstate = NS}) of
		AMI when AMI == true; P#dp.globalversion == 0 ->
			case global_majority_up(P) of
				% Nothing is in process of commiting so start with this change immediately
				X when X == true andalso P#dp.global_commiting == undefined orelse
								P#dp.globalversion == 0 ->
					NV = P#dp.globalversion + 1,
					handle_call(global_state_update,ok,P#dp{global_commiting = {NV,NS}, global_commiting_from = [From]});
				% Queue up the change to state
				true ->
					{noreply,P#dp{global_precommiting = L ++ P#dp.global_precommiting, 
												 global_precommiting_from = [From|P#dp.global_precommiting_from]}};
				false ->
					{reply,nomajority,P}
			end;
		false ->
			{reply,{notmaster,state_val(bkdcore,master,P#dp.globalstate)},P}
	end;
handle_call(global_state_update,_,P) ->
	?DBG("global_state_update"),
	case ok of
		_ when P#dp.global_commiting == undefined, P#dp.global_precommiting /= [] ->
			State = lists:foldl(fun({App,Key,Val},S) -> 
														save_state_val(App,Key,Val,S)
													end,P#dp.globalstate,lists:reverse(P#dp.global_precommiting)),
			handle_call(global_state_update,ok,P#dp{global_commiting = {P#dp.globalversion+1, State}, 
															global_commiting_from = P#dp.global_precommiting_from,
												 global_precommiting = [], global_precommiting_from = []});
		_ when P#dp.global_commiting /= undefined ->
			{NV,NS1} = P#dp.global_commiting,
			case P#dp.globalversion of
				0 ->
					[gen_server:reply(From,ok) || From <- P#dp.global_commiting_from],
					case state_val(bkdcore,master_group,NS1) of
						undefined ->
							% Master = [{bkdcore,master,bkdcore:node_name()},
							% 		  {bkdcore,master_group,,[])}],
							MG = check_master_group(P#dp.master_group,state_val(bkdcore,nodes,NS1),[]),
							NS = save_state_val(bkdcore,master,bkdcore:node_name(),save_state_val(bkdcore,master_group,MG,NS1));
						_ ->
							NS = NS1
					end,
					self() ! figureout_master,
					handle_call({global_master_update,NV,NS,erlang:phash2(NS)},ok,P#dp{global_commiting = undefined, 
												global_commiting_from = undefined});
				_ when P#dp.global_update_pid /= undefined ->
					{noreply,P};
				_ ->
					Masters = state_val(bkdcore,master_group,NS1),
					NewNodes = state_val(bkdcore,nodes,NS1),
					OldNodes = state_val(bkdcore,nodes,P#dp.globalstate),
					NS = save_state_val(bkdcore,master_group,check_master_group(Masters,NewNodes,OldNodes),NS1),
					{Pid,_} = spawn_monitor(fun() -> spread_global_state(P#dp.master_group, NV,NS) end),
					{noreply,P#dp{global_update_pid = Pid}}
			end;
		_ ->
			{reply,ok,P}
	end;
handle_call({has_concluded,Node},_,P) ->
	?DBG("has_concluded ~p",[Node]),
	case butil:ds_val(Node,?MODULE) of
		init ->
			case lists:keyfind(Node,1,P#dp.concluded_votings) of
				false ->
					Res = false;
				_ ->
					Res = true
			end;
		true ->
			Res = true;
		% Either false or undefined. Which means local node does not see Node.
		_ ->
			Res = false
	end,
	{reply,{ok,Res},P};
handle_call({register_app,Name,Pid,MFA},_,P) ->
	?DBG("register_app ~p",[{Name,Pid,MFA}]),
	erlang:monitor(process,Pid),
	case P#dp.connected of
		true ->
			Pid ! {?MODULE,cluster_connected};
		false ->
			ok
	end,
	[Pid ! {?MODULE,Node,Type} || {Node,Type,_Votes} <- P#dp.votings],
	NV = [{Pid,Name,MFA}|P#dp.voters],
	butil:ds_add(apps,NV,?MODULE),
	{reply,ok,P#dp{voters = NV}};
handle_call({subscribe,Proc},_,P) ->
	Subs = butil:ds_val(subscribers,?MODULE,[]),
	butil:ds_add(subscribers,butil:lists_add(Proc,Subs),?MODULE),
	{reply,ok,P};
handle_call({unregister_app,Name},_,P) ->
	?DBG("unregister_app ~p",[Name]),
	case lists:keyfind(Name,2,P#dp.voters) of
		false ->
			{reply,ok,P};
		{Pid,Name,_MFA} ->
			NV = lists:keydelete(Pid,1,P#dp.voters),
			butil:ds_add(apps,NV,?MODULE),
			{reply,ok,P#dp{voters = NV,
											votings = [{Node,lists:delete(Pid,Votes)} || {Node,Votes} <- P#dp.votings]}}
	end;
handle_call({clusterstate_unconfirmed,Nd,V,S},_,P) ->
	?DBG("clusterstate_unconfirmed ~p",[{Nd,V,S}]),
	{reply,ok,P#dp{clusterunconfirmed = {Nd,V,S,erlang:phash2(S)}}};
handle_call({clusterstate_confirm,Nd,V},_,P) ->
	?DBG("set_clusterstate ~p",[{Nd,V}]),
	case P#dp.clusterunconfirmed of
		{Nd,V,S,Hash} ->
			handle_call({master_update,V,S,Hash},ok,P#dp{clusterunconfirmed = undefined});
		_ ->
			{reply,false,P}
	end;
handle_call({clusterstate_confirm,Nd,V,S},_,P) ->
	?DBG("clusterstate_confirm ~p",[{Nd,V,S}]),
	handle_call({clusterstate_confirm,Nd,V},ok,P#dp{clusterunconfirmed = {Nd,V,S}});
handle_call({master_update,Vers,State,InHash},_,P) ->
	?DBG("master_update ~p",[{Vers,State,InHash}]),
	case P#dp.clusterversion < Vers orelse InHash /= P#dp.clusterhash andalso P#dp.clusterversion == Vers of
		true ->
			ets:insert(?MODULE,[{clusterstate,State},{clusterversion,Vers}]),
			[Pid ! {?MODULE,cluster_state_change} || {_App,Pid,_MFA} <- P#dp.voters],
			[butil:safesend(Pid,{?MODULE,cluster_state_change}) || Pid <- butil:ds_val(subscribers,?MODULE)],
			ok = savetermfile([bkdcore:statepath(),"/statecluster"],{Vers,State}),
			{reply,ok, P#dp{clusterstate = State, clusterversion = Vers, clusterhash = erlang:phash2(State)}};
		false ->
			{reply,ok,P}
	end;
handle_call({globalstate_unconfirmed,Nd,V,S},_,P) ->
	?DBG("globalstate_unconfirmed ~p",[{Nd,V}]),
	{reply,ok,P#dp{globalunconfirmed = {Nd,V,S,erlang:phash2(S)}}};
handle_call({globalstate_confirm,Nd,V},_,P) ->
	?DBG("globalstate_confirm ~p",[{Nd,V,P#dp.globalunconfirmed}]),
	case P#dp.globalunconfirmed of
		{Nd,V,S,Hash} ->
			handle_call({global_master_update,V,S,Hash},ok,P#dp{globalunconfirmed = undefined});
		_ when P#dp.globalversion == V ->
			{reply,ok,P};
		_ ->
			?ERR("globalstate can not confirm, my uncofnirmed state ~p, input from ~p with ~p",[P#dp.globalunconfirmed,Nd,V]),
			{reply,false,P}
	end;
handle_call({globalstate_confirm,Nd,V,S},_,P) ->
	?DBG("globalstate_confirm ~p",[{Nd,V,S}]),
	handle_call({globalstate_confirm,Nd,V},ok,P#dp{globalunconfirmed = {Nd,V,S,erlang:phash2(S)}});
handle_call({global_master_update,Vers,State,InHash},_,P) ->
	?DBG("global_master_update ~p",[{Vers,InHash}]),
	case P#dp.globalversion < Vers orelse (InHash /= P#dp.globalhash andalso P#dp.globalversion == Vers) of
		true ->
			ets:insert(?MODULE,[{globalstate,State},{globalversion,Vers}]),
			bkdcore_changecheck:insert_nodes_to_ets(state_val(bkdcore,nodes,State)),
			bkdcore_changecheck:insert_groups_to_ets(state_val(bkdcore,groups,State)),
			global_to_changecheck(State),
			case nodes() of
				[] ->
					erlang:send_after(5000,self(),figureout_master),
					self() ! figureout_master;
				_ ->
					ok
			end,
			[Pid ! {?MODULE,global_state_change} || {_App,Pid,_MFA} <- P#dp.voters],
			[butil:safesend(Pid, {?MODULE,global_state_change}) || Pid <- butil:ds_val(subscribers,?MODULE)],
			% bkdcore_changecheck ! {?MODULE,global_state_change},

			ok = savetermfile([bkdcore:statepath(),"/stateglobal"],{Vers,State}),
			{reply,ok, P#dp{globalstate = State, globalversion = Vers, globalhash = erlang:phash2(State),
											global_ping_nodes = global_ping_nodes(),
											master_group = state_val(bkdcore,master_group,State)}};
		false ->
			{reply,ok,P}
	end;
handle_call({clusterstate,_App,_Key},_,#dp{clusterversion = 0} = P) ->
	{reply,nostate,P};
handle_call({clusterstate,App,Key},_,P) ->
	{reply,butil:ds_val({App,Key},P#dp.clusterstate),P};
handle_call({globalstate,_App,_Key},_,#dp{globalversion = 0} = P) ->
	{reply,nostate,P};
handle_call({globalstate,App,Key},_,P) ->
	{reply,butil:ds_val({App,Key},P#dp.globalstate),P};
handle_call(globalstate,_,P) ->
	case am_i_global_master(P) of
		true ->
			case global_majority_up(P) of
				true ->
					{reply,{ok,P#dp.globalversion,P#dp.globalstate},P};
				false ->
					{reply,{nomajority,P#dp.globalversion,P#dp.globalstate},P}
			end;
		false when P#dp.globalversion == 0 ->
			{reply,undefined,P};
		false ->
			{reply,{ok,P#dp.globalversion,P#dp.globalstate},P}
	end;
handle_call({reload}, _, P) ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	{reply, ok, ?MODULE:deser_prop(?R2P(P))};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

deser_prop(P) ->
	?P2R(P).


check_master_group(Masters,NewNodes,OldNodes) ->
	case NewNodes of
		undefined ->
			NewNames = bkdcore:nodelist();
		_ ->
			NewNames = [begin
					{Name,_AddrReal,_Port,_Pub,_Dist} = bkdcore_changecheck:read_node(Info),Name 
				end || Info <- NewNodes]
	end,
	case OldNodes of
		undefined ->
			OldNames = [];
		_ ->
			OldNames = [begin
					{Name,_AddrReal,_Port,_Pub,_Dist} = bkdcore_changecheck:read_node(Info),Name 
				end || Info <- OldNodes]
	end,
	check_master_group1(Masters,NewNames,OldNames).
check_master_group1(Masters,New,Old) ->
	case lists:subtract(Masters,New) of
		% All master nodes are still part of the cluster
		[] ->
			case length(Masters) < ?GLOBAL_MASTER_GROUP_SIZE of
				true when Masters == [] ->
					check_master_group(New);
				true ->
					case lists:subtract(New,Old) of
						% Nodes have not changed
						[] ->
							Masters;
						NewNodes when Old == [] ->
							check_master_group(Masters++(NewNodes--Masters));
						NewNodes ->
							check_master_group(Masters++NewNodes)
					end;
				false ->
					Masters
			end;
		% Some nodes were removed, forget them.
		Gone ->
			check_master_group1(lists:subtract(Masters,Gone),New,Old)
	end.
check_master_group(All) ->
	case length(All) > ?GLOBAL_MASTER_GROUP_SIZE of
		true ->
			{NewMasters,_} = lists:split(?GLOBAL_MASTER_GROUP_SIZE,All),
			NewMasters;
		false ->
			All
	end.

% Master set and online ok - do nothing
% Master set but not online, majority nodes online - PICK NEW MASTER
% Master set but not online, majority nodes offline - do nothing
% Master not set, majority nodes online - PICK NEW MASTER
% Master not set, majority not online - do nothing
figureout_cluster_master(P,Nodeinfo) ->
	% Highest nodenum is potentially new master
	[{Node,_NodeNum,_Vers,_State}|_] = lists:reverse(lists:keysort(2,Nodeinfo)),
	?DBG("figureout_cluster_master ~p",[{Node,bkdcore:node_name()}]),
	% Only change state on this potentially new master node
	case Node == bkdcore:node_name() of
		true ->
			case state_val(bkdcore,master,P#dp.clusterstate) of
				% Master not set
				undefined ->
					case majority_up() of
						true ->
							i_am_master(P);
						false ->
							erlang:send_after(5000,?MODULE,figureout_master),
							P
					end;
				% Master set
				Master ->
					% Master online?
					case lists:member(bkdcore:dist_name(Master), [bkdcore:dist_name()|nodes()]) of
						true ->
							P;
						false ->
							case majority_up() of
								false ->
									erlang:send_after(5000,?MODULE,figureout_master),
									P;
								true ->
									i_am_master(P)
							end
					end
			end;
		false ->
			P
	end.
i_am_master(P) ->
	% Just spawn and call state change. If nodes come online/offline often there 
	%  may be more than one of these processes running.
	% But state changes will be applied in the right sequence so there should not be a problem.
	spawn(fun() ->
		case gen_server:call(?MODULE,{set_clusterstate,[{bkdcore,master,bkdcore:node_name()}]}) of
			ok ->
				ok;
			_Err ->
				erlang:send_after(5000,?MODULE,figureout_master)
		end
	end),
	P.

spread_cluster_state(NV,NS) ->
	?DBG("spread_cluster_state ~p",[bkdcore:cluster_nodes()]),
	[spawn_monitor(fun() ->  
			exit(rpc:call(bkdcore:dist_name(Nd),gen_server,call,
								[?MODULE,{clusterstate_unconfirmed,bkdcore:node_name(),NV,NS}]))
		  end) || Nd <- bkdcore:cluster_nodes()],
	spawn_monitor(fun() -> exit(gen_server:call(?MODULE,{clusterstate_unconfirmed,bkdcore:node_name(),NV,NS})) end),

	NConfirmed = gather_confirmations(length(bkdcore:cluster_nodes())+1,0),
	case NConfirmed*2 > length(bkdcore:cluster_nodes())+1 of
		true ->
			[spawn_monitor(fun() ->  
									exit(rpc:call(bkdcore:dist_name(Nd),gen_server,call,
													[?MODULE,{clusterstate_confirm,bkdcore:node_name(),NV}]))
							end) || Nd <- bkdcore:cluster_nodes()],
			NConfirmed1 = gather_confirmations(length(bkdcore:cluster_nodes()),0),
			case NConfirmed1 > 0 orelse length(bkdcore:cluster_nodes()) == 0 of
				true ->
					spawn(fun() -> gen_server:call(?MODULE,{clusterstate_confirm,bkdcore:node_name(),NV}) end),
					exit(ok);
				false ->
					exit(nomajority)
			end;
		false ->
			exit(nomajority)
	end.
		 


gather_confirmations(0,Conf) ->
	Conf;
gather_confirmations(N,Conf) ->
	receive
		{'DOWN',_Monitor,_,_PID,ok} ->
			gather_confirmations(N-1,Conf+1);
		{'DOWN',_,_,_PID,_Reason} ->
			gather_confirmations(N-1,Conf)
	end.

majority_up() ->
	case bkdcore:cluster_nodes() of
		% single node cluster
		[] ->
			true;
		Cluster ->
			% Connected number of nodes*2 larger than configured size of cluster
			(length(bkdcore:cluster_nodes_connected())+1)*2 > length(Cluster)
	end.

figureout_global_master(P,Nodeinfo) ->
	% Highest nodenum is potentially new master
	[{Node,_NodeNum,_Vers,_State}|_] = lists:reverse(lists:keysort(2,Nodeinfo)),
	?DBG("figureout_global_master ~p",[{Node,bkdcore:node_name()}]),
	% Only change state on this potentially new master node
	case Node == bkdcore:node_name() of
		true ->
			case state_val(bkdcore,master,P#dp.globalstate) of
				% Master not set
				undefined ->
					case global_majority_up(P) of
						true ->
							i_am_global_master(P);
						false ->
							P
					end;
				% Master set
				Master ->
					% Master online?
					case is_global_node_online(Master) of
						true ->
							P;
						false ->
							case global_majority_up(P) of
								false ->
									P;
								true ->
									i_am_global_master(P)
							end
					end
			end;
		false ->
			P
	end.
i_am_global_master(P) ->
	spawn(fun() ->
		case gen_server:call(?MODULE,{set_globalstate,[{bkdcore,master,bkdcore:node_name()},
										 {bkdcore,master_group,P#dp.master_group}]}) of
			ok ->
				ok;
			_Err ->
				io:format("Cant set globalstate ~p~n",[_Err]),
				ok
		end
	end),
	P.

spread_global_state(Nodes,NV,NS) ->
	[spawn_monitor(fun() ->  
				exit(bkdcore:rpc(Nd,{gen_server,call,[?MODULE,{globalstate_unconfirmed,bkdcore:node_name(),NV,NS}]}))
			  end) || Nd <- Nodes,Nd /= bkdcore:node_name()],
	spawn_monitor(fun() ->exit(gen_server:call(?MODULE,{globalstate_unconfirmed,bkdcore:node_name(),NV,NS})) end),
	?DBG("spread_global_state ~p",[{Nodes,bkdcore:node_name()}]),
	LenNodes = length(Nodes),
	NConfirmed = gather_confirmations(LenNodes,0),
	case NConfirmed*2 >= LenNodes of
		true ->
			[spawn_monitor(fun() ->
									exit(bkdcore:rpc(Nd,{gen_server,call,
												[?MODULE,{globalstate_confirm,bkdcore:node_name(),NV}]}))
							end) || Nd <- Nodes, Nd /= bkdcore:node_name()],
			NConfirmed1 = gather_confirmations(LenNodes-1,0),
			case NConfirmed1 > 0 orelse LenNodes == 1 of
				true ->
					ok = gen_server:call(?MODULE,{globalstate_confirm,bkdcore:node_name(),NV}),
					gen_server:cast(?MODULE,do_global_broadcast),
					exit(ok);
				false ->
					exit(nomajority)
			end;
		false ->
			exit(nomajority)
	end.


global_majority_up(P) ->
	case P#dp.master_group of
		[Nd] ->
			% Must be local node and this is a single node network.
			Nd == bkdcore:node_name();
		_ ->
			NumActive = lists:foldl(fun(Nd,N) ->
										case is_global_node_online(Nd) of
											true ->
												N+1;
											false ->
												N
										end
									end,0,P#dp.master_group),
			NumActive*2 > length(P#dp.master_group)
	end.

is_global_node_online(Nd) ->
	case butil:ds_val({lastseen,Nd},?MODULE) of
		undefined ->
			false;
		{_,_,_} = LS ->
			case timer:now_diff(os:timestamp(),LS) < 2000000 of
				true ->
					true;
				false ->
					false
			end
	end.

% Type = reconnect = tcp reconnect
% Type = init = erlang on node (re)started
% Type = offline = node has disconnected
% Type = online = node has been set as online (all apps sent app_init_done)
handle_cast({nodechange,Node,Type},P) ->
	?DBG("nodechange ~p",[{Node,Type}]),
	[Pid ! {?MODULE,Node,Type} || {Pid,_App,_MFA} <- P#dp.voters],
	[butil:safesend(Pid, {?MODULE,Node,Type}) || Pid <- butil:ds_val(subscribers,?MODULE)],
	case ok of
		_ when Type == offline; Type == online ->
			{noreply,P#dp{votings = lists:keydelete(Node,1,P#dp.votings),
							concluded_votings = lists:keydelete(Node,1,P#dp.concluded_votings)}};
		_ ->
			case lists:keyfind(Node,1,P#dp.votings) of
				false ->
					{noreply,P#dp{votings = [{Node,Type,[]}|P#dp.votings]}};
				% Node was already in the process of being setup. 
				% Is ignoring this message and continue from old one right?????
				_ ->
					{noreply,P}
			end
	end;
handle_cast({app_vote_done,Pid,_App,Node},P) ->
	case lists:keyfind(Node,1,P#dp.votings) of
		{Node,Type,Votes} ->
			Votes1 = butil:lists_add(Pid,Votes),
			case length(P#dp.voters) == length(Votes1) of
				true ->
					% case amimaster(P) of
					% 	true ->
							self() ! check_concluded,
					% 	false ->
					% 		ok
					% end,
					{noreply,P#dp{concluded_votings = lists:keystore(Node,1,P#dp.concluded_votings,{Node,undefined})}};
				false ->
					{noreply,P#dp{votings = lists:keystore(Node,1,P#dp.votings,{Node,Type,Votes1})}}
			end;
		_ ->
			{noreply,P}
	end;
handle_cast({figureout_master,L1},#dp{connected = false} = P) ->
	?DBG("figureout_master ~p",[L1]),
	[Pid ! {?MODULE,cluster_connected} || {Pid,_,_} <- P#dp.voters],
	[butil:safesend(Pid, {?MODULE,cluster_connected}) || Pid <- butil:ds_val(subscribers,?MODULE)],
	handle_cast({figureout_master,L1},P#dp{connected = true});
handle_cast({figureout_master,L1},P) ->
	?DBG("figureout_master ~p",[L1]),
	% Sorted from highest version number to lowest
	L = lists:reverse(lists:keysort(3,[{bkdcore:node_name(),nodenum(),P#dp.clusterversion,P#dp.clusterstate}|L1])),
	[begin
		% Has NodeNum changed from previous check
		case butil:ds_val({{nodenum,Node},NodeNum},?MODULE) of
			% no change
			NodeNum ->
				case butil:ds_val(Node,?MODULE) of
					false ->
						gen_server:cast(?MODULE,{nodechange,Node,reconnect});
					_ ->
						ok
				end;
			% changed or not known, node has restarted
			_ ->
				butil:ds_add({{nodenum,Node},NodeNum},?MODULE),
				butil:ds_add(Node,init,?MODULE),
				gen_server:cast(?MODULE,{nodechange,Node,init})
		end
	 end || {Node,NodeNum,_Vers,_State} <- L],
	case L of
		[] ->
			{noreply,P};
		% Version 0 is higher, cluster just starting and nothing set yet
		[{_,_,0,_}|_] ->
			{noreply,figureout_cluster_master(P,L)};
		[{_Node,_NodeNum,Vers,State}|_] ->
			case P#dp.clusterversion >= Vers of
				true ->
					{noreply, figureout_cluster_master(P,L)};
				false ->
					{_,_,NP} = handle_call({master_update,Vers,State,erlang:phash2(State)},ok,P),
					{noreply,figureout_cluster_master(NP,L)}
			end
	end;
handle_cast({figureout_global_master,L1},P) ->
	?DBG("figureout_global_master ~p",[L1]),
	L = lists:reverse(lists:keysort(3,[{bkdcore:node_name(),nodenum(),P#dp.globalversion,P#dp.globalstate}|L1])),
	case L of
		[{_,_,0,_}|_] = L ->
			{noreply,figureout_global_master(P,L)};
		[{_Node,_NodeNum,Vers,State}|_] ->
			case P#dp.globalversion >= Vers of
				true ->
					{noreply,figureout_global_master(P,L)};
				false ->
					{_,_,NP} = handle_call({global_master_update,Vers,State,erlang:phash2(State)},ok,P),
					case state_val(bkdcore,master_group,State) of
						undefined ->
							Masters = P#dp.master_group;
						Masters ->
							ok
					end,
					case lists:member(bkdcore:node_name(),Masters) of
						true ->
							{noreply,figureout_global_master(NP,L)};
						false ->
							{noreply,NP}
					end
			end
	end;
handle_cast(dobroadcast,P) ->
	case am_i_cluster_master(P) of
		true ->
			[spawn(fun() -> 
				rpc:call(Nd,gen_server,call,
						[?MODULE,{master_update,P#dp.clusterversion,P#dp.clusterstate,P#dp.clusterhash}]) 
			 end) || Nd <- bkdcore:cluster_nodes_connected()];
		false ->
			ok
	end,
	{noreply,P};
handle_cast(do_global_broadcast,P) ->
	case am_i_global_master(P) of
		true ->
			spawn(fun() -> global_broadcast(P#dp.globalversion,P#dp.globalstate,P#dp.globalhash) end);
		false ->
			ok
	end,
	{noreply,P};
handle_cast({print_info},P) ->
	% io:format("~p~nam_i_cluster_master ~p~nam_i_global_master ~p~n",[?R2P(P),am_i_cluster_master(P),am_i_global_master(P)]),
	?INF("~p~n",[?R2P(P)]),
	{noreply,P};
handle_cast(_Msg, P) ->
	?ERR("bkdcore_sharedstate uknown cast ~p~n", [_Msg]),
	{noreply, P}.

rec_global_broadcast(L,V,S,Hash) ->
	gen_server:call(?MODULE,{global_master_update,V,S,Hash}),
	[bkdcore:rpc(Nd,{?MODULE,rec_global_broadcast,[[],V,S,Hash]}) || Nd <- L],
	ok.

global_broadcast(Vers,State,Hash) ->
	Nodes = lists:delete(bkdcore:node_name(),bkdcore:nodelist()),
	global_broadcast(Nodes,Vers,State,Hash).
global_broadcast([Nd|T],Vers,State,Hash) ->
	case T of
		[A,B,C,D,E,F,G,H,I,J,K|Rem] ->
			L = [A,B,C,D,E,F,G,H,I,J,K];
		L->
			Rem = []
	end,
	case bkdcore:rpc(Nd,{?MODULE,rec_global_broadcast,[L,Vers,State,Hash]}) of
		ok ->
			global_broadcast(Rem,Vers,State,Hash);
		_ ->
			global_broadcast(T,Vers,State,Hash)
	end;
global_broadcast([],_,_,_) ->
	ok.

check_concluded(Node) ->
	[spawn_monitor(fun() ->  
					{ok,Bool} = gen_server:call({?MODULE,bkdcore:dist_name(Nd)},{has_concluded,Node}),
					exit({ok,Bool}) 
			   end) || Nd <- bkdcore:cluster_nodes(), node_online(Nd) /= false],
	case gather_concluded(length(bkdcore:cluster_nodes()),[]) of
		ok ->
			butil:ds_add(Node,true,?MODULE),
			gen_server:cast(?MODULE,{nodechange,Node,online});
		_ ->
			ok
	end.


gather_concluded(0,_) ->
	ok;
gather_concluded(N,L) ->
	receive
		{'DOWN',_Monitor,_,_PID,{ok,false}} ->
			false;
		{'DOWN',_Monitor,_,_PID,{ok,true}} ->
			gather_concluded(N-1,L);
		{'DOWN',_,_,_,_} = _Msg ->
			false
	end.

handle_info(check_concluded,P) ->
	case P#dp.concluded_votings of
		[] ->
			{noreply,P};
		_ ->
			erlang:send_after(2000,self(),check_concluded),
			Votings = [begin 
						case Node of
							{Name,undefined} ->
								{CheckPid,_} = spawn_monitor(fun() -> check_concluded(Name) end),
								{Name,CheckPid};
							{Name,Somepid} ->
								{Name,Somepid}
						end
					end || Node <- P#dp.concluded_votings],
			{noreply,P#dp{concluded_votings = Votings}}
	end;
handle_info({?MODULE,Node,Type},P) ->
	case ok of
		_ when Type == init; Type == reconnect ->
			app_vote_done(?MODULE,Node);
		_ ->
			ok
	end,
	{noreply,P};
handle_info({nodeup, Nd},P) ->
	?DBG("nodeup ~p ~p",[Nd,bkdcore:node_name()]),
	case bkdcore:name_from_dist_name(Nd) of
		undefined ->
			{noreply,P};
		Name ->
			case butil:ds_val(Name,?MODULE) of
				undefined ->
					self() ! figureout_master;
				false ->
					self() ! figureout_master;
				_ ->
					ok
			end,
			{noreply,P}
	end;
handle_info({nodedown, Nd},P) ->
	?DBG("nodedown ~p ~p",[Nd,bkdcore:node_name()]),
	case bkdcore:name_from_dist_name(Nd) of
		undefined ->
			{noreply,P};
		Name ->
			case butil:ds_val(Name,?MODULE) of
				X when X == true; X == init ->
					gen_server:cast(?MODULE,{nodechange,Name,offline}),
					self() ! figureout_master,
					butil:ds_add(Name,false,?MODULE);
				_ ->
					ok
			end,
			{noreply,P}
	end;
handle_info({nodedown,global,_Nd},P) ->
	?INF("Nodedown global ~p~n",[_Nd]),
	self() ! figureout_global_master,
	{noreply,P#dp{global_do_exchange = true}};
handle_info({nodeup,global,_Nd},P) ->
	?INF("Nodeup global ~p~n",[_Nd]),
	{noreply,P#dp{global_do_exchange = true}};
handle_info({'DOWN',_Monitor,_,StatePid,Reason},#dp{find_global_state_pid = StatePid} = P) ->
	?DBG("find_global_state_pid done ~p",[Reason]),
	case Reason of
		verified ->
			Ref = make_ref(),
			self() ! {ping_global_timeout,Ref},
			{noreply,P#dp{find_global_state_pid = undefined,pingref = Ref}};
		unverified ->
			?DBG("Verifying global state ~p  ~p",[state_val(bkdcore,master,P#dp.globalstate),P#dp.globalstate]),
			{Pid,_} = spawn_monitor(fun() -> verify_global_state(state_val(bkdcore,master,P#dp.globalstate)) end),
			{noreply,P#dp{find_global_state_pid = Pid}};
		nostate ->
			Nodes = lists:sort(bkdcore:nodelist()),
			Len = length(Nodes),
			case Len >= ?GLOBAL_MASTER_GROUP_SIZE of
				true ->
					{Masters,_} = lists:split(?GLOBAL_MASTER_GROUP_SIZE,Nodes);
				false ->
					Masters = Nodes
			end,
			case lists:member(bkdcore:node_name(),Masters) of
				true ->
					case Masters == [bkdcore:node_name()] of
						true ->
							self() ! {nodeup,global,bkdcore:node_name()};
						false ->
							ok
					end,
					Ref = make_ref(),
					self() ! {ping_global_timeout,Ref};
				false ->
					Ref = P#dp.pingref,
					erlang:send_after(2000,self(),start_global)
			end,
			{noreply,P#dp{find_global_state_pid = undefined,master_group = Masters,pingref = Ref}};
		Err ->
			?DBG("Unable to setup global state ~p.~nRetrying in 2s.~n",[Err]),
			erlang:send_after(2000,self(),start_global),
			{noreply,P#dp{find_global_state_pid = undefined,globalstate = [],globalversion = 0, master_group = []}}
	end;
handle_info({'DOWN',_Monitor,_,Pid,Reason},#dp{cluster_update_pid = Pid} = P) ->
	?DBG("cluster_update down  ~p",[Reason]),
	% Return to callers either ok or nomajority
	[gen_server:reply(From,Reason) || From <- P#dp.cluster_commiting_from],
	case handle_call(cluster_state_update,ok,P#dp{cluster_update_pid = undefined,
												cluster_commiting = undefined, cluster_commiting_from = []}) of
		{_,_,NP} ->
			ok;
		{noreply,NP} ->
			ok
	end,
	{noreply,NP};
handle_info({'DOWN',_Monitor,_,Pid,Reason},#dp{global_update_pid = Pid} = P) ->
	?DBG("global_update result  ~p",[Reason]),
	% Return to callers either ok or nomajority
	[gen_server:reply(From,Reason) || From <- P#dp.global_commiting_from],
	case handle_call(global_state_update,ok,P#dp{global_update_pid = undefined,
											global_commiting = undefined, global_commiting_from = []}) of
		{_,_,NP} ->
			ok;
		{noreply,NP} ->
			ok
	end,
	{noreply,NP};
handle_info({'DOWN',_Monitor,_,Pid,_Reason},#dp{exchange_state_pid = Pid} = P) ->
	{noreply,P#dp{exchange_state_pid = undefined}};
handle_info({'DOWN',_Monitor,_,Pid,_Reason},#dp{exchange_global_state_pid = Pid} = P) ->
	{noreply,P#dp{exchange_global_state_pid = undefined}};
handle_info({'DOWN',_Monitor,_,Pid,_Reason},P) ->
	case lists:keyfind(Pid,1,P#dp.voters) of
		false ->
			case lists:keyfind(Pid,2,P#dp.concluded_votings) of
				false ->
					% io:format("~p down pid ~p not found ~p~n", [?MODULE,Pid,?R2P(P)]),
					{noreply,P};
				{Node,_} ->
					{noreply,P#dp{concluded_votings = lists:keystore(Node,1,P#dp.concluded_votings,{Node,undefined})}}
			end;
		{Pid,Appname,MFA} ->
			spawn(fun() -> register_app(Appname,MFA) end),
			NV = lists:keydelete(Pid,1,P#dp.voters),
			butil:ds_add(apps,NV,?MODULE),
			{noreply,P#dp{voters = NV}}
	end;
handle_info(figureout_master,P) ->
	?DBG("figureout_master info",[]),
	case P#dp.exchange_state_pid of
		undefined ->
			{Pid,_} = spawn_monitor(fun() -> exchange_state() end),
			{noreply,P#dp{exchange_state_pid = Pid}};
		_ ->
			{noreply,P}
	end;
handle_info(figureout_global_master,P) ->
	?DBG("figureout_global_master",[]),
	case lists:member(bkdcore:node_name(),P#dp.master_group) of
		true ->
			case P#dp.exchange_global_state_pid of
				undefined ->
					{Pid,_} = spawn_monitor(fun() -> exchange_global_state(P#dp.master_group) end),
					{noreply,P#dp{exchange_global_state_pid = Pid}};
				_ ->
					{noreply,P}
			end;
		false ->
			{noreply,P}
	end;
handle_info(broadcast,P) ->
	case P#dp.clusterversion == 0 andalso nodes() /= [] of
		true ->
			self() ! figureout_master;
		_ ->
			ok
	end,
	erlang:send_after(10000,self(),broadcast),
	handle_cast(dobroadcast,P);
handle_info(start_global,P) ->
	case state_val(bkdcore,master,P#dp.globalstate) of
		undefined ->
			{Pid,_} = spawn_monitor(fun() -> find_global_state() end);
		Master ->
			{Pid,_} = spawn_monitor(fun() -> verify_global_state(Master) end)
	end,
	{noreply,P#dp{find_global_state_pid = Pid}};
handle_info({ping_global_timeout,Ref},P) when P#dp.pingref == Ref ->
	erlang:send_after(1000,self(),{ping_global_timeout,Ref}),
	handle_info(ping_global,P#dp{pingnum = P#dp.pingnum + 1});
handle_info(ping_global,P) ->
	case lists:member(bkdcore:node_name(),P#dp.master_group) of
		true ->
			case P#dp.global_do_exchange of
				true when P#dp.pingnum > 5 ->
					self() ! figureout_global_master,
					DoExchange = false;
				true ->
					DoExchange = true;
				_ ->
					DoExchange = P#dp.global_do_exchange
			end,
			butil:ds_add({lastseen,bkdcore:node_name()},os:timestamp(),?MODULE),
			[spawn(fun() -> ping_global(Nd,P#dp.globalversion) end) || Nd <- P#dp.master_group];
		false ->
			% Every second ping a different node. If a node gets disconnected for a period and a state change occurs,
			%  they might have missed it. Slowly traversing nodes with pings will cause it to eventually get the state change.
			case P#dp.global_ping_nodes of
				{} ->
					DoExchange = false;
				_ ->
					Node = element((P#dp.global_ping_pos rem tuple_size(P#dp.global_ping_nodes))+1,P#dp.global_ping_nodes),
					spawn(fun() -> 
							case bkdcore:rpc(Node,{?MODULE,ping,[bkdcore:node_name(),P#dp.globalversion]}) of
								uninitialized ->
									bkdcore:rpc(Node,{?MODULE,ping,[bkdcore:node_name(),P#dp.globalversion,P#dp.globalstate]});
								_ ->
									ok
							end
						end),
					DoExchange = false
			end
	end,
	{noreply,P#dp{global_ping_pos = P#dp.global_ping_pos+1, global_do_exchange = DoExchange}};
handle_info({stop},P) ->
	handle_info({stop,noreason},P);
handle_info({stop,Reason},P) ->
	{stop, Reason, P};
handle_info({?MODULE,cluster_state_change},P) ->
	{noreply,P};
handle_info({?MODULE,global_state_change},P) ->
	{noreply,P};
handle_info({?MODULE,cluster_connected},P) ->
	{noreply,P};
handle_info(wait_statepath,P) ->
	case bkdcore:statepath() of
		undefined ->
			erlang:send_after(1000,self(),wait_statepath),
			{noreply,P};
		_ ->
			{ok,NP} = init(P),
			{noreply,NP}
	end;
handle_info(_Msg, P) -> 
	?ERR("bkdcore_sharedstate unknown msg ~p~n",[_Msg]),
	{noreply, P}.


find_global_state() ->
	find_global_state(bkdcore:nodelist(),0).
find_global_state([Nd|T],Myver) ->
	case Nd /= bkdcore:node_name() of
		true ->
			case bkdcore:rpc(Nd,{?MODULE,get_global_state,[]}) of
				{nomajority,NV,NS} ->
					gen_server:call(?MODULE,{globalstate_confirm,bkdcore:node_name(),NV,NS}),
					exit(unverified);
				{ok,NV,NS} when NV > Myver ->
					gen_server:call(?MODULE,{globalstate_confirm,bkdcore:node_name(),NV,NS}),
					exit(unverified);
				{ok,_,_} ->
					find_global_state(T,Myver);
				undefined ->
					find_global_state(T,Myver);
				_ ->
					find_global_state(T,Myver)
			end;
		_ ->
			find_global_state(T,Myver)
	end;
find_global_state([],_) ->
	exit(nostate).


verify_global_state(undefined) ->
	exit(nomaster);
verify_global_state(Master) ->
	case Master == bkdcore:node_name() of
		true ->
			ok;
		false ->
			case bkdcore:rpc(Master,{?MODULE,get_global_state,[]}) of
				{nomajority,NV,NS} ->
					ok;
				{ok,NV,NS} ->
					ok
			end,
			case state_val(bkdcore,master,NS) of
				Master ->
					gen_server:call(?MODULE,{globalstate_confirm,bkdcore:node_name(),NV,NS});
				NMaster ->
					verify_global_state(NMaster)
			end
	end,
	exit(verified).


ping_global(Node,GlobalVersion) ->
	case Node == bkdcore:node_name() of
		true ->
			exit(samenode);
		_ ->
			ok
	end,
	LastSeen = butil:ds_val({lastseen,Node},?MODULE),
	case LastSeen of
		{_,_,_} ->
			case timer:now_diff(os:timestamp(),LastSeen) < 700000 of
				true ->
					exit(stop);
				false ->
					ok
			end;
		_ ->
			ok
	end,
	case bkdcore:rpc(Node,{?MODULE,ping,[bkdcore:node_name(),GlobalVersion]}) of
		pong ->
			Nodeup = true,
			butil:ds_add({lastseen,Node},os:timestamp(),?MODULE);
		uninitialized ->
			Nodeup = false,
			{ok,_,_,GState} = globalstate(),
			bkdcore:rpc(Node,{?MODULE,ping,[bkdcore:node_name(),GlobalVersion,GState]});
		_X ->
			Nodeup = false
	end,
	case LastSeen of
		undefined when Nodeup == false ->
			ok;
		undefined when Nodeup ->
			?MODULE ! {nodeup,global,Node};
		{_,_,_} = LastSeen when Nodeup == false ->
			?MODULE ! {nodedown,global,Node},
			butil:ds_add({lastseen,Node},undefined,?MODULE);
		{_,_,_} when Nodeup ->
			ok
	end.

ping(_FromNode,Vers,State) ->
	?DBG("Received global state from node ~p ~p",[_FromNode,State]),
	gen_server:call(?MODULE,{globalstate_confirm,bkdcore:node_name(),Vers,State}),
	% bkdcore_sharedstate:set_global_state(State),
	ok.
ping(FromNode,HisGlobalVersion) ->
	case bkdcore:is_uninitialized() of
		true ->
			uninitialized;
		false ->
			case butil:ds_val(globalversion,?MODULE) of
				HisGlobalVersion ->
					ok;
				MyVer when MyVer > HisGlobalVersion ->
					spawn(fun() -> 
							bkdcore:rpc(FromNode,{?MODULE,ping,[bkdcore:node_name(),MyVer]})
						end);
				MyVer when MyVer < HisGlobalVersion ->
					spawn(fun() -> 
							{ok,Vers,_Num,State} = bkdcore:rpc(FromNode,{?MODULE,globalstate,[]}), 
							gen_server:call(?MODULE,{globalstate_confirm,bkdcore:node_name(),Vers,State})
						end)
			end,
			ping(FromNode)
	end.
ping(FromNode) ->
	case butil:ds_val({lastseen,FromNode},?MODULE) of
		undefined ->
			?MODULE ! {nodeup,global,FromNode};
		_ ->
			ok
	end,
	butil:ds_add({lastseen,FromNode},os:timestamp(),?MODULE),
	pong.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	init(#dp{});
init(PI) ->
	case ets:info(?MODULE,size) of
		undefined ->
			ets:new(?MODULE, [named_table,public,set,{heir,whereis(bkdcore_sup),<<>>}]),
			Voters = [{self(),?MODULE,undefined}],
			butil:ds_add(subscribers,[],?MODULE),
			butil:ds_add(apps,Voters,?MODULE);
		_ ->
			% Read apps from existing ets, replace old self as voter
			Voters1 = butil:ds_val(apps,?MODULE),
			Voters = lists:keystore(?MODULE,2,Voters1,{self(),?MODULE,undefined}),
			butil:ds_add(apps,Voters,?MODULE),
			[erlang:monitor(process,VoterPid) || {VoterPid,_,_} <- Voters, self() /= VoterPid],
			% Delete nodenum for init nodes, this will create a voting for it
			[ets:delete(?MODULE,{nodenum,NodeName}) || {NodeName,init} <- ets:tab2list(?MODULE)]
	end,
	case bkdcore:statepath() of
		undefined ->
			erlang:send_after(1000,self(),wait_statepath),
			{ok,PI};
		_ ->
			erlang:send_after(10000,self(),broadcast),
			net_kernel:monitor_nodes(true),
			butil:ds_add(bkdcore:node_name(),false,?MODULE),
			erlang:send_after(5000,self(),figureout_master),
			self() ! start_global,
			case readtermfile([bkdcore:statepath(),"/stateglobal"]) of
				{GlobalV,Global} ->
					global_to_changecheck(Global),
					ok;
				_ ->
					GlobalV = 0,
					Global = []
			end,
			case readtermfile([bkdcore:statepath(),"/statecluster"]) of
				undefined ->
					ClusterV = 0,
					Cluster = [];
				{ClusterV,Cluster} ->
					ok
			end,
			ets:insert(?MODULE,[{clusterstate,Cluster},{clusterversion,ClusterV}]),
			ets:insert(?MODULE,[{globalstate,Global},{globalversion,ClusterV}]),
			P = PI#dp{clusterversion = ClusterV, clusterstate = Cluster,  clusterhash = erlang:phash2(Cluster),
								  globalversion = GlobalV, globalstate = Global,
								  master_group = state_val(bkdcore,master_group,Global),
								  global_ping_nodes = global_ping_nodes(),
								  global_ping_pos = 0,
								  globalhash = erlang:phash2(Global),
								  voters = Voters},
			?INF("Sharedstate started with ~p~n",[?R2P(P)]),
			{ok,P}
	end.

global_to_changecheck(Global) ->
	Groups = state_val(bkdcore,groups,Global),
	Nodes = state_val(bkdcore,nodes,Global),
	case Groups == undefined orelse Nodes == undefined of
		true ->
			ok;
		_ ->
			% Some cfg files might be saved to global state
			% Check names from cfgfiles env. variable. 
			case application:get_env(bkdcore,cfgfiles) of
				{ok,[_|_] = FileCfgs} ->
					[case butil:ds_val(butil:tolist(Key),FileCfgs) of
						undefined ->
							ok;
						_ ->
							ok = bkdcore_changecheck:setcfg(butil:tolist(Key),Val)
					end || {{App,Key},Val} <- Global, App /= bkdcore];
				_ ->
					ok
			end,
			bkdcore_changecheck:set_nodes_groups(Nodes,Groups)
	end.

global_ping_nodes() ->
	list_to_tuple(hashsort(lists:delete(bkdcore:node_name(),bkdcore:nodelist()))).
hashsort(L) ->
	L1 = lists:keysort(1,[{erlang:phash2([{bkdcore:node_name(),E},12394829857,255]),E} || E <- L]),
	[E || {_,E} <- L1].



nodenum() ->
	{ok,N} = application:get_env(bkdcore,randnum),
	N.

am_i_cluster_master(P) ->
	bkdcore:node_name() == state_val(bkdcore,master,P#dp.clusterstate).
am_i_global_master(P) ->
	bkdcore:node_name() == state_val(bkdcore,master,P#dp.globalstate).

exchange_state() ->
	case bkdcore:dist_name() of
		undefined ->
			ok;
		DN ->
			case node() == 'nonode@nohost' of
				true ->
					net_kernel:start([DN]);
				_ ->
					ok
			end,
			[spawn_monitor(fun() ->  
									case rpc:call(bkdcore:dist_name(Nd),?MODULE,clusterstate,[]) of
										{ok,Vers,Num,State} ->
											exit({ok,Nd,Num,Vers,State});
										Err ->
											exit(Err)
									end end) || Nd <- bkdcore:cluster_nodes()],
			gen_server:cast(?MODULE,{figureout_master, gather_states(length(bkdcore:cluster_nodes()),[])})
	end.
exchange_global_state(Nodes) ->
	Pids = [spawn_monitor(fun() ->  
									{ok,Vers,Num,State} = bkdcore:rpc(Nd,{?MODULE,globalstate,[]}), 
									exit({ok,Nd,Num,Vers,State}) end) || Nd <- Nodes, Nd /= bkdcore:node_name()],
	gen_server:cast(?MODULE,{figureout_global_master, gather_states(length(Pids)-1,[])}).

clusterstate() -> 
	{ok,butil:ds_val(clusterversion,?MODULE),nodenum(),butil:ds_val(clusterstate,?MODULE)}.
globalstate() ->
	{ok,butil:ds_val(globalversion,?MODULE),nodenum(),butil:ds_val(globalstate,?MODULE)}.

gather_states(N,L) when N =< 0 ->
	L;
gather_states(N,L) ->
	receive
		{'DOWN',_Monitor,_,_PID,{ok,Node,NodeNum,Vers,State}} ->
			gather_states(N-1,[{Node,NodeNum,Vers,State}|L]);
		{'DOWN',_,_,_,_} ->
			gather_states(N-1,L)
	end.


% State is propery list of apps, each app is a property list of values.
state_val(App,Key,State) ->
	butil:ds_val({App,Key},State).
save_state_val(App,Key,Val,State) ->
	lists:keystore({App,Key},1,State,{{App,Key},Val}).



savetermfile(Path,Term) ->
	savebinfile(Path,term_to_binary(Term,[compressed,{minor_version,1}])).
savebinfile(Path,Bin) ->
	filelib:ensure_dir(Path),
	ok = file:write_file(Path,[<<(erlang:crc32(Bin)):32/unsigned>>,Bin]).
readtermfile(Path) ->
	case readbinfile(Path) of
		undefined ->
			undefined;
		Bin ->
			binary_to_term(Bin)
	end.
readbinfile(Path) ->
	case file:read_file(Path) of
		{ok,<<Crc:32/unsigned,Body/binary>>} ->
			case erlang:crc32(Body) of
				Crc ->
					Body;
				_ ->
					undefined
			end;
		_Err ->
			undefined
	end.



