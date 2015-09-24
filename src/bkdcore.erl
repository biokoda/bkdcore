% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore).
-compile(export_all).
% -include_lib("../include/bkdcore.hrl").

reload() ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	?MODULE:onload().
onload() ->
	ok.


% [{cfg,"cfgname",[{autoload,true/false},
% 								 {mod,Modname},
% 								 {preload,{M,F,A}},
% 								 {onload,{M,F,A}},
% 								 {typeinfo,{M,F,A}}]}]
start(Opts) ->
	save_opts(Opts),
	application:start(bkdcore,permanent).
start() ->
	application:start(bkdcore,permanent).

save_opts([{cfg,Name,Opts}|T]) ->
	case application:get_env(bkdcore,cfgfiles) of
		undefined ->
			application:set_env(bkdcore,cfgfiles,[{Name,Opts}]);
		{ok,L} ->
			application:set_env(bkdcore,cfgfiles,[{Name,Opts}|L])
	end,
	save_opts(T);
save_opts([{cluster_types,L}|T]) ->
	case application:get_env(bkdcore,cluster_types) of
		undefined ->
			application:set_env(bkdcore,cluster_types,L);
		{ok,L1} ->
			application:set_env(bkdcore,cluster_types,L++L1)
	end,
	save_opts(T);
save_opts([_|T]) ->
	save_opts(T);
save_opts([]) ->
	ok.


stop() ->
	application:stop(bkdcore).

statepath() ->
	case application:get_env(bkdcore,statepath) of
		{ok,{Mod,Func}} ->
			apply(Mod,Func,[]);
		{ok,V} ->
			V++"/"++butil:tolist(node_name());
		_ ->
			undefined
	end.

rpcport() ->
	case application:get_env(bkdcore,rpcport) of
		{ok,V} ->
			V;
		_ ->
			undefined
	end.
webport() ->
	case application:get_env(bkdcore,webport) of
		{ok,V} ->
			V;
		_ ->
			undefined
	end.

% In microseconds
uptime() ->
	{ok,T} = application:get_env(bkdcore,starttime),
	timer:now_diff(os:timestamp(),T).

dist_name() ->
	dist_name(node_name()).
dist_name(Name) when is_binary(Name) ->
	butil:ds_val({distname,butil:tobin(Name)},bkdcore_nodes);
dist_name(Name) when is_atom(Name) ->
	Name.

name_from_dist_name(DN) when is_atom(DN) ->
	butil:ds_val({realname,DN},bkdcore_nodes);
name_from_dist_name(DN) when is_binary(DN) ->
	DN.

node_name() ->
	case application:get_env(bkdcore,name) of
		{ok,N} ->
			N;
		_ ->
			undefined
	end.

public_address() ->
	public_address(node_name()).
public_address(Name) ->
	butil:ds_val({pubip,butil:tobin(Name)},bkdcore_nodes).

node_address() ->
	node_address(node_name()).
node_address(N) ->
	butil:ds_val({address,butil:tobin(N)},bkdcore_nodes).
	
node_membership() ->
	node_membership(node_name()).
is_member(Grp) ->
	lists:member(node_name(),nodelist(Grp)).
is_member(Node,Grp) ->
	lists:member(butil:tobin(Node),nodelist(Grp)).
node_membership(Node1) ->
	Node = butil:tobin(Node1),
	[Group || {{nodes,Group},Nodes} <- ets:select(bkdcore_groups,[{{'$1','$2'},[{'==',{element,1,'$1'},nodes}], ['$_']}]),
						lists:member(Node,Nodes)].

group_param(Grp) ->
	butil:ds_val({param,butil:toatom(Grp)},bkdcore_groups).
group_type(Grp) ->
	butil:ds_val({type,butil:toatom(Grp)},bkdcore_groups).
group_index(Grp) ->
	butil:ds_val({index,butil:tobin(Grp)},bkdcore_groups).
indexed_groups() ->
	butil:ds_val(indexed_groups,bkdcore_groups,[]).

all_groups() ->
	[Group || {{nodes,Group},_} <- ets:select(bkdcore_groups,[{{'$1','$2'},[{'==',{element,1,'$1'},nodes}], ['$_']}])].

groups_of_type(Type) ->
	butil:ds_val(butil:toatom(Type),bkdcore_groups).
group_types() ->
	[Type || {Type,_} <- ets:select(bkdcore_groups,[{{'$1','$2'},[{is_atom,'$1'}], ['$_']}])].
cluster_group() ->
	cluster_group(node_name()).
cluster_group(Node) ->
	butil:ds_val({cluster_group,butil:tobin(Node)},bkdcore_groups).
% All nodes in cluster besides current
cluster_nodes() ->
	[Nd || Nd <- nodelist(cluster_group()), Nd /= node_name()].
all_cluster_nodes() ->
	nodelist(cluster_group()).
cluster_nodes_connected() ->
	CL = cluster_nodes(),
	[Nd || Nd <- nodes(), lists:member(name_from_dist_name(Nd),CL)].
% List of nodes that are in cluster groups.
nodelist_allclusters() ->
	Grps = groups_of_type(cluster),
	lists:flatten([nodelist(Grp) || Grp <- Grps]).

arpc(Node,Func) ->
	spawn(fun() -> rpc(Node,Func) end).
rpc(Node,Func) ->
	bkdcore_rpc:call(butil:tobin(Node),Func).
rpc(Node,Mod,F,A) ->
	bkdcore_rpc:call(butil:tobin(Node),{Mod,F,A}).

rpccookie() ->
	rpccookie(bkdcore:node_name()).
rpccookie(Node) ->
	case application:get_env(bkdcore,{rpc,butil:tobin(Node)}) of
		undefined ->
			C = butil:hash([Node,butil:tobin(erlang:get_cookie())],<<1,34,42,54,243,4,35,3,123,5,234,5>>,20000,40),
			application:set_env(bkdcore,{rpc,butil:tobin(Node)},C),
			C;
		{ok,C} ->
			C
	end.

arpc_group(V,Grp) ->
	[arpc(N,V) || N <- nodelist(Grp)].

is_uninitialized() ->
	Size = ets:info(bkdcore_nodes,size),
	Size == 0 orelse Size == undefined.


nodelist() ->
	[Name || {{realname,_},Name} <- ets:select(bkdcore_nodes,[{{'$1','$2'},[{'==',{element,1,'$1'},realname}], ['$_']}])].
		
nodelist(G) ->
	case butil:ds_val({nodes,butil:toatom(G)},bkdcore_groups) of
		undefined ->
			[];
		L ->
			L
	end.

% node_online(Nd1) ->
	% Nd = butil:tobin(Nd1),
	% case node_name() == Nd of
	% 	true ->
	% 		true;
	% 	_ ->
			% bkdcore_sharedstate:node_online(butil:tobin(Nd1)).
	% end.

% active_nodelist(Grp) ->
% 	lists:filter(fun node_online/1,nodelist(Grp)).
% inactive_nodelist(Grp) ->
% 	lists:filter(fun(Nd) -> case node_online(Nd) of true -> false; false -> true end end,nodelist(Grp)).

% Mod - atom name of module
% T can be:
% 	- [{FuncNameAtom,FuncValueTerm},...] 
% 	Translates to:
% 		Mod:FundNameAtom() -> 
%			 FuncValue

%   - [{FuncNameAtom,FuncParameter,FuncValue}] 
% 		Translates to:
% 			Mod:FuncNameAtom(FuncParameter) -> 
%					FuncValue

%   - [{FuncNameAtom,multihead,[{FuncParam1,FuncVal1},{FuncParam2,FuncVal2},{any,undefined}]}] 
% 		Translates to:
%				FuncNameAtom(FuncParam1) ->
%					FuncVal1;
%				FuncNameAtom(FuncParam2) ->
%					FuncVal2;
% 			FuncNameAtom(_) ->
% 				undefined.
mkmodule(Mod, T) ->
	mkmodule(Mod, T, mod).
mkmodule(Mod, T, Ret) ->
	Modatr = erl_syntax:attribute(
	       erl_syntax:atom(module),
	       [erl_syntax:atom(Mod)]),
	Export = erl_syntax:attribute(
		       erl_syntax:atom(export),
		       [erl_syntax:list([erl_syntax:arity_qualifier(erl_syntax:atom(Name),erl_syntax:integer(0)) || {Name,_} <- T]++
										       	[erl_syntax:arity_qualifier(erl_syntax:atom(Name),erl_syntax:integer(1)) || {Name,_,_} <- T])]),
	Funs = [erl_syntax:function(erl_syntax:atom(Name),[erl_syntax:clause([], none, [erl_syntax:abstract(Vals)])]) || {Name,Vals} <- T],
	Funs1 = [erl_syntax:function(erl_syntax:atom(Name),[erl_syntax:clause([erl_syntax:abstract(Param)], none, [erl_syntax:abstract(Vals)])]) 
						|| {Name,Param,Vals} <- T, Param /= multihead],
	Funs2 = [begin 
				erl_syntax:function(erl_syntax:atom(Name),[
								begin
									case Param of
										any ->
											erl_syntax:clause([erl_syntax:underscore()], none, [erl_syntax:abstract(Vals)]);
										_ ->
											erl_syntax:clause([erl_syntax:abstract(Param)], none, [erl_syntax:abstract(Vals)])
									end
								end
								|| {Param,Vals} <- ParamVals]) 
					 end
						|| {Name,multihead, ParamVals} <- T],
	L = [erl_syntax:revert(X) || X <- [Modatr,Export|Funs]++Funs1++Funs2],
	{ok, Mod, Bin} = compile:forms(L, [verbose, report_errors]),
	code:purge(Mod),
	MR = code:load_binary(Mod, atom_to_list(Mod) ++ ".erl", Bin),
	case Ret of
		mod ->
			MR;
		bin ->
			{ok, Mod, Bin}
	end.


nodebyip(IP) ->
	nodebyip(nodelist(),IP).
nodebyip(L,IP) when is_list(IP) == false ->
	nodebyip(L,butil:int_to_ip(IP));
nodebyip([_|T],IP) ->
	nodebyip(T,IP);
nodebyip([],_) ->
	undefined.


% If etc/mime.types is present.
mime_from_ext("."++Ext) ->
	mime_from_ext(butil:tobin(Ext));
mime_from_ext(<<".",Ext/binary>>) ->
	mime_from_ext(Ext);
mime_from_ext(E1) ->
	Ext = butil:tobin(string:to_lower(butil:tolist(E1))),
	case catch butil:ds_val(Ext,bkdcore_mimetypes) of
		{'EXIT',_} ->
			<<"application/octet-stream">>;
		undefined ->
			<<"application/octet-stream">>;
		X ->
			X
	end.


