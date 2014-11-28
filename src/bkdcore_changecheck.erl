% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_changecheck).
-behaviour(gen_server).
-export([start/0, stop/0, reload/0,print_info/0,deser_prop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([make/0,startup_node/0,folders/0,is_valid_path/1,read_node/1,set_nodes_groups/2,
		 insert_nodes_to_ets/1,insert_groups_to_ets/1,setcfg/1,setcfg/2,
		 parse_yaml_groups/1]).
-include_lib("kernel/include/file.hrl").
-compile([{parse_transform, lager_transform}]).

print_info() ->
	gen_server:cast(?MODULE,{print_info}).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).
	
reload() ->
	gen_server:call(?MODULE, {reload_module}).



-record(cdat, {dict,paths = [], dist_dir = [],fswatcher}).
-define(R2P(Record), butil:rec2prop(Record, record_info(fields, cdat))).
-define(P2R(Prop), butil:prop2rec(Prop, cdat, #cdat{}, record_info(fields, cdat))).
	
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call({reload_module}, _, P) ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	{reply, ok, ?MODULE:deser_prop(?R2P(P))};
handle_call(_, _, P) ->
	{reply, ok, P}.

deser_prop(P) ->
	?P2R(P).
	 
handle_cast({save_to_dict,{Path,Time}},P) ->
	{noreply,P#cdat{dict = butil:ds_add(Path,Time,P#cdat.dict)}};
handle_cast(_X, P) ->
	lager:info("~p~n~p ~p", [P,self(), butil:ds_tolist(P#cdat.dict)]),
	{noreply, P}.

explodepath("apps") ->
	explodepath(butil:project_rootpath()++"/apps/");
explodepath("deps") ->
	explodepath(butil:project_rootpath()++"/deps/");
explodepath(X) ->
	case lists:reverse(X) of
		[$/|_] ->
			X;
		RX ->
			lists:reverse([$/|RX])
	end.

is_valid_path(P) ->
	case [true || X <- folders(), is_list(X), lists:prefix(explodepath(X),explodepath(P))] of
		[] ->
			false;
		_ ->
			true
	end.

handle_info({_, {data, Pathbin}},P) ->
	Paths = [butil:tolist(X) || X <- binary:split(Pathbin,<<"\r\n">>,[global,trim])],
	{ok,Dict} = traverse_paths(P#cdat.dict,[butil:tolist(X) || X <- Paths, is_valid_path(X)], change_check),
	{noreply,P#cdat{dict = Dict}};
handle_info({check_changes}, #cdat{fswatcher = undefined} = P) ->
	case butil:ds_size(P#cdat.dict) of
		0 ->
			case ets:info(bkdcore_mimetypes,size) of
				undefined ->
					ets:new(bkdcore_mimetypes, [named_table,public,set,{read_concurrency,true},{heir,whereis(bkdcore_sup),<<>>}]),
					ets:new(bkdcore_groups, [named_table,public,set,{read_concurrency,true},{heir,whereis(bkdcore_sup),<<>>}]),
					ets:new(bkdcore_nodes, [named_table,public,set,{read_concurrency,true},{heir,whereis(bkdcore_sup),<<>>}]);
				_ ->
					ok
			end,
			Op = first,
			case application:get_env(bkdcore,autoload_files)  of
				{ok,false} ->
					ok;
				_ ->
					erlang:send_after(5000,self(),{check_changes})
			end;
		_ ->
			Op = change_check,
			erlang:send_after(5000,self(),{check_changes})
	end,
	case catch traverse_paths(P#cdat.dict,P#cdat.paths, Op) of
		{ok,Dict} ->
			ok;
		Err ->
			Dict = P#cdat.dict,
			lager:info("Traversing folders crashed ~p", [Err])
	end,
	{noreply, P#cdat{dict = Dict}};
handle_info(_X, State) -> 
	{noreply, State}.

terminate(_, _) ->
	ok.
code_change(_, State, _) ->
	{ok, State}.
init(_) ->
	filelib:ensure_dir(butil:project_rootpath() ++ "/priv/"),
	% If autoload_files not set, check if bkdcore is in "lib" directory. 
	% If it is set autoload_files to false. "lib" directory is only in deployed mode.
	case application:get_env(bkdcore,autoload_files) of
		undefined ->
			{_,_,Path} = code:get_object_code(?MODULE),
			PathParts = filename:split(Path),
			case lists:reverse(PathParts) of
				[_,"ebin",_,"lib"|_] ->
					Autoload = false,
					application:set_env(bkdcore,autoload_files,false);
				_ ->
					Autoload = true
			end;
		{ok,Autoload} ->
			io:format("HAVE AUTOLOAD ~p",[Autoload]),
			ok
	end,
	case butil:get_os() of
		osx ->
			case Autoload andalso have_fswatcher() of
				false ->
					ok;
				true ->
					lager:info("Compiling fswatcher~n"), 
					os:cmd("gcc -lobjc -framework CoreFoundation -framework CoreServices " ++butil:bkdcore_path()++"/c_src/fswatcher.m -o "++
						  butil:project_rootpath()++"/priv/fswatcher")
			end;
		_ ->
			ok
	end,
	RootPath = butil:project_rootpath(),
	Paths = folders(),
	{_,P} = handle_info({check_changes},#cdat{dict = butil:ds_new(dict), paths = Paths, dist_dir = RootPath ++ "/priv/code_dist"}),

	case have_fswatcher() andalso butil:get_os() == osx of
		true ->
			Watcher = open_port({spawn,butil:project_rootpath()++"/priv/fswatcher "++butil:project_rootpath()},[binary,stream,use_stdio]);
		_ ->
			Watcher = undefined
	end,
	{ok, P#cdat{fswatcher = Watcher}}.

have_fswatcher() ->
	filelib:is_regular(butil:project_rootpath() ++ "/priv/fswatcher").


folders() ->
	RootPath = butil:project_rootpath(),
	case application:get_env(bkdcore,etc) of
		undefined ->
			Etc = [RootPath ++ "/etc"];
		none ->
			Etc = "";
		{ok,[$\~|Etcrem]} ->
			Etc = [butil:project_rootpath()++Etcrem];
		{ok,Etc1} ->
			Etc = [Etc1]
	end,
	Srces = [RootPath ++ "/" ++ Pth || Pth <- filelib:wildcard("src_*",RootPath)],
	Etc ++ ["apps","deps",RootPath++"/dtl",RootPath++"/src", RootPath ++ "/ebin"|Srces].
make() ->
	traverse_paths(butil:ds_new(dict),folders(),make).
startup_node() ->
	% lager:info("Startup node ~n"),
	traverse_paths(butil:ds_new(dict),folders(),startupnode).

traverse_paths(Dict,[ApDep|T], Op) when ApDep == "deps"; ApDep == "apps" ->
	case Op of
		% touchcfg ->
		% 	traverse_paths(T,Op);
		make when ApDep == "deps" ->
			traverse_paths(Dict,T,Op);
		_ ->
			case file:list_dir([butil:project_rootpath(),"/",ApDep]) of
				{ok,L1} ->
					L = [begin
							case ApDep of
								"deps" ->
									case application:get_env(autocompile) of
										{ok,AppList}  ->
											case lists:member(X,AppList) of
												true ->
													[lists:concat([butil:project_rootpath(),"/",ApDep,"/",X,"/src"]),
													lists:concat([butil:project_rootpath(),"/",ApDep,"/",X,"/ebin"])];
												false ->
													[lists:concat([butil:project_rootpath(),"/",ApDep,"/",X,"/ebin"])]
											end;
										_ ->
											[lists:concat([butil:project_rootpath(),"/",ApDep,"/",X,"/ebin"])]
									end;
								_ ->
									[lists:concat([butil:project_rootpath(),"/",ApDep,"/",X,"/ebin"]),
								 		lists:concat([butil:project_rootpath(),"/",ApDep,"/",X,"/src"]),
							 		lists:concat([butil:project_rootpath(),"/",ApDep,"/",X,"/dtl"])]
							 end
						end || X <- L1, hd(X) /= $.],
					{ok,NDict} = traverse_paths(Dict,L,Op);
				_ ->
					NDict = Dict
			end,
			traverse_paths(NDict,T,Op)
	end;
traverse_paths(Dict,[[[_|_]|_] = P|T], Op) ->
	{ok,NDict} = traverse_paths(Dict,P,Op),
	traverse_paths(NDict,T,Op);
traverse_paths(Dict,[P|T], Op) ->
	case file:list_dir(P) of
		{ok, Files} when Op == change_check; Op == make; Op == startupnode; Op == first ->
			{ok,NDict} = traverse_files(Dict,P,Files,Op);
		% {ok, Files} when Op == touchcfg ->
		% 	touchcfg(P, Files);
		_X ->
			% lager:info("Unable to read ~p, error ~p~n", [P,_X]),
			NDict = Dict
	end,
	traverse_paths(NDict,T, Op);
traverse_paths(Dict,[], _Op) ->
	{ok,Dict}.
	
traverse_files(Dict,Path, ["." ++ _|T],Op) ->
	traverse_files(Dict,Path, T,Op);
traverse_files(Dict,Path, [H|T],Op) ->
	PrevTime = butil:ds_val(H,Dict),
	case file:read_file_info([Path, "/", H]) of
		{ok, I} ->
			case erlang:localtime() of
				LTime when LTime == I#file_info.mtime ->
					[FType|_] = lists:reverse(string:tokens(H, ".")),
					case lists:member(FType,relevant_types()) of
						false ->
							traverse_files(Dict,Path,T,Op);
						true ->
							timer:sleep(1000),
							traverse_files(Dict,Path,[H|T],Op)
					end;
				_ ->
					NDict = butil:ds_add(H,I#file_info.mtime,Dict),
					case true of
						_ when PrevTime == undefined ->
							[FType|_] = lists:reverse(string:tokens(H, ".")),
							case FType of
								"beam" ->
									case lists:member(Path,code:get_path()) of
										false ->
											case Path /= butil:project_rootpath() ++ "/ebin" of
												true ->
													lager:info("Adding path ~p~n", [Path]),
													code:add_path(Path);
												false ->
													ok
											end;
										true ->
											ok
									end;
								_ ->
									ok
							end,
							reload(Path,H,Op);
						_ ->
							case I#file_info.mtime of
								PrevTime ->
									true;
								_Diff ->
									reload(Path, H,Op)
							end
					end,
					traverse_files(NDict,Path, T,Op)
			end;
		_X ->
			traverse_files(Dict,Path, T,Op)
	end;
traverse_files(Dict,_, [],_) ->
	{ok,Dict}.

relevant_types() ->
	["beam","dtl","erl","hrl","json","nodes","groups","mime","cfg","config"].
	
reload(Path,Name,Op) ->
	% lager:info("~p~n", [Name]),
	FileParts = string:tokens(Name, "."),
	case catch application:get_env(bkdcore,docompile) of
		{ok,Docompile} when Docompile == true; Docompile == false ->
			ok;
		_ ->
			Docompile = true
	end,
	case lists:reverse(FileParts) of
		["beam"|_] when Op /= make, Op /= first, Op /= startupnode ->
				spawn(fun() ->
					lager:info("Changed beam ~p~n", [Path ++ "/" ++ Name]),
					[Root|_] = FileParts,
					Module = list_to_atom(Root),
					% Only load beam if not loaded yet
					case code:is_loaded(Module) of
						false ->
							true;
						_ ->
							case proplists:get_value(reload,Module:module_info(exports)) of
								0 ->
									{_,BinBefore,_} = code:get_object_code(Module),
									Module:reload(),
									{_,BinAfter,_} = code:get_object_code(Module),
									case ok of
										_ when BinBefore == BinAfter ->
											code:purge(Module),
											code:load_file(Module);
										_ ->
											ok
									end;
								_ ->
									code:purge(Module),
									code:load_file(Module)
							end
					end
			end);
		["dtl"|[Root]] when Docompile, Op /= startupnode ->
			case Op of
				first ->
					ok;
				_ ->					
					lager:info("DTL changed ~p~n", [Path ++ "/" ++ Root]),
					EbinPath = filename:join(lists:reverse(tl(lists:reverse(filename:split(Path)))))++"/ebin/",
					filelib:ensure_dir(EbinPath),
					case erlydtl:compile(Path ++ "/" ++ Name, list_to_atom(Root),
									[{out_dir,EbinPath}]) of
						ok ->
							case Op of
								make ->
									ok;
								_ ->
									growl(io_lib:format("Compiled ~p~n", [Path ++ "/" ++ Name]),"success")
							end;
						{ok,_} ->
							ok;
						X ->
							lager:error("Unable to compile dtl ~p~n", [X]),
							case Op of
								make ->
									exit(compile_error);
								_ ->
									growl(io_lib:format("Compile error ~p~n~p~n", [Path ++ "/" ++ Name,X]),"error")

							end
					end,
					case string:tokens(Root,"_") of
						[Tag,"base"] ->
							[reload(Path,F,Op)|| F <- filelib:wildcard(Tag ++ "_*.dtl",Path), F /= Name];
						_X ->
							ok
					end
			end;
		["erl"|[Root]] when (Name /= "bkdcore_changecheck.erl" orelse Op == make) andalso Docompile 
												andalso Op /= first andalso Op /= startupnode -> 
			Rewrite = fun(Bin) ->
								EbinPath = [filename:join(lists:reverse(tl(lists:reverse(filename:split(Path))))),"/ebin/",Root,".beam"],
								filelib:ensure_dir(EbinPath),
								file:write_file(EbinPath,Bin),
								{ok, I} = file:read_file_info(EbinPath),
								gen_server:cast(?MODULE,{save_to_dict,{Root ++ ".beam",I#file_info.mtime}})
					  end,
			lager:info("Recompiling erl ~p~n", [Name]),
			case application:get_env(bkdcore,compileopts) of
				{ok,CompileOpts} ->
					ok;
				_ ->
					CompileOpts = []
			end,
			case filelib:is_file([butil:project_rootpath(),"/ebin/pmod_pt.beam"]) of
				true ->
					Addp = [{parse_transform,pmod_pt}];
				false ->
					Addp = []
			end,
			case compile:file(Path ++ "/" ++ Name, CompileOpts ++ [binary,return_errors,return_warnings|Addp]) of
				{ok, Mod,Bin} ->
					Rewrite(Bin),
					code:load_binary(Mod, Name, Bin),
					catch Mod:reload();
				{ok, Mod,Bin,W} ->
					Rewrite(Bin),
					case W of
						[] ->
							case Op of
								make ->
									ok;
								_ ->
									growl(io_lib:format("Compiled ~p~n", [Path ++ "/" ++ Name]),"success")
							end,
							lager:info("Compiled ~p~n", [Path ++ "/" ++ Name]);
						_ ->
							case Op of
								make ->
									ok;
								_ ->
									growl(io_lib:format("Compiled ~p~n", [Path ++ "/" ++ Name]),"success")
							end,
							lager:info("Compile warnings ~p ~p~n", [Path ++ "/" ++ Name, W])
					end,
					code:load_binary(Mod, Name, Bin),
					catch Mod:reload();
				X ->
					lager:error("Unable to compile ~p ~p~n", [Path ++ "/" ++ Name, X]),
					case Op of
						make ->
							% exit(compile_error);
							ok;
						_ ->
							growl(io_lib:format("Compile error ~p~n~p~n", [Path ++ "/" ++ Name,X]),"error")
					end
			end;
		["config","rebar"] ->
			ok;
		["types","mime"] when Op /= make,Op /= startupnode ->
			case ets:info(bkdcore_mimetypes) of
				undefined ->
					ok;
				_ ->
					case catch parsemime(Path++"/"++Name) of
						{ok,L} ->
							ets:insert(bkdcore_mimetypes,L);
						_Ex ->
							lager:info("Parsing mime.types ~p failed ~p~n", [Path++"/"++Name,_Ex])
					end
			end;
		["hrl",Root] when Op /= first, Op /= make, Op /= startupnode ->
			lager:info("hrl changed, reloading name-matching files ~n"),
			timer:sleep(1000),
			[reload(Path,F,Op) || F <- filelib:wildcard(Root ++ "*.erl", Path)];
		[Ext,Root]  when (Root == "allnodes" orelse Root == "nodes") andalso Op /= startupnode andalso 
					 Op /= make andalso (Ext == "cfg" orelse Ext == "config" orelse Ext == "yaml") ->
			lager:info("Nodes file ~p~n", [Op]),
			insert_nodes_to_ets(Path ++ "/" ++ Name);
		[Ext,Root] when (Root == "allgroups" orelse Root == "groups") andalso Op /= startupnode andalso 
						Op /= make andalso (Ext == "cfg" orelse Ext == "config" orelse Ext == "yaml") ->
			lager:info("Groups file ~p~n",[Op]),
			insert_groups_to_ets(Path ++ "/" ++ Name);
		[Ext,Root] when Op /= make andalso (Ext == "cfg" orelse Ext == "config" orelse "yaml") andalso 
											Root /= "allgroups" andalso Root /= "allnodes" ->
			lager:info("Cfg changed ~p~n", [Path ++ "/" ++ Name]),
			setcfg(Path++"/"++Name);
		["json",Root] when Op /= make ->
			lager:info("Json changed ~p~n", [Path ++ "/" ++ Name]),
			{ok,Bin} = file:read_file(Path ++ "/" ++ Name),
			case catch bjson:decode(Bin) of
				[_|_] = Obj ->
					setcfg(Root++".json",Obj);
				_Err ->
					lager:info("Invalid json file. ~p~n",[_Err])
			end;
		_X ->
			% lager:info("nomatch ~p",[{Op,_X}]),
			ok
	end.

setcfg(Name) ->
	case filename:extension(Name) of
		".yaml" ->
			case catch yamerl_constr:file(Name) of
				[Cfg] ->
					setcfg(Name,Cfg);
				_Err ->
					lager:info("Cfg failed to consult ~p~n", [_Err]),
					false
			end;
		_ ->
			case catch file:consult(Name) of
				{ok, [[_|_] = Cfg]} ->
					setcfg(Name,Cfg);
				_Err ->
					lager:info("Cfg failed to consult ~p~n", [_Err]),
					false
			end
	end.
setcfg(Name,Obj) ->
	F = fun() ->
				case application:get_env(bkdcore,cfgfiles) of
					{ok,[_|_] = FileCfgs} ->
						PresetCfg = butil:ds_val(filename:basename(Name),FileCfgs,[]);
					_ ->
						PresetCfg = []
				end,
				case application:get_env(bkdcore,ignorecfg) of
					{ok,[_|_] = Ignores} ->
						case lists:member(Name,Ignores) of
							true ->
								exit(ignore);
							false ->
								ok
						end;
					_ ->
						ok
				end,
				% lager:info("Options for cfgfile ~p ~p~n",[Name,PresetCfg]),
				[Autoload,Module1,TypeCallStr,OnLoad,Preload] = 
						butil:ds_vals([autoload,mod,typeinfo,onload,preload],Obj,[true,"","","",""]),
					case Autoload of
						false ->
							exit(ok);
						_ ->
							ok
					end,
					case Module1 of
						"" ->
							case butil:ds_val(mod,PresetCfg) of
								undefined ->
									Module = Name++"_conf";
								Module ->
									ok
							end;
						Module ->
							ok
					end,
					case string:tokens(butil:tolist(TypeCallStr),":") of
						[A,B] ->
							case catch apply(butil:toatom(A),butil:toatom(B),[]) of
								TypeObj when is_list(TypeObj) ->
									ok;
								_X ->
									lager:info("Failed to call ~p:~p/0 for type info~n", [butil:toatom(A),butil:toatom(B)]),
									TypeObj = []
							end;
						_ ->
							case butil:ds_val(typeinfo,PresetCfg) of
								undefined ->
									TypeObj = [];
								{Mt,Ft,At} ->
									case catch apply(Mt,Ft,At) of
										TypeObj when is_list(TypeObj) ->
											ok;
										_X ->
											lager:info("Typeinfo call ~p returned invalid val ~p~n", [{Mt,Ft,At},_X]),
											TypeObj = []
									end
							end
					end,
					case Preload of
						"" ->
							case butil:ds_val(preload,PresetCfg) of
								undefined ->
									bkdcore:mkmodule(butil:toatom(Module),parsecfgobj(Obj,TypeObj));
								{Mpr,Fpr,Apr} ->
									bkdcore:mkmodule(butil:toatom(Module),apply(Mpr,Fpr,[Obj|Apr]))
							end;
						_ ->
							[Mod,Fun] = string:tokens(Preload,":"),
							bkdcore:mkmodule(butil:toatom(Module),apply(butil:toatom(Mod),butil:toatom(Fun),[Obj]))
					end,
					case string:tokens(butil:tolist(OnLoad),":") of
						[OA,OB] ->
							spawn(butil:toatom(OA),butil:toatom(OB),[]),
							ok;
						_ ->
							case butil:ds_val(onload,PresetCfg) of
								undefined ->
									ok;
								{Mol,Fol,Aol} ->
									spawn(Mol,Fol,Aol)
							end
					end,
					ok
			end,	
	case catch F() of
		ok ->
			ok;
		_Err ->
			_Err
	end.

parsecfgobj(Obj,TypeObj) ->
	[begin
					KA = butil:toatom(K),
					case butil:ds_val(KA,TypeObj) of
						undefined ->
							case V of
								"\~"++RemV ->
									{KA,butil:project_rootpath()++RemV};
								<<"\~",RemV/binary>> ->
									{KA,<<(butil:tobin(butil:project_rootpath()))/binary,RemV/binary>>};
								_ ->
									{KA,V}
							end;
						F when is_function(F) ->
							{KA,F(V)};
						Type ->
							{KA,butil:totype(Type,V)}
					end
				end || {K,V} <- Obj, K /= mod, K /= autoload, K /= onload, K /= typeinfo, K /= preload].

read_node(Nd) ->
	case Nd of
		[X|_] when is_integer(X) ->
			case string:tokens(butil:tolist(Nd),"@") of
				[Namestr] ->
					{butil:tobin(Nd),"127.0.0.1",4380,"127.0.0.1",butil:toatom(Namestr++"@127.0.0.1")};
				[Namestr,Addr] ->
					{Address,Port} = parseaddress(Addr,4380),
					{butil:tobin(Namestr),Address,Port,Address,butil:toatom(Namestr++"@"++Address)}
			end;
		_ ->
			[Name1,Addr1,Pub1,Port1] = butil:ds_vals([name,address,pubip,rpcport],Nd),
			case string:tokens(butil:tolist(Name1),"@") of
				[Namestr] ->
					Name = butil:tobin(Name1),
					case Addr1 of
						undefined ->
							{Address,Port} = parseaddress("127.0.0.1",Port1);
						_ ->
							{Address,Port} = parseaddress(Addr1,Port1)
					end,
					Dist = butil:toatom(Namestr++"@"++Address);
				[Name,Address1] ->
					{Address,Port} = parseaddress(Address1,Port1),
					Dist = butil:toatom(Name1)
			end,
			case Pub1 of
				undefined ->
					Pub = Address;
				_ ->
					Pub = butil:tolist(Pub1)
			end,
			{butil:tobin(Name),Address,Port,Pub,Dist}
	end.

parseaddress(Addr1,Port1) ->
	case string:tokens(butil:tolist(Addr1),":") of
		[Address] ->
			case Port1 of
				undefined ->
					Port = 4380;
				Port ->
					ok
			end;
		[Address,Ps] ->
			Port = butil:toint(Ps)
	end,
	{Address,Port}.

% wait_for_setup(0) ->
% 	exit(timeout);
% wait_for_setup(N) ->
% 	case bkdcore_sharedstate:is_ok() of
% 		true ->
% 			ok;
% 		false ->
% 			timer:sleep(1000),
% 			wait_for_setup(N-1)
% 	end.

% startapps() ->
% 	case application:get_env(bkdcore,startapps) of
% 		{ok,L} ->
% 			[spawn(fun() -> wait_for_setup(10), application:start(App,permanent) end) || App <- L];
% 		_X ->
% 			ok
% 	end.

set_nodes_groups(N,G) ->
	lager:info("Setting nodes groups ~p",[{N,G}]),
	DoStart = ets:info(bkdcore_nodes,size) == 0,
	try ok = insert_nodes_to_ets(N),
		  ok = insert_groups_to_ets(G) of
		ok ->
			case DoStart of
				true ->
					% startapps(),
					ok;
				false ->
					ok
			end
		catch
			_:Err ->
				lager:info("Set nodes groups ~p~n",[Err]),
				ok
		end.

insert_nodes_to_ets([Char|_] = Path) when is_integer(Char) ->
	case filename:extension(Path) of
		".yaml" ->
			case catch yamerl_constr:file(Path) of
				[[A1,A2]] ->
					case A1 of
						{"nodes",Nodes} ->
							{"groups",Groups} = A2,
							{Nodes,Groups};
						{"groups",Groups} ->
							{"nodes",Nodes} = A2,
							{Nodes,Groups}
					end,
					insert_nodes_to_ets(Nodes),
					insert_groups_to_ets(parse_yaml_groups(Groups));
				[Nodes] ->
					insert_nodes_to_ets(Nodes);	
				Err ->
					lager:info("Unable to load nodes ~p ~p",[Err,Path]),
					false
			end;
		_ ->
			case file:consult(Path) of
				{ok,[Nodes]} ->
					insert_nodes_to_ets(Nodes);
				Err ->
					lager:info("Unable to load nodes ~p ~p~n", [Err,Path]),
					false
			end
	end;
insert_nodes_to_ets([_|_] = Nodes) ->
	Nodesbin = term_to_binary(Nodes),
	case butil:ds_val(nodesbin,bkdcore_nodes) of
		Nodesbin ->
			ok;
		_ ->
			Now = butil:sec(),
			ToStore = [begin
							{Name,AddrReal,Port,Pub,Dist} = read_node(Nd),
							[{{lastchange,Name},Now},{{address,Name},{AddrReal,Port}},
							 {{distname,Name},Dist},{{pubip,Name},Pub},{{realname,Dist},Name}]
						end || Nd <- Nodes],
			[butil:ds_add(NodeInfo,bkdcore_nodes) || NodeInfo <- ToStore],

			butil:ds_add(nodesbin,Nodesbin,bkdcore_nodes),

			% For any node not updated, delete from ets.
			[begin
				butil:ds_rem({lastchange,Name},bkdcore_nodes),
				butil:ds_rem({address,Name},bkdcore_nodes),
				DN = butil:ds_val({distname,Name},bkdcore_nodes),
				butil:ds_rem({realname,DN},bkdcore_nodes),
				butil:ds_rem({distname,Name},bkdcore_nodes),
				butil:ds_rem({pubip,Name},bkdcore_nodes)
			end || {{lastchange,Name},LC} <- ets:tab2list(bkdcore_nodes), LC /= Now],
			Todelete = [Name || {{lastchange,Name},LC} <- ets:tab2list(bkdcore_nodes), LC /= Now],
			[begin
				% Remove all {Key,NodeName} from ets
				[butil:ds_rem({K,Name1},bkdcore_nodes) || {{K,Name1},_} <- ets:tab2list(bkdcore_nodes),Name1 == Name]
			 end || Name <- Todelete],
			 ok
		end;
insert_nodes_to_ets(_) ->
	false.

parse_yaml_groups(Groups) ->
	[{butil:toatom(butil:ds_val(name,G)),
	   butil:ds_val(nodes,G),
	   butil:toatom(butil:ds_val(type,G)),
	   butil:ds_val(properties,G,[])} || G <- Groups].

insert_groups_to_ets([Char|_] = Path) when is_integer(Char) ->
	case filename:extension(Path) of
		".yaml" ->
			case catch yamerl_constr:file(Path) of
				[Groups] ->
					insert_groups_to_ets(parse_yaml_groups(Groups));
				Err ->
					lager:info("Unable to load nodes ~p ~p~n",[Err,Path]),
					false
			end;
		_ ->
			case file:consult(Path) of
				{ok,[Groups]} ->
					insert_groups_to_ets(Groups);
				Err ->
					lager:info("Unable to load groups file: ~p~n", [Err]),
					false
			end
	end;
insert_groups_to_ets([_|_] = Groups) ->
	Groupsbin = term_to_binary(Groups),
	case butil:ds_val(groupsbin,bkdcore_groups) of
		Groupsbin ->
			ok;
		_ ->
			Now = butil:sec(),
			{ToInsert1,TypesToInsert} = 
			lists:foldl(fun(Group,{ToInsert,Types}) ->
				% [Name1,Nodes,Index] = butil:ds_vals([<<"name">>,<<"nodes">>,<<"index">>],Grp),
				case Group of
					{Name1,Nodes1} ->
						Type = undefined,
						GP = [];
					{Name1,Nodes1,Type} ->
						GP = [];
					{Name1,Nodes1,Type,GP} ->
						ok
				end,
				Nodes = lists:sort([butil:tobin(N) || N <- Nodes1]),
				Name = butil:toatom(Name1),
				% butil:ds_add({lastchange,Name},Now,bkdcore_groups),
				% butil:ds_add({nodes,Name},Nodes,bkdcore_groups),
				case Type of
					cluster ->
						ClusterNodes = [{{cluster_group,Node},Name} || Node <- Nodes];
					_ ->
						ClusterNodes = []
				end,
				L1 = [{{nodes,Name},Nodes},{{lastchange,Name},Now}|ClusterNodes],
				case Type of
					undefined ->
						L2 = [],
						L3 = Types;
					_ ->
						L2 = [{{type,Name},Type},{{param,Name},GP}],
						case butil:ds_val(Type,Types) of
							undefined ->
								L3 = [{Type,[Name]}|Types];
							GroupsInType ->
								L3 = lists:keystore(Type,1,Types,{Type,butil:lists_add(Name,GroupsInType)})
						end
				end,
				{L2 ++ L1 ++ ToInsert,L3}
			end,{[],[]},Groups),
			butil:ds_add(TypesToInsert++ToInsert1,bkdcore_groups),
			% For every group that has not been updated (it was removed from file) take name and type
			Todelete = [{Name,butil:ds_val({type,Name},bkdcore_groups)} || 
													{{lastchange,Name},LC} <- ets:tab2list(bkdcore_groups), LC /= Now],
			[begin
				% Remove all {Key,GroupName} from ets
				[butil:ds_rem({K,Name1},bkdcore_groups) || {{K,Name1},_} <- ets:tab2list(bkdcore_groups),Name1 == Name],
				% Also remove node from list of nodes of this type
				case Type of
					undefined ->
						ok;
					_ ->
						butil:ds_add(Type,lists:delete(Name,butil:ds_val(Type,bkdcore_groups)),bkdcore_groups)
				end
			 end || {Name,Type} <- Todelete],
			 butil:ds_add(groupsbin,Groupsbin, bkdcore_groups),
			 ok
	end;
insert_groups_to_ets(_) ->
	false.

growl(Message,Success) ->
	case butil:get_os() of
		osx ->
			spawn(fun() ->
			  GrowlMsg = io_lib:format("growlnotify -n \"Sync\" -m \"~s\" \"~s\"", [Message, Success]),
			  os:cmd(GrowlMsg)
			 end);
		_ ->
			ok
	end.

% Parses mime.types file from apache
% http://svn.apache.org/viewvc/httpd/httpd/branches/2.2.x/docs/conf/mime.types?revision=1301896&view=co
parsemime(Pth) ->
	{ok,B} = file:read_file(Pth),
	Lines = binary:split(B,<<"\n">>,[global,trim]),
	parsemime(Lines,[]).
parsemime([<<"#",_/binary>>|T], L) ->
	parsemime(T,L);
parsemime([H|T],L) ->
	[Mime|Extensions] = binary:split(H,[<<" ">>,<<"\t">>],[global,trim]),
	Exl = [{E,Mime} || E <- Extensions,byte_size(E) > 0],
	parsemime(T,Exl ++ L);
parsemime([],L) ->
	{ok,L}.




	