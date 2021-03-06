% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-ifndef(MODULED).
-module(butil).
-endif.
-compile(export_all).
% -define(DEBUG,true).
-include_lib("../include/bkdcore.hrl").
-include_lib("../include/emysql.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 						WEB FUNCTIONS
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fbcookie(Secret,CV) ->
	Lsig = [list_to_tuple(string:tokens(X,"=")) || X <- string:tokens(mochiweb_util:unquote(CV),"&")],
	Sig = tobin(ds_val("sig",Lsig)),
	L = [[K,$=,V]|| {K,V} <- lists:keysort(1,lists:keydelete("sig",1,Lsig))],
	case dec2hex(erlang:md5(iolist_to_binary([L|Secret]))) of
		Sig ->
			{ok,Lsig};
		_ ->
			throw(invalid_session)
	end.

% Check facebook cookies and match with signiture
is_fbsession(AppId,Secret,[{_,_}|_] = Cookies) ->
	case find(fun({"fbs_" ++ AppId1,V}) ->
						% match on appid not api key
						AppId = AppId1,
					 	{ok,V};
				     (_) ->
						undefined
					end,
				Cookies) of
		{ok, CV} ->
			ok;
		_ ->
			CV = ok,
			throw(invalid_session)
	end,
	fbcookie(Secret,CV);
is_fbsession(ApiKey,Secret,[Cookies]) ->
	is_fbsession(ApiKey,Secret,string:tokens(Cookies,"; "));
is_fbsession(ApiKey,Secret,Cookies) ->
	{FBcookies,ExHash} = fbcookies(tobin(ApiKey),undefined,[], Cookies),
	CL = lists:foldl(fun({K,V},B) ->
					<<B/binary,K/binary,"=",V/binary>>
		        end,<<>>,FBcookies),
	case dec2hex(erlang:md5(<<CL/binary,(tobin(Secret))/binary>>)) of
		ExHash ->
			true;
		_X ->
			false
	end.


fbcookies(Api,Hash,L,[{K,V}|T]) ->
	ApiSize = byte_size(Api),
	case tobin(K) of
		<<Api:ApiSize/binary,"_",Name/binary>> ->
			fbcookies(Api,Hash,[{Name,tobin(V)}|L],T);
		<<Api:ApiSize/binary,"=",SH/binary>> ->
			fbcookies(Api,SH,L,T);
		_ ->
			fbcookies(Api,Hash,L,T)
	end;
fbcookies(Api,Hash,L,[H|T]) ->
	ApiSize = byte_size(Api),
	case tobin(H) of
		<<Api:ApiSize/binary,"=",SH/binary>> ->
			fbcookies(Api,SH,L,T);
		<<Api:ApiSize/binary, "_", Key/binary>> ->
			fbcookies(Api,Hash,[splitat($=, <<>>, Key)|L], T);
		_X ->
			fbcookies(Api,Hash,L,T)
	end;
fbcookies(_,Hash, L,[]) ->
	{lists:keysort(1,L),Hash}.

splitat(Char,Left,<<Char,Right/binary>>) ->
	{Left,Right};
splitat(Char,Left,<<F,R/binary>>) ->
	splitat(Char,<<Left/binary, F>>,R);
splitat(_,Left,<<>>) ->
	{Left,<<>>}.

-ifdef(DEBUG).
-define(VALIDATE(A,Fun), Fun()).
-else.
-define(VALIDATE(A,Fun), validate_request(A,Fun)).
-endif.
-define(ERRACTION(X),	case get(error_fun) of
							undefined ->
								case get(langmod) of
									undefined ->
										io:format("path crashed ~p~nexception ~p~nstack ~p~n", [butil:raw_path(A),X,erlang:get_stacktrace()]);
									Langmod ->
										Langmod:error_message(A,X)
								end;
							EF ->
								EF(A,erlang:get_stacktrace(), X)

						end).

funout(Module,Name) ->
	funout(Module,Name,#pgr{status = 404, content = "404"}).
funout(Module,Name,NotFound) ->
	case catch list_to_existing_atom(Name) of
		X when is_atom(X) ->
			case erlang:function_exported(Module,X,1) of
				true ->
					{Module,X};
				false ->
					NotFound
			end;
		_X ->
			NotFound
	end.

out(A,#pgr{} = P) ->
	validate(A,P);
out(A,Fun) ->
	case is_record(A,arg) of
		true ->
			out1(A,Fun(A#arg.appmoddata,A),yaws,undefined);
		false ->
			try Fun((tl(mochiweb_request:get(path,A))),A) of
				X ->
					out1(A,X,mochi,undefined)
			catch
				throw:X when is_record(X,pgr) ->
					out1(A,X,mochi,undefined);
				throw:X ->
					out1(A,undefined,mochi,?ERRACTION(X));
				error:X ->
					out1(A,undefined,mochi,?ERRACTION(X));
				exit:X ->
					out1(A,undefined,mochi,?ERRACTION(X))
			end
	end.
out1(A,X,Srv,undefined) ->
	case X of
		#pgr{} = _ ->
			validate(A,X);
		_ when is_function(X) ->
			validate(A,X);
		{M,F} when is_atom(M), is_atom(F) ->
			validate(A,X);
		{content,Mime,Body} when Srv == mochi ->
			mochiweb_request:respond({200,[{"Content-type",Mime}],Body},A);
		_ ->
			X
	end;
out1(A,_,_,Err) ->
	validation_response(A,Err,undefined,undefined,false).

validate(_A,#pgr{status = noop}) ->
	ok;
validate(_A,#pgr{status = exitnormal}) ->
	exit(normal);
validate(A, #pgr{} = F) ->
	validation_response(A,F,undefined,undefined,false);
validate(A,F) ->
	validation_response(A,?VALIDATE(A,F),undefined,undefined,false).
validate(A,F,ErrPath,ErrCookie) ->
	validation_response(A,?VALIDATE(A,F),ErrPath,ErrCookie,false).
logged_in_required(A,F,ErrPath,ErrCookie) ->
	validation_response(A,?VALIDATE(A,F),ErrPath,ErrCookie,true).

validation_response(_,#pgr{status = noop} = _P,_,_,_) ->
	ok;
validation_response(_,#pgr{status = exitnormal} = _P,_,_,_) ->
	exit(normal);
validation_response(A,#pgr{content = {ok, Body}} = X,ErrPath,ErrCookie,LoginReq) ->
	validation_response(A,X#pgr{content = Body},ErrPath,ErrCookie,LoginReq);
validation_response(A,{error, Type, _Msg, _Param}, _ErrPath, _ErrCookie,true) when
 													Type == invalid_user; Type == invalid_session ->
	case is_record(A,arg) of
		true ->
			case raw_path(A) of
				undefined ->
					Path = "/";
				Path ->
					true
			end,
			[set_msg_cookie(A, "loginaction", #pgerr{docid = tobin(Path)}),{redirect_local,{any_path,"/login"}}];
		false ->
			case raw_path(A) of
				undefined ->
					Path = "/";
				Path ->
					true
			end,
			mochiweb_request:respond({302,lists:flatten([set_msg_cookie(A, "loginaction", #pgerr{docid = tobin(Path)}),
							{"Location","/login"}]),
					   <<>>},A)
	end;
validation_response(A,X,ErrPath,ErrCookie,LoginReq) ->
	case X of
		R when R#pgr.status < 226 ->
			case LoginReq of
				true ->
					mochiweb_request:respond({R#pgr.status,lists:flatten([kill_cookie(A,"loginaction"),R#pgr.headers,{"Content-type",R#pgr.mime},R#pgr.cookies]),R#pgr.content},A);
				_ ->
					mochiweb_request:respond({R#pgr.status,lists:flatten([{"Content-Type",R#pgr.mime},
																				{"Content-Length",iolist_size(R#pgr.content)},
																				R#pgr.headers,R#pgr.cookies]),
												R#pgr.content},A)
			end;
		R when R#pgr.status < 400, is_record(A,arg) ->
			case R#pgr.content of
				"rtsp" ++ _ ->
					Location = R#pgr.content;
				[$h,$t|_] ->
					Location = R#pgr.content;
				"market://" ++ _ ->
					Location = R#pgr.content;
				_ ->
					Location = [$h,$t,$t,$p,$:,$/,$/|(A#arg.headers)#headers.host] ++ R#pgr.content
			end,
			[R#pgr.cookies,{status, R#pgr.status},{header, [$L,$o,$c,$a,$t,$i,$o,$n,$:,$\s|Location]}];
		R when R#pgr.status < 400 ->
			% io:format("Calling flatten on ~p~n", [[{"Location",Location}|R#pgr.cookies]]),
			mochiweb_request:respond({R#pgr.status,lists:flatten([{"Location",R#pgr.content},R#pgr.cookies]),<<>>},A);
		R when R#pgr.status > 400, is_record(A,arg) ->
			[{status, R#pgr.status},{content, "text/html", R#pgr.content}];
		R when R#pgr.status > 400 ->
			mochiweb_request:respond({R#pgr.status,lists:flatten(R#pgr.headers),R#pgr.content},A);
		{error, invalid_session, _, _} when is_record(A,arg), LoginReq == false ->
			[{status, 403},{content, "text/plain",<<"403 forbidden">>}];
		{error, invalid_session, _, _} when LoginReq == false ->
			mochiweb_request:respond({403,[], <<"403 forbidden">>},A);
		{error, Type, Msg, Param} when is_record(A,arg), ErrPath /= undefined ->
			case ErrCookie of
				true ->
					[set_error_cookie(A, #pgerr{docid = tobin(Type), msg = Msg, param = Param}),
			 			{redirect_local, {any_path, ErrPath}}];
				false ->
					{redirect_local,{any_path,ErrPath}}
			end;
		{error, Type, Msg, Param} when ErrPath /= undefined ->
			case ErrCookie of
				true ->
					mochiweb_request:respond({302,lists:flatten([set_error_cookie(A, #pgerr{docid = tobin(Type), msg = Msg, param = Param}),
									{"Location",ErrPath}]),
							   <<>>},A);
				false ->
					% {redirect_local,{any_path,ErrPath}}
					mochiweb_request:respond({302,[{"Location",ErrPath}], <<>>},A)
			end;
		_ when is_record(A,arg) ->
			mochiweb_request:respond({500, [{"Content-Type", "text/plain"}],
                         <<"Internal Server Error">>},A);
		{error,undefined,undefined,undefined} ->
			mochiweb_request:respond({500, [{"Content-Type", "text/plain"}],
                         <<"Internal Server Error">>},A);
		X ->
			mochiweb_request:respond({400,[],X#pgr.content},A)
	end.

validate_request(A,{M,F}) ->
	case catch apply(M,F,[A]) of
		#pgr{} = P ->
			P;
		X ->
			?ERRACTION(X)
	end;
validate_request(A, Fun) ->
	case catch Fun() of
		#pgr{} = P ->
			P;
		X ->
			?ERRACTION(X)
	end.




safecall(undefined) ->
	undefined;
safecall(F) when is_function(F) ->
	F();
safecall({M,F,P}) ->
	apply(M,F,P);
safecall({M,F}) when is_atom(M) ->
	apply(M,F,[]);
safecall({M,F}) when is_function(M) ->
	M(F).
safecall(undefined,_,_) ->
	undefined;
safecall(M,F,P) ->
	apply(M,F,P).
safecall(undefined,_) ->
	undefined;
safecall(M,F) when is_atom(M) ->
	apply(M,F,[]);
safecall(M,F) when is_function(M) ->
	M(F).

json_resp(V) ->
	json_resp(V,[]).
json_resp(#pgr{content = <<_/binary>>} = V,Cookies) ->
	V#pgr{mime = "application/json",
		 cookies = Cookies,
		 headers = [{"Cache-control","no-cache"},{"Pragma", "no-cache"}]};
json_resp({array,[]},Cookies)->
	#pgr{mime = "application/json",
		 cookies = Cookies,
		 headers = [{"Cache-control","no-cache"},{"Pragma", "no-cache"}],
		 content = bjson:encode({array,[]})};
json_resp({K,V},Cookies) when is_list(V) ->
	json_resp([{K,tobin(V)}],Cookies);
json_resp({K,V},Cookies) ->
	json_resp([{K,V}],Cookies);
json_resp(C,Cookies) ->
	#pgr{mime = "application/json",
		 cookies = Cookies,
		 headers = [{"Cache-control","no-cache"},{"Pragma", "no-cache"}],
		 content = bjson:encode(C)}.

cookie_domain(Host) ->
	case Host of
		"www" ++ HRem ->
			HRem;
		HR ->
			[$.|HR]
	end.

get_session_cookie(#arg{} = A) ->
	case yaws_api:find_cookie_val("session", (A#arg.headers)#headers.cookie) of
		[] ->
			throw(invalid_session);
		X ->
			case string:tokens(tolist(base64:decode(X)), ",") of
				[R] ->
					R;
				R ->
					R
			end
	end;
get_session_cookie(R) ->
	case mochiweb_request:get_cookie_value("session",R) of
		undefined ->
			throw(invalid_session);
		X ->
			case string:tokens(tolist(hex2dec(X)), ",") of
				[S] ->
					S;
				S ->
					S
			end
	end.
get_session(R) ->
	case mochiweb_request:get_cookie_value("session",R) of
		undefined ->
			throw(invalid_session);
		X ->
			case string:tokens(mochiweb_util:unquote(X), ",") of
				[S] ->
					S;
				S ->
					S
			end
	end.
try_session(R) ->
	case mochiweb_request:get_cookie_value("session",R) of
		undefined ->
			undefined;
		X ->
			case string:tokens(mochiweb_util:unquote(X), ",") of
				[S] ->
					S;
				S ->
					S
			end
	end.


kill_session_cookie(A) ->
	kill_cookie(A,"session").
session_cookie(A, Us, Key) ->
	set_cookie(A,"session", tolist(dec2hex(<<(tobin(Us))/binary,",", (tobin(Key))/binary>>)), 14 * 24 * 60 * 60).
session_cookie(A, Key) ->
	set_cookie(A,"session", tolist(base64:encode(tobin(Key))), 14 * 24 * 60 * 60).
set_session(A,Key) ->
	set_cookie(A,"session",mochiweb_util:quote_plus(Key),14 * 24 * 60 * 60).
set_session(A,U,Key) ->
	set_cookie(A,"session",mochiweb_util:quote_plus(tolist(U) ++ "," ++ tolist(Key)),14 * 24 * 60 * 60).

set_msg_cookie(A, Name, Rec) ->
	set_cookie(A, Name, dec2hex(mongodb:encoderec(Rec)), 300).
set_msg_cookie(A, Name, Rec, Path) ->
	set_cookie(A, Name, dec2hex(mongodb:encoderec(Rec)), 300, Path).
get_msg_cookie(A, Name, RecIn) ->
	case get_cookie(A,Name) of
		undefined ->
			undefined;
		Msg ->
			case catch mongodb:decoderec(RecIn, hex2dec(Msg)) of
				[Rec] ->
					Rec;
				Res ->
					Res
			end
	end.

set_error_cookie(A, Rec) ->
	set_cookie(A, "error", dec2hex(mongodb:encoderec(Rec)), 60).
get_error_cookie(ErrRec, A) ->
	get_msg_cookie(A,"error",ErrRec).

get_cookie(#arg{} = A, Name) ->
	case yaws_api:find_cookie_val(Name, (A#arg.headers)#headers.cookie) of
		[] ->
			undefined;
		"none" ->
			undefined;
		X ->
			X
	end;
get_cookie(R,N) ->
	case mochiweb_request:get_cookie_value(N,R) of
		"none" ->
			undefined;
		X ->
			X
	end.
kill_cookie(#arg{} = A,Name) ->
	yaws_api:setcookie(Name, "none", "/", rfctime(sec()-10),
								cookie_domain((A#arg.headers)#headers.host));
kill_cookie(R,N) ->
	mochiweb_cookies:cookie(N,"none",[{path, "/"},{max_age,-10},{domain,cookie_domain(mochiweb_request:get_header_value("host",R))}]).

set_cookie(#arg{} = A,Name,Val,SecsValid, Path) ->
	yaws_api:setcookie(Name, Val, Path, rfctime(sec() + SecsValid),
								cookie_domain((A#arg.headers)#headers.host));
set_cookie(R,N,V,Secs,Path) ->
	mochiweb_cookies:cookie(N,V,[{path, Path},{max_age,Secs},{domain,cookie_domain(mochiweb_request:get_header_value("host",R))}]).
set_cookie(#arg{} = A,Name,Val,SecsValid) ->
	yaws_api:setcookie(Name, Val,	"/", rfctime(sec() + SecsValid),
								cookie_domain((A#arg.headers)#headers.host));
set_cookie(R,N,V,Secs) ->
	mochiweb_cookies:cookie(N,V,[{path, "/"},{max_age,Secs},{domain,cookie_domain(mochiweb_request:get_header_value("host",R))}]).

add_params_mochiweb_request(Items)->
	erlang:put(mochiweb_request_qs, Items).
remove_params_mochiweb_request(Items)->
	erlang:put(mochiweb_request_qs, lists:filter(fun(X) -> lists:member(element(1,X),Items) /= true end, erlang:get(mochiweb_request_qs))).

form_validation(L) ->
	form_validation([],L).
form_validation(E,[{password, [_|_] = P}|T]) when length(P) =< 30, length(P) > 5 ->
	case lists:keytake(password, 1, T) of
		false ->
			form_validation([password|E], T);
		{value, {password, P}, NT} ->
			form_validation(E, NT);
		_ ->
			form_validation([password|E], lists:keydelete(password, 1, T))
	end;
form_validation(E, [{email, [_|_] = N}|T]) ->
	case catch validate_email(N) of
		ok ->
			form_validation(E, T);
		_ ->
			form_validation([email|E], T)
	end;
form_validation(E, [{{email,TypeName}, [_|_] = N}|T]) ->
	case catch validate_email(N) of
		ok ->
			form_validation(E, T);
		_X ->
			form_validation([TypeName|E], T)
	end;
form_validation(E, [{Name, [_|_] = N}|T]) when Name == name; Name == last_name; Name == first_name ->
	F = fun() ->
			case length(N) of
				X when X > 0, X < 31 ->
					true = valid_name(decode_percent(N))
			end,
			Low = string:to_lower(N),
			0 = string:str(Low,"select "),
			0 = string:str(Low,"delete "),
			0 = string:str(Low,"drop "),
			0 = string:str(Low,"insert "),
			ok
		end,
	case catch F() of
		ok ->
			form_validation(E,T);
		_X ->
			form_validation([Name|E],T)
	end;

form_validation(E, [{Name, [_|_] = N}|T]) when Name == post_number ->
	F= fun() ->
			case erlang:length(N) of
				X when X > 3, X < 5 ->
					true = valid_integer(N)
			end,
			ok
		end,
	case catch F() of
		ok ->
			form_validation(E,T);
		_X ->
			form_validation([Name|E],T)
	end;

form_validation(E,[{phone,N}|T]) ->
	form_validation(E,[{gsm,N}|T]);
form_validation(E, [{gsm, N}|T]) when is_list(N) ->
	case length(N) of
		0 ->
			form_validation(E,T);
		9 ->
			case valid_gsm(N) of
				true when hd(N) == $0 ->
					form_validation(E, T);
				_ ->
					form_validation([gsm|E], T)
			end;
		_ ->
			form_validation([gsm|E], T)
	end;
form_validation(E, [{Name, N}|T]) when Name == client_addr; Name == challenge; Name == response; Name == privatekey  ->
	{value, {_, IP}, LR} = lists:keytake(client_addr, 1, [{Name, N}|T]),
	{value, {_, Challenge}, LR1} = lists:keytake(challenge, 1, LR),
	{value, {_, Response}, LR2} = lists:keytake(response, 1, LR1),
	{value, {_, Privkey}, LRem} = lists:keytake(privatekey, 1, LR2),

	case http("http://www.google.com/recaptcha/api/verify",
						[{"User-Agent", "reCAPTCHA/PHP"},
						 {"Content-type", "application/x-www-form-urlencoded"}],
						post, "privatekey=" ++ tolist(Privkey) ++ "&" ++
							  "remoteip=" ++ to_ip(IP) ++ "&challenge=" ++ tolist(Challenge) ++
							  "&response=" ++ tolist(Response),
						[]) of
		{ok, "200", _, RS} ->
			% io:format("~p~n", [RS]),
			[Bool|_] = string:tokens(tolist(RS),"\n"),
			case Bool of
				"true" ++ _ ->
					form_validation(E, LRem);
				_X ->
					% io:format("~p~n", [_X]),
					form_validation([captcha|E], LRem)
			end;
		_X ->
			% io:format("~p~n", [_X]),
			form_validation([captcha|E], LRem)
	end;
form_validation(E,[{Key,_Val}|T]) ->
	form_validation([Key|E], T);
form_validation([],[]) ->
	ok;
form_validation(E, []) ->
	throw({form_validation, E}).

validate_email(N) ->
	[Before,After] = string:tokens(N,"@"),
	LB = length(Before),
	LA = length(After),
	case true of
		_ when LB > 0, LA > 0, LB < 30, LA < 30 ->
			true
	end,
	case hd(Before) of
		$. ->
			throw(email);
		_ ->
			ok
	end,
	true = valid_email_local(Before),
	[_,_|_] = string:tokens(After, "."),
	ok.

valid_name([C|T]) when C < 128 andalso C >= $a, C =< $z;
									   C >= $A, C =< $Z;
									   C >= $0, C =< $9;
									   C == $.; C == $'; C == $-; C == $_; C == $\s ->
	valid_name(T);
valid_name([C|T]) when C > 128 ->
	valid_name(T);
valid_name([]) ->
	true;
valid_name(_) ->
	false.


valid_integer(Str)->
	case catch toint(Str) of
		Int when is_integer(Int) ->
			true;
		_ ->
			false
	end.


filter_name(L) ->
	filter_name(tolist(L),[]).
filter_name([C|T],L) when C < 128 andalso C >= $a, C =< $z;
									   C >= $A, C =< $Z;
									   C >= $0, C =< $9;
									   C == $.; C == $'; C == $-; C == $_; C == $\s ->
	filter_name(T,[C|L]);
filter_name([C|T],L) when C > 128 ->
	filter_name(T,[C|L]);
filter_name([_|T],L) ->
	filter_name(T,L);
filter_name([],L) ->
	lists:reverse(L).

valid_email_local([C|T]) when C < 128 andalso C >= $a, C =< $z;
										C >= $A, C =< $Z;
										C >= $0, C =< $9 ->
	valid_email_local(T);
valid_email_local([$.,$.|_]) ->
	false;
valid_email_local([C|T]) ->
	case lists:member(C,[$.,$!, $#, $$, $&, $', $*, $+, $-, $/, $=, $?, $^, $_, $`, ${, $|, $}, $~]) of
		true ->
			valid_email_local(T);
		false ->
			false
	end;
valid_email_local([]) ->
	true.


valid_gsm([C|T]) when C >= $0, C =< $9 ->
	valid_gsm(T);
valid_gsm([]) ->
	true;
valid_gsm(_) ->
	false.

filter_gsm(L) when is_list(L); is_binary(L) ->
	filter_gsm(tolist(L),[]).
filter_gsm("+386" ++ T, L) ->
	filter_gsm(T,[$0|L]);
filter_gsm([C|T],L) when C >= $0, C =< $9 ->
	filter_gsm(T,[C|L]);
filter_gsm([_|T],L) ->
	filter_gsm(T,L);
filter_gsm([],L) ->
	lists:reverse(L).

is_sql(L) ->
	is_sql(string:to_lower(L),[]).
is_sql("select " ++ _,_) ->
	true;
is_sql("drop " ++ _,_) ->
	true;
is_sql("delete " ++ _,_) ->
	true;
is_sql("insert " ++ _,_) ->
	true;
is_sql([_|T],L) ->
	is_sql(T,L);
is_sql([],_) ->
	false.

cleanup_vars(L) ->
	cleanup_vars([], L).
cleanup_vars(L, [{Key, {ok, Val}}|T]) ->
	cleanup_vars([{Key, Val}|L], T);
cleanup_vars(L, [{Key, X}|T]) ->
	cleanup_vars([{Key,X}|L], T);
cleanup_vars(L, []) ->
	L.

check_cachecall(Line) ->
	case get(cachecall) of
		true ->
			throw({invalid_cachecall,Line});
		_ ->
			ok
	end.

% sendfile(ServerObject,NameOfProject,PathToFile)
% Yaws has all necessary info in project configuration.
sendfile(#arg{} = _A,_,Path) ->
	{page,Path};
sendfile(R,Project,Path) ->
	mochiweb_request:serve_file(Path,project_rootpath() ++ "/static/" ++ Project,R).

host(#arg{} = A) ->
	check_cachecall(?LINE),
	(A#arg.headers)#headers.host;
host(R) ->
	check_cachecall(?LINE),
	mochiweb_request:get_header_value("host",R).

peer_ip(#arg{} = A) ->
	check_cachecall(?LINE),
	case A#arg.client_ip_port of
		{IP,_} ->
			IP;
		_ ->
			"127.0.0.1"
	end;
peer_ip(R) ->
	check_cachecall(?LINE),
	mochiweb_request:get(peer,R).

qvar([_|_] = Name,A) ->
	qvar(A,Name);
qvar(#arg{} = Arg,Name) ->
	check_cachecall(?LINE),
	case yaws_api:queryvar(Arg,Name) of
		{ok, V} ->
			decode_percent(V);
		_ ->
			""
	end;
qvar(Req,Name) ->
	check_cachecall(?LINE),
	case proplists:get_value(Name,mochiweb_request:parse_qs(Req)) of
		undefined ->
			"";
		X ->
			X
	end.

qmvar([_|_] = Name, A) ->
	qmvar(A,Name);
qmvar(#arg{} = Arg,Name) ->
	check_cachecall(?LINE),
	case yaws_api:queryvar(Arg,Name) of
		{ok, V} ->
			decode_percent(V);
		_ ->
			throw({invalid_query,Arg#arg.appmoddata,Name})
	end;
qmvar(Req,Name) ->
	check_cachecall(?LINE),
	case proplists:get_value(Name,mochiweb_request:parse_qs(Req)) of
		undefined ->
			throw({invalid_query,mochiweb_request:get(path,Req),Name});
		X ->
			X
	end.

pgvar([_|_] = Name,A) ->
	pgvar(A,Name);
pgvar(#arg{} = Arg,Name) ->
	check_cachecall(?LINE),
	case qvar(Arg,Name) of
		"" ->
			case yaws_api:getvar(Arg,Name) of
				{ok, Var} ->
					decode_percent(Var);
				undefined ->
					""
			end;
		V ->
			V
	end;
pgvar(R,N) ->
	check_cachecall(?LINE),
	case qvar(R,N) of
		"" ->
			case mochiweb_request:get(method,R) of
				'POST' ->
					case mochiweb_request:get_primary_header_value("content-type",R) of
		            	"application/x-www-form-urlencoded" ->
							case proplists:get_value(N,mochiweb_request:parse_post(R)) of
								undefined ->
									"";
								X ->
									X
							end;
						_ ->
							""
					end;
				_ ->
					""
			end;
		X ->
			X
	end.


raw_path(#arg{} = Arg) ->
	check_cachecall(?LINE),
	case Arg#arg.req of
		{_,_,{_,Path}, _} ->
			Path;
		_ ->
			undefined
	end;
raw_path(R) when element(1,R) == http_req ->
	case cowboy_req:qs(R) of
		<<>> ->
			cowboy_req:path(R);
		QS ->
			iolist_to_binary([cowboy_req:path(R),"?",QS])
	end;
raw_path(R) ->
	check_cachecall(?LINE),
	mochiweb_request:get(raw_path,R).

userip(#arg{} = Arg) ->
	check_cachecall(?LINE),
	% io:format("~p~n", [(Arg#arg.headers)]),
	case lists:keyfind('X-Forwarded-For',3,(Arg#arg.headers)#headers.other) of
		{_,_,_,_,IP} ->
			case IP of
				"77.234.129.251" ->
					"127.0.0.1";
				_ ->
					toipv4(IP)
			end;
		_ ->
			"127.0.0.1"
	end;
userip(R) ->
	check_cachecall(?LINE),
	toipv4(mochiweb_request:get(peer,R)).
toipv4("::ffff:"++IP) ->
	IP;
toipv4(X) ->
	X.

remove_trail_slash(L) ->
	case lists:reverse(L) of
		"/" ++ R ->
			lists:reverse(R);
		_ ->
			L
	end.
body(#arg{} = A) ->
	check_cachecall(?LINE),
	A#arg.clidata;
body(A) ->
	check_cachecall(?LINE),
	mochiweb_request:recv_body(A).
body(#arg{} = A,Limit) when byte_size(A#arg.clidata) < Limit ->
	check_cachecall(?LINE),
	A#arg.clidata;
body(A,Limit) ->
	check_cachecall(?LINE),
	mochiweb_request:recv_body(Limit,A).



hexlist_to_integer(List) ->
    hexlist_to_integer(lists:reverse(tolist(List)), 1, 0).

hexlist_to_integer([H | T], Multiplier, Acc) ->
    hexlist_to_integer(T, Multiplier*16, Multiplier*to_ascii(H) + Acc);
hexlist_to_integer([], _, Acc) ->
    Acc.
to_ascii($A) -> 10;
to_ascii($a) -> 10;
to_ascii($B) -> 11;
to_ascii($b) -> 11;
to_ascii($C) -> 12;
to_ascii($c) -> 12;
to_ascii($D) -> 13;
to_ascii($d) -> 13;
to_ascii($E) -> 14;
to_ascii($e) -> 14;
to_ascii($F) -> 15;
to_ascii($f) -> 15;
to_ascii($1) -> 1;
to_ascii($2) -> 2;
to_ascii($3) -> 3;
to_ascii($4) -> 4;
to_ascii($5) -> 5;
to_ascii($6) -> 6;
to_ascii($7) -> 7;
to_ascii($8) -> 8;
to_ascii($9) -> 9;
to_ascii($0) -> 0.



parse_address({_IP,_Port} = A) ->
	A;
parse_address(Addr) ->
	case string:tokens(tolist(Addr),":") of
		[IP,Port] ->
			{IP,toint(Port)};
		[IP] ->
			{IP,5999}
	end.

expand_path([$\~|Path]) ->
	project_rootpath()++Path;
expand_path(P) ->
	P.
project_rootpath() ->
	case application:get_env(bkdcore,rootpath) of
		undefined ->
			{_,_,Path} = code:get_object_code(?MODULE),
			PathParts = filename:split(Path),
			F = case lists:reverse(PathParts) of
				[_,"ebin",_,"lib",_,"_build"|Rem] ->
					filename:join(lists:reverse(Rem));
				[_,"ebin",_,"apps"|Rem] ->
					filename:join(lists:reverse(Rem));
				[_,"ebin",_,"deps"|Rem] ->
					filename:join(lists:reverse(Rem));
				[_,"ebin"|Rem] ->
					filename:join(lists:reverse(Rem))
			end,
			application:set_env(bkdcore,rootpath,F),
			F;
		{ok,F} ->
			F
	end.
bkdcore_path() ->
	{_,_,Path} = code:get_object_code(?MODULE),
	PathParts = filename:split(Path),
	[_,"ebin"|Rem] = lists:reverse(PathParts),
	filename:join(lists:reverse(Rem)).

safesend(undefined,_) ->
	ok;
safesend(PID,Msg) when is_pid(PID) ->
	PID ! Msg;
safesend(R,M) when is_atom(R) ->
	safesend(whereis(R),M);
safesend(_,_) ->
	ok.

set_permission(Path) ->
	case prim_file:read_file_info(Path) of
		{ok, I} ->
			prim_file:write_file_info(Path, I#file_info{mode = 8#00400 + 8#00200 + 8#00100 + 8#00040 + 8#00020 +
														  8#00010 + 8#00004 + 8#00002 + 8#00001 + 16#800 + 16#400});
		_ ->
			true
	end.

filetype(Filename) ->
	case string:sub_string(Filename, length(Filename)-3) of
		".mp4" ->
			mp4;
		".mov" ->
			mp4;
		".m4v" ->
			mp4;
		".f4v" ->
			mp4;
		".flv" ->
			flv;
		".org" ->
			whatever;
		_ ->
			whatever
	end.

file_age(Path) when is_list(Path) ->
	{ok,I} = prim_file:read_file_info(Path),
	file_age(I);
file_age(I) ->
	Now = calendar:datetime_to_gregorian_seconds(erlang:localtime()),
	MTime = calendar:datetime_to_gregorian_seconds(I#file_info.mtime),
	Now - MTime.

deldir(Root) ->
	F = fun(F, Dir, [H|T]) ->
			Path = filename:join([Dir,  H]),
			case file:read_file_info(Path) of
				{ok,Info} ->
					case Info#file_info.type of
						regular ->
							file:delete(Path),
							F(F,Dir,T);
						symlink ->
							file:delete(Path),
							F(F,Dir,T);
						_ when H == "." ->
							F(F,Dir,T);
						_ when H == ".." ->
							F(F,Dir,T);
						directory ->
							case file:list_dir(Path) of
								{ok,L} ->
									F(F,Path,L),
									F(F,Dir,T);
								_ ->
									F(F,Dir,T)
							end;
						_ ->
							F(F,Dir,T)
					end;
				_ ->
					file:delete(Path),
					F(F,Dir,T)
			end;
			(_F,Dir,[]) ->
				file:del_dir(Dir)
		end,
	case file:list_dir(Root) of
		{ok,L} ->
			F(F,Root,L);
		_ ->
			ok
	end.

savetermfile(Path,Term) ->
	savebinfile(Path,term_to_binary(Term,[compressed,{minor_version,1}])).
savebinfile(Path,Bin) ->
	filelib:ensure_dir(Path),
	ok = prim_file:write_file(Path,[<<(erlang:crc32(Bin)):32/unsigned>>,Bin]).
readtermfile(Path) ->
	case readbinfile(Path) of
		undefined ->
			undefined;
		Bin ->
			binary_to_term(Bin)
	end.
readbinfile(Path) ->
	case prim_file:read_file(Path) of
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

move_file(From,To) ->
	case filelib:file_size(From) of
		0 ->
			ok;
		_ ->
			case file:rename(From,To) of
				ok ->
					ok;
				_MX ->
					case file:copy(From,To) of
						{ok,_} ->
							file:delete(From),
							ok;
						X ->
							X
					end
			end
	end.
is_app_running(Name) ->
	lists:keymember(Name,1,application:which_applications()).
wait_for_app(Name) ->
	case is_app_running(Name) of
		true ->
			ok;
		false ->
			timer:sleep(100),
			wait_for_app(Name)
	end.
reloadmod(Module) ->
	code:purge(Module),
	code:load_file(Module).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 						TIME FUNCTIONS
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% rfc3339
% 2010-03-23T08:36:11Z
% rfc3339(X) when is_list(X) ->
% 	rfc3339(tobin(X));
% rfc3339(<<Year:4/binary, "-", Month:2/binary, "-", Day:2/binary, "T", Hour:2/binary,":",Min:2/binary,":",Sec:2/binary,_/binary>>) ->
% 	{{toint(Year),toint(Month),toint(Day)},{toint(Hour),toint(Min),toint(Sec)}}.

rfc3339() ->
    rfc3339(calendar:now_to_local_time(os:timestamp())).
rfc3339(Gsec) when is_integer(Gsec) ->
    rfc3339(datetime(Gsec));
rfc3339(X) when is_list(X) ->
	rfc3339(tobin(X));
rfc3339(<<Year:4/binary, "-", Month:2/binary, "-", Day:2/binary, "T", Hour:2/binary,":",Min:2/binary,":",Sec:2/binary,_/binary>>) ->
	{{toint(Year),toint(Month),toint(Day)},{toint(Hour),toint(Min),toint(Sec)}};
rfc3339(<<Year:4/binary, "-", Month:2/binary, "-", Day:2/binary, "T", Hour:2/binary,":",Min:2/binary>>) ->
	{{toint(Year),toint(Month),toint(Day)},{toint(Hour),toint(Min),0}};
rfc3339(<<Year:4/binary, "-", Month:2/binary, "-", Day:2/binary>>) ->
	{{toint(Year),toint(Month),toint(Day)},{toint(0),toint(0),toint(0)}};
rfc3339({{Year, Month, Day}, {Hour, Min, Sec}}) ->
    tobin(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w~s",
                  [Year,Month,Day, Hour, Min, Sec, zone()])).

zone() ->
    Time = erlang:universaltime(),
    LocalTime = calendar:universal_time_to_local_time(Time),
    DiffSecs = calendar:datetime_to_gregorian_seconds(LocalTime) -
        calendar:datetime_to_gregorian_seconds(Time),
    zone(DiffSecs div 3600, (DiffSecs rem 3600) div 60).

zone(Hr, Min) when Hr < 0; Min < 0 ->
    io_lib:format("-~2..0w~2..0w", [abs(Hr), abs(Min)]);
zone(Hr, Min) when Hr >= 0, Min >= 0 ->
    io_lib:format("+~2..0w~2..0w", [Hr, Min]).

% miliseconds since epoch
milisec() ->
	milisec(os:timestamp()).
milisec({M,S,Mi}) ->
	M * 1000000000 + S*1000 + Mi div 1000.

% seconds since 0AD
sec() ->
	calendar:datetime_to_gregorian_seconds(erlang:localtime()).
sec(Date) ->
	case Date of
		{{_,_,_},{_,_,_}} ->
			calendar:datetime_to_gregorian_seconds(Date);
		{_,_,_} ->
			sec(calendar:now_to_local_time(Date));
		_ ->
			Date
	end.
datetime(I) when is_integer(I) ->
	calendar:gregorian_seconds_to_datetime(I).

epochsec_to_datetime(S) ->
	calendar:gregorian_seconds_to_datetime(S+719528 * 24 * 60 * 60).

epochsec_utc() ->
	D = calendar:now_to_universal_time(os:timestamp()),
	epochsec(D).

localtime() ->
	{_, _, _Micro} = Now = os:timestamp(),
  calendar:now_to_local_time(Now).

utctime() ->
	utctime(localtime()).
utctime(LocalTime) ->
	case calendar:local_time_to_universal_time_dst(LocalTime) of
		[] ->
			LocalTime;
		[UtcTime] ->
			UtcTime;
		[_,UtcTime2] ->
			UtcTime2
	end.

utcepoch() ->
	utcepoch(utctime()).
utcepoch(UtcTime) ->
	calendar:datetime_to_gregorian_seconds(UtcTime) - 62167219200. % - 719528 * 24 * 60 * 60.

epochsec() ->
	{M,S,_} = os:timestamp(),
	M*1000000+S.
	% epochsec(erlang:localtime()).
	% epochsec(calendar:universal_time()).
epochsec({M,S,_} = _Loctime) ->
	% epochsec(calendar:now_to_datetime(Loctime));
	M*1000000+S;
epochsec({{_,_,_},{_,_,_}} = Loctime) ->
	calendar:datetime_to_gregorian_seconds(Loctime) - 719528 * 24 * 60 * 60.
datetime_to_now({{_,_,_},{_,_,_}} = Loctime) ->
	datetime_to_now(epochsec(Loctime));
datetime_to_now(Secs) when is_integer(Secs) ->
	% Secs = epochsec(Loctime),
	{Secs div 1000000, Secs rem 1000000,0}.

datetime_to_string({Date,Time}) ->
	<<(date_to_bstring(Date, <<".">>))/binary, " ", (time_to_bstring(Time,<<":">>))/binary>>.

date_to_bstring(Delim) ->
	date_to_bstring(date(),Delim).
date_to_bstring({Year,Month,Day}, Delim) ->
	YearBin = list_to_binary(integer_to_list(Year)),
	MonthBin = list_to_binary(string:right(integer_to_list(Month), 2, $0)),
	DayBin = list_to_binary(string:right(integer_to_list(Day), 2, $0)),
	<<YearBin/binary, (tobin(Delim))/binary, MonthBin/binary, (tobin(Delim))/binary, DayBin/binary>>.

date_to_regstring(Delim) ->
	date_to_regstring(date(),Delim).
date_to_regstring({Year,Month,Day}, Delim) ->
	YearBin = list_to_binary(integer_to_list(Year)),
	MonthBin = list_to_binary(string:right(integer_to_list(Month), 2, $0)),
	DayBin = list_to_binary(string:right(integer_to_list(Day), 2, $0)),
	<<DayBin/binary, (tobin(Delim))/binary, MonthBin/binary, (tobin(Delim))/binary, YearBin/binary>>.

time_to_bstring(Delim) when is_binary(Delim) ->
	time_to_bstring(time(), Delim);
time_to_bstring(Delim) when is_list(Delim) ->
	time_to_bstring(time(), list_to_binary(Delim)).
time_to_bstring(T, Delim) when is_list(Delim) ->
	time_to_bstring(T,list_to_binary(Delim));
time_to_bstring({Hour, Min, Sec}, Delim) when is_binary(Delim), is_integer(Sec) ->
	HourBin = list_to_binary(string:right(integer_to_list(Hour), 2, $0)),
	MinBin = list_to_binary(string:right(integer_to_list(Min), 2, $0)),
	SecBin = list_to_binary(string:right(integer_to_list(Sec), 2, $0)),
	<<HourBin/binary, Delim/binary, MinBin/binary, Delim/binary, SecBin/binary>>;
time_to_bstring({Hour, Min, Sec}, Delim) when is_binary(Delim), is_float(Sec) ->
	HourBin = list_to_binary(string:right(integer_to_list(Hour), 2, $0)),
	MinBin = list_to_binary(string:right(integer_to_list(Min), 2, $0)),
	SecBin = tobin(Sec),
	<<HourBin/binary, Delim/binary, MinBin/binary, Delim/binary, SecBin/binary>>.

timestr_to_loctime(Date) when is_binary(Date) ->
	timestr_to_loctime(binary_to_list(Date));
timestr_to_loctime(Date) ->
	[Day,Time] = string:tokens(Date, " "),
	[Year,Month,MDay] = string:tokens(Day, "."),
	[Hour,Min,Sec] = string:tokens(Time, ":"),
	{{toint(Year), toint(Month), toint(MDay)}, {toint(Hour),toint(Min),toint(Sec)}}.

check_datetime({{Yr,Mo,Da},{Hr,Min,Sec}}) ->
	{{toint(Yr),toint(Mo),toint(Da)},{toint(Hr),toint(Min),toint(Sec)}}.

% Get time in rfc1123 string.
rfctime() ->
	rfctime(erlang:localtime()).
rfctime({_,_,_} = T) ->
	rfctime(calendar:now_to_local_time(T));
rfctime(Sec) when is_integer(Sec) ->
	rfctime(calendar:gregorian_seconds_to_datetime(Sec));
rfctime(T) ->
	httpd_util:rfc1123_date(T).

flatnow() ->
	{MS,S,MiS} = now(),
	MS*1000000000000 + S*1000000 + MiS.

sec_to_timestr(Sec) ->
	sec_to_timestr(Sec,":").
sec_to_timestr(Sec,Delim) ->
	D = tolist(Delim),
	S = Sec rem 60,
	Min = Sec div 60,
	Hr = Min div 60,
	string:right(tolist(Hr),2,$0)++D++string:right(tolist(Min),2,$0)++D++string:right(tolist(S),2,$0).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 						XML, records and JSON
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Convert erlmongo record into a simplexml format.
rec2xml(Rec) ->
	% [_|Fields] = element(element(2, Rec), ?RECTABLE),
	rec2xml(mongodb:recfields(Rec), Rec, mongodb:recoffset(Rec), []).
rec2xml([FieldN|T],Rec,N, L) ->
	case FieldN of
		docid ->
			Field = id;
		{Field,_} ->
			true;
		Field ->
			true
	end,
	case element(N, Rec) of
		undefined ->
			rec2xml(T,Rec,N+1,L);
		{array,_} ->
			rec2xml(T,Rec,N+1,L);
		X when is_tuple(X) ->
			rec2xml(T, Rec,N+1,[{Field, [], rec2xml(X)}|L]);
		Val when is_binary(Val) ->
			rec2xml(T, Rec,N+1, [{Field,[], [unicode:characters_to_list(Val)]}|L]);
		Val ->
			rec2xml(T, Rec,N+1, [{Field,[], [tolist(Val)]}|L])
	end;
rec2xml([],_,_,L) ->
	L.

% Returns in proplist format that needs to be converted with bjson:encode(X)
rectojson(Rec) ->
	% mongodb:recoffset(Rec)
	rectojson(recfields(Rec),Rec,2,[]).
rectojson([Field|T],Rec,N,L) ->
	% case FieldN of
	% 	docid ->
	% 		Field = id;
	% 	{Field,_} ->
	% 		ok;
	% 	Field ->
	% 		ok
	% end,
	case element(N, Rec) of
		undefined ->
			rectojson(T,Rec,N+1,L);
		X when is_tuple(X) ->
			rectojson(T, Rec,N+1,[{tobin(Field), rectojson(X)}|L]);
		Val when is_binary(Val); is_list(Val) ->
			rectojson(T, Rec,N+1, [{tobin(Field),Val}|L]);
		Val ->
			rectojson(T, Rec,N+1, [{Field, Val}|L])
	end;
rectojson([],_,_,L) ->
	L.

% record to proplist
rec2prop(Rec) ->
	rec2prop(Rec,get({recinfo,element(1,Rec)})).
rec2prop(Rec,jsonterm) ->
	rec2prop(Rec,get({recinfo,element(1,Rec)}),jsonterm);
rec2prop(Rec, RecordFields) ->
	loop_rec(RecordFields, 1, Rec, []).
rec2prop(Rec, RecordFields,jsonterm) ->
	loop_rec(RecordFields, 1, Rec, [],jsonterm).

loop_rec([H|T], N, Rec, L) ->
	loop_rec(T, N+1, Rec, [{H, element(N+1, Rec)}|L]);
loop_rec([], _, _, L) ->
	L.

loop_rec([H|T], N, Rec, L, jsonterm) ->
	loop_rec(T, N+1, Rec, [{capitalize(tolist(H)), element(N+1, Rec)}|L], jsonterm);
loop_rec([], _, _, L, jsonterm) ->
	L.

capitalize(S) ->
    F = fun([H|T]) -> [string:to_upper(H) | string:to_lower(T)] end,
    [H|T] = string:join(lists:map(F, string:tokens(S, "_")), ""),
 	[string:to_lower(H)|T].

%
% [[{<<"username">>,<<"test">>},{<<"user_id">>,1101}]] turns this into
% [[{<<"username">>,<<"test">>},{<<"userId">>,1101}]]
capitalize_proplist_for_json(List)->
	capitalize_proplist_for_json(List,[]).
capitalize_proplist_for_json([],Acc)->
	Acc;
 capitalize_proplist_for_json([Head|Tail],Acc)->
 	FixedHead = [{butil:tobin(capitalize(tolist(element(1,H)))),element(2,H)}||H<-Head],
 	capitalize_proplist_for_json(Tail,[FixedHead|Acc]).

% % convert prop list to record
prop2rec(Prop,RecName) ->
	prop2rec(Prop,RecName,get({recdef,RecName}),get({recinfo,RecName})).
prop2rec(Prop, RecName, DefRec, RecordFields) ->
	loop_fields(erlang:make_tuple(tuple_size(DefRec), RecName), RecordFields, DefRec, Prop, 2).

loop_fields(Tuple, [Field|T], DefRec, Props, N) ->
	case lists:keysearch(Field, 1, Props) of
		{value, {_, Val}} ->
			loop_fields(setelement(N, Tuple, Val), T, DefRec, Props, N+1);
		false ->
			loop_fields(setelement(N, Tuple, element(N, DefRec)), T, DefRec, Props, N+1)
	end;
loop_fields(Tuple, [], _, _, _) ->
	Tuple.


recinfo(Rec,RecType,Fields) ->
	put({recinfo, element(1,Rec)}, Fields),
	put({recdef,element(1,Rec)},Rec),
	put({rectype,element(1,Rec)},RecType).
recfields(Rec) ->
	case true of
		_ when is_tuple(Rec) ->
			get({recinfo, element(1,Rec)});
		_ when is_atom(Rec) ->
			get({recinfo, Rec})
	end.
recdef(Rec) ->
	case true of
		_ when is_tuple(Rec) ->
			get({recdef, element(1,Rec)});
		_ when is_atom(Rec) ->
			get({recdef, Rec})
	end.
rectypeinfo(Rec) ->
	case true of
		_ when is_tuple(Rec) ->
			get({rectype, element(1,Rec)});
		_ when is_atom(Rec) ->
			get({rectype, Rec})
	end.

% Simplexml to xml list.
toxml(T) when is_tuple(T) ->
	toxml([T]);
toxml(L) ->
	lists:flatten(xmerl:export_simple(L, xmerl_xml, [{prolog, ["<?xml version=\"1.0\" encoding=\"utf-8\"?>"]}])).
	% toxml(<<"<?xml version=\"1.0\" encoding=\"utf-8\"?>">>, L).
% Simplexml to xml binary
toxmlbin(L) ->
	unicode:characters_to_binary(toxml(L)).


% Write xml from simple xml format. It supports nested lists (will be placed on the same level) and cdata.
%   iolist_to_binary(butil:simplexml({a,[{b,c},[{e,f}]]})).  -> {e,f} is in it's own nested list but same level as {b,c}
%   <<"<?xml version=\"1.0\" encoding=\"utf-8\"?><a><b>c</b><e>f</e></a>">>
simplexml(V) when is_tuple(V) ->
	simplexml([V]);
simplexml(V) ->
	simplexml(<<"<?xml version=\"1.0\" encoding=\"utf-8\"?>">>,V).

simplexml1(V) when is_tuple(V) ->
	simplexml1([V]);
simplexml1(V) ->
	simplexml(<<>>,V).


simplexml(Bin,[{Name,Attr,Val}|T]) ->
	case Val of
		{cdata,Text} ->
			ValBin = <<"<![CDATA[", (tobin(Text))/binary,"]]>">>;
		_ ->
			ValBin = simplexml1(Val)
	end,
	NB = tobin(Name),
	simplexml([Bin, $<, NB, prop_to_attr(<<>>,Attr), $>, ValBin, <<"</">>, NB, $>], T);
simplexml(Bin,[{Name,{array,Val}}|T]) ->
	simplexml(Bin,[{Name,Val}|T]);
simplexml(Bin,[{Name,Val}|T]) ->
	simplexml(Bin, [{Name,[],Val}|T]);
simplexml(X,[[{_,_}|_] = H|T]) ->
	simplexml(simplexml(X,H),T);
simplexml(X,[[{_,_,_}|_] = H|T]) ->
	simplexml(simplexml(X,H),T);
simplexml(_,[L]) when is_list(L) ->
	xmerl_lib:export_text(L);
simplexml(_, [L]) when is_binary(L) ->
	xmerl_lib:export_text(tolist(L));
simplexml(X,B) when is_binary(B) ->
	simplexml(X,[B]);
simplexml(X,B) when is_integer(B); is_atom(B); is_float(B) ->
	simplexml(X,tolist(B));
simplexml(X,[_|_] = L) ->
	simplexml(X,[L]);
simplexml(B,[]) ->
	B.

prop_to_attr(Bin,{K,V}) ->
	prop_to_attr(Bin,[{K,V}]);
prop_to_attr(Bin,[{K,V}|T]) ->
	prop_to_attr([Bin,$\s,tobin(K),"=\"",tobin(V),$\"],T);
prop_to_attr(Bin,[]) ->
	Bin.


%
% 		Parsing OS X plist xmls. Optimized as much as I know how.
% 		Optional second parameter, list of binaries of key names that should be ignored ([<<"Tracks">>,<<"Playlist Items">>]
% 				speeds things up	noticeably).
%
plistxml(B) ->
	plistxml(B,[],[]).
plistxml(B,Terms) ->
	plistxml(B,Terms,[]).
plistxml(Bin,Term,L) ->
	case Bin of
		<<"<",R/binary>> ->
			case R of
				<<"key>",ContentRem/binary>> ->
					[Content,ValRem] = split_first(ContentRem,<<"</key>">>),
					case Term of
						[] ->
							case plistxml(ValRem,Term,[]) of
								{Val,Rem} ->
									plistxml(Rem,Term,[{Content,Val}|L]);
								Val ->
									[{Content,Val}|L]
							end;
						[ignore] ->
							case plistxml(ValRem,[ignore],[]) of
								{_,Rem} ->
									plistxml(Rem,Term,L);
								_ ->
									L
							end;
						_ ->
							case lists:member(Content,Term) of
								true ->
									case plistxml(ValRem,[ignore],[]) of
										{_,Rem} ->
											plistxml(Rem,Term,L);
										Val ->
											[{Content,Val}|L]
									end;
								false ->
									case plistxml(ValRem,Term,[]) of
										{Val,Rem} ->
											plistxml(Rem,Term,[{Content,Val}|L]);
										Val ->
											[{Content,Val}|L]
									end
							end
					end;
				<<"dict>",ContentRem/binary>> ->
					case plistxml(ContentRem,Term,[]) of
						{NL,Rem} ->
							ok;
						NL ->
							Rem = <<>>
					end,
					case Term of
						[ignore] ->
							{[],Rem};
						_ when L == [] ->
							{NL,Rem};
						_ ->
							{NL++L,Rem}
					end;
				<<"array>",ContentRem/binary>> ->
					{NL,Rem} = plist_array(ContentRem,Term,[]),
					case Term of
						[ignore] ->
							{[],Rem};
						_ when L == [] ->
							{NL,Rem};
						_ ->
							{NL++L,Rem}
					end;
				<<"integer>",ContentRem/binary>> ->
					[Content,Next] = split_first(ContentRem,<<"</integer>">>),
					{toint(Content),Next};
				<<"string>",ContentRem/binary>> ->
					[Content,Next] = split_first(ContentRem,<<"</string>">>),
					{Content,Next};
				<<"date>",ContentRem/binary>> ->
					[Content,Next] = split_first(ContentRem,<<"</date>">>),
					{Content,Next};
				<<"data>",ContentRem/binary>> ->
					[Content,Next] = split_first(ContentRem,<<"</data>">>),
					{base64:decode(Content),Next};
				<<"real>",ContentRem/binary>> ->
					[Content,Next] = split_first(ContentRem,<<"</real>">>),
					{tofloat(Content),Next};
				<<"true/>",ContentRem/binary>> ->
					{true,ContentRem};
				<<"false/>",ContentRem/binary>> ->
					{false,ContentRem};
				<<"/dict>",ContentRem/binary>> ->
					{L,ContentRem};
				<<"/plist>",_ContentRem/binary>> ->
					L;
				<<"/array>",ContentRem/binary>> ->
					{L,ContentRem};
				<<"plist",RX/binary>> ->
					[_,ContentRem] = split_first(RX,<<">">>),
					case plistxml(ContentRem,Term,[]) of
						{Val,Rem} ->
							plistxml(Rem,Term,[{<<"plist">>,Val}|L]);
						Val ->
							[{<<"plist">>,Val}|L]
					end;
				<<"?",Rem/binary>> ->
					[_,Next] = split_first(Rem,<<"?>">>),
					plistxml(Next,Term,L);
				<<"!",Rem/binary>> ->
					[_,Next] = split_first(Rem,<<">">>),
					plistxml(Next,Term,L)
			end;
		% <<X,R/binary>> when X == $\r; X == $\n; X == $\s; X == $\t ->
		<<_,R/binary>> ->
			plistxml(R,Term,L);
		<<>> ->
			L
	end.
plist_array(<<X,R/binary>>,Term,L) when X == $\r; X == $\n; X == $\s; X == $\t ->
	plist_array(R,Term,L);
plist_array(<<"</array>",R/binary>>,_Term,L) ->
	{L,R};
plist_array(Bin,[ignore] = Term,L) ->
	case plistxml(Bin,Term,[]) of
		{_,R} ->
			plist_array(R,Term,L);
		NL ->
			{NL,<<>>}
	end;
plist_array(Bin,Term,L) ->
	case plistxml(Bin,Term,[]) of
		{NL,R} ->
			plist_array(R,Term,[NL|L]);
		NL ->
			{NL,<<>>}
	end.

xmlvals({A,B,C},L) ->
	xmlvals([{A,B,C}],L);
xmlvals(L,[X|_] = Xml) when is_tuple(X) ->
	xmlvals(Xml,L);
xmlvals(Xml,L) ->
	F = fun([]) ->
				[];
			([E]) ->
				E;
			(E) ->
				E
		end,
	% [hd(element(3,lists:keyfind(E,1,Xml))) || E <- L].
	[F(element(3,lists:keyfind(X,1,Xml))) || X <- L].

xml2prop({_,_,_} = X) ->
	xml2prop([X]);
xml2prop(X) ->
	xml2prop(X,[]).
xml2prop([{Name,_,[{_,_,_}|_] = SL}|T],L) ->
	xml2prop(T,[{Name,xml2prop(SL)}|L]);
xml2prop([{Name,_,[V]}|T],L) ->
	xml2prop(T,[{Name,unicode:characters_to_binary(V)}|L]);
xml2prop([{Name,_,[]}|T],L) ->
	xml2prop(T,[{Name,[]}|L]);
xml2prop([],L) ->
	L.

% Parse xml binary to simplexml
parsexml(InputXml) ->
	parsexml(InputXml,[]).
parsexml(<<"<?xml version=\"1.0\" encoding=\"Windows-1250\"?>",_/binary>> = I,Opt) ->
	S = "/tmp/" ++ tolist(flatnow()),
	prim_file:write_file(S,I),
	R = os:cmd("iconv -f CP1250 -t UTF-8 " ++ S),
	file:delete(S),
	parsexml(R,Opt);
parsexml(I,O) when is_binary(I) ->
	parsexml(tolist(I),O);
parsexml(InputXml,Opt) ->
	F = fun(B) ->
			{Xml,_} = xmerl_scan:string(B, [{space,normalize},{encoding,"utf-8"},{validation,off}|Opt]),
			strip_whitespace(xmerl_lib:simplify_element(Xml))
		end,
	try F(InputXml) of
		Res ->
			Res
	catch
		error:_X ->
			false
	end.

strip_whitespace({El,Attr,Children}) ->
  NChild = lists:filter(fun(X) ->
    case X of
    " " -> false;
    _   -> true
    end
  end,Children),
  Ch = lists:map(fun(X) -> strip_whitespace(X) end,NChild),
  {El,Attr,Ch};
strip_whitespace(String) ->
	String.


parseform(Bin) ->
	parseform(<<>>,<<>>,Bin,[]).
parseform(K,_,<<"=",X,R/binary>>,L) ->
	parseform(K,<<X>>,R,L);
parseform(K,V,<<"&",R/binary>>,L) ->
	parseform(<<>>,<<>>,R,[{K, V}|L]);
parseform(K,<<>>,<<X,R/binary>>, L) ->
	parseform(<<K/binary,X>>,<<>>,R,L);
parseform(K,V,<<X,R/binary>>, L) ->
	parseform(K,<<V/binary,X>>,R,L);
parseform(<<>>,<<>>,<<>>,L) ->
	L;
parseform(K,V,<<>>,L) ->
	parseform(K,V,<<"&">>,L).


get_os() ->
	case string:tokens(erlang:system_info(system_architecture),"-") of
		[_,"apple","darwin" ++ _] ->
			osx;
		[_,_,"linux",_] ->
			linux;
		["win"++_] ->
			win;
		[_,_,"freebsd"++_] ->
			freebsd
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 						SQL HELPER FUNCTIONS
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Example: butil:sql_select(t_session,[{id,Id},{skey,Key}])
sql_select(T,Cond) when is_tuple(T) ->
	sql_select(element(1,T),Cond);
sql_select(T,Cond) ->
	Sql = sql_select_cmd(T,Cond),
	sql_torec(emysql:execute(get(sqlcon),Sql)).

sql_select_cmd(T,Cond) ->
	case Cond of
		_ when is_tuple(Cond) ->
			["SELECT * FROM ",tolist(T)," WHERE ",query_pair(Cond)];
		[Pair|L] ->
			["SELECT * FROM ",tolist(T)," WHERE ",query_pair(Pair) , [  ["AND ",query_pair(Pair1)]  || Pair1 <- L]]
	end.

query_pair({A,V}) ->
	query_pair({A,"=",V});
query_pair({A,Op,V,multiple}) ->
	query_pair_multiple(V,Op,A,[],length(V)-1);
query_pair({A,Op,V}) ->
	case ok of
		_ when V == null; V == undefined ->
			case Op of
				_ when Op == "<>" orelse Op == "!="  ->
					[sqlescape(butil:tolist(A))," IS NOT NULL "];
				_ ->
					[sqlescape(butil:tolist(A))," IS NULL "]
			end;
		_ ->
			[sqlescape(butil:tolist(A))," ",Op," ",sqlquote(V)," "]
	end;
query_pair(A) when is_binary(A)->
    butil:tolist(A).

query_pair_multiple([],_Op,_A,Output,_Len)->
	Output;
query_pair_multiple([H|T],Op,A,Output,Len)->
	case length(T) of
		0 ->
			Out = ["(",sqlescape(butil:tolist(A))," ",Op," ",sqlquote(H)," OR "];
		Len ->
			Out = [sqlescape(butil:tolist(A))," ",Op," ",sqlquote(H),")"];
		_ ->
			Out = [sqlescape(butil:tolist(A))," ",Op," ",sqlquote(H)," OR "]
	end,
	query_pair_multiple(T,Op,A,[Out|Output],Len).

value_pairs(Data) ->
	case Data of
		[{_,_} = L] ->
			value_pair(L);
		[{_,_} = L|R] ->
			[value_pair(L), [ [",",value_pair(Rx)] || Rx <- R ] ]
	end.

value_pair({C,V}) ->
	case V of
		V when is_integer(V) ->
			[butil:tolist(C), "=", butil:tolist(V)];
		V when is_float(V) ->
			[butil:tolist(C), "=",butil:tolist(V)];
		V when is_list(V); is_binary(V); is_atom(V) ->
			[butil:tolist(C), "=", sqlquote(V)]
	end.

insert_pair({C,D}) ->
	case D of
		D when is_integer(D) ->
			{butil:tolist(C), butil:tolist(D)};
		D when is_float(D) ->
			{butil:tolist(C), butil:tolist(D)};
		D when is_list(D); is_binary(D); is_atom(D) ->
			{butil:tolist(C), sqlquote(D)};
		D when D==undefined ->
			{}
	end.

insert_pairs(D) ->
	insert_pairs(D,"","").

insert_pairs([],Cols,Vals) ->
	{Cols,Vals};
insert_pairs([Vp|T],Cols,Vals) ->
	case insert_pair(Vp) of
		{}->
			insert_pairs(T,Cols,Vals);
		{Cv,Dv} ->
			case Cols of
				"" ->
					AddVal = Dv,
					AddCol = Cv;
				_ ->
					AddVal = [Vals,",", Dv],
					AddCol = [Cols, ",", Cv]
			end,
			insert_pairs(T,AddCol,AddVal)
	end.

sql_createtable(Rec,Constraints) ->
	Cmd = sql_createtable_cmd(Rec,Constraints),
	Cmdbin = iolist_to_binary(Cmd),
	{Cmdbin, emysql:execute(get(sqlcon),Cmdbin)}.

sql_createtable_cmd(Rec,Constraints) ->
	Name = element(1,Rec),
	[["CREATE TABLE ", tobin(Name)," ("],
		sql_createtable_cmd_body(Rec,Constraints),
		   ")"].
sql_createtable_cmd_body(Rec,Constraints) ->
	case Constraints of
		"" ->
			Cons = "";
		_ ->
			Cons = [",",Constraints]
	end,
	RecType = rectypeinfo(Rec),
	L = recfields(Rec),
	F = fun(Field,{Str,Ind}) ->
				case sqltotype(element(Ind,RecType)) of
					undefined ->
						{Str,Ind+1};
					FType ->
						Line = [tobin(Field)," ", FType],
						{[Line|Str],Ind+1}
				end
	end,
	{Str1,_} = lists:foldl(F,{[],2},L),
	[lists:reverse(iolist_join(Str1,$,)),Cons].
% If only record parameter: sql_udpate(#ad{id = 1, name = "abc"})
%  it presumes first element in record is id and it updates on that.
sql_update(Rec) ->
	sql_update_on(2,Rec).
% Example: sql_update_on(#ad.id,#ad{id = 1, name = "abc"})
sql_update_on(Ind,Rec) ->
	% sql_checkrec(Rec),
	Cmd = sql_update_on_cmd(Ind,Rec),
	case emysql:execute(get(sqlcon),Cmd) of
		#ok_packet{} = O ->
			{ok,O#ok_packet.insert_id};
		#error_packet{} = E ->
			{error,iolist_to_binary(Cmd),E}
	end.
sql_update_cmd(Rec) ->
	sql_update_on_cmd(2,Rec).
sql_update_on_cmd(Ind,Rec) ->
	Name = element(1,Rec),
	Fields = recfields(Rec),
	RecType = rectypeinfo(Rec),
	RecDef = recdef(Rec),
	IndName = lists:nth(Ind-1,Fields),
	SetString = rectoset(Fields,setelement(Ind,Rec,undefined),RecDef,RecType,2,[]),
  [<<"UPDATE ">>,tobin(Name),<<" SET ">>,SetString,<<" WHERE ">>,tobin(IndName),$\s,$=,$\s,
				sqlquote(sqltypecheck(element(Ind,RecType),element(Ind,Rec)))].


% Insert on record.
%  Requires that butil:recinfo(#ad{},?ADTYP,record_info(fields,ad)),
%  has been called beforehand (in ?SETUP)
sql_insert(Rec) ->
	% sql_checkrec(Rec),
	Name = element(1,Rec),
	L = recfields(Rec),
	RecType = rectypeinfo(Rec),
	RecDef = recdef(Rec),
	{Names,Vals} = sqlnames(L,Rec,RecDef,RecType,2,[],[]),
	sql_insert([<<"INSERT INTO ">>, tobin(Name),<<" (">>,Names,<<")">>],Vals).

sql_insert_cmd(Rec) ->
	sql_insert_cmd1(<<"INSERT INTO ">>,element(1,Rec),Rec).

sql_insert_cmd(Table,Rec) ->
	sql_insert_cmd1(<<"INSERT INTO ">>,Table,Rec).

sql_insert_replace_cmd(Rec) ->
	sql_insert_cmd1(<<"INSERT OR REPLACE INTO ">>,element(1,Rec),Rec).

sql_insert_cmd1(Insert,Name,Rec) ->
	L = recfields(Rec),
	RecType = rectypeinfo(Rec),
	RecDef = recdef(Rec),
	{Names,Vals} = sqlnames(L,Rec,RecDef,RecType,2,[],[]),
	[Insert, tobin(Name),<<" (">>,Names,<<")">>,<<" VALUES (">>,iolist_join(sparsemap(fun sqlquote/1,Vals),<<",">>),<<")">>].

% Example:
%  butil:sql_insert("INSERT INTO campaigns (name)", [Name]).
sql_insert(C,F) ->
	Str = [C,<<" VALUES (">>,iolist_join(sparsemap(fun sqlquote/1,F),<<",">>),<<")">>],
	% iolist_to_binary(Str).
	case emysql:execute(get(sqlcon),Str) of
		#ok_packet{} = O ->
			{ok,O#ok_packet.insert_id};
		#error_packet{} = E ->
			{error,iolist_to_binary(Str),E}
	end.

% Example:
%   X = emysql:execute("SELECT ...."),
%   butil:sql_toprop(X)
% Output:
%   [{<<"tablename1">>,[{<<"Fieldname">>,Fieldval},...]},
% 	 {<<"tablename2">>,[{<<"name">>,Val}]},...]
% If only one table, just proplist without table name is returned:
%   [{<<"FieldName">>,Val},...]
sql_toprop(#error_packet{} = E) ->
	E;
sql_toprop(#result_packet{rows = []}) ->
	[];
sql_toprop(#result_packet{field_list = FL, rows = RL}) ->
	R = list_to_tuple(hd(RL)),
	F = fun(Field,Rs) ->
			Val = sqlchecktype(Field#field.type,element(Field#field.seq_num-1,R)),
			case lists:keyfind(Field#field.org_table,1,Rs) of
				false ->
					[{Field#field.org_table,[{Field#field.name,Val}]}|Rs];
				{_,Obj} ->
					lists:keystore(Field#field.org_table,1,Rs,
							{Field#field.org_table,[{Field#field.name,Val}|Obj]})

			end
		end,
	case lists:foldl(F,[],FL) of
		[{_,L}] ->
			L;
		L ->
			L
	end.

sql_torec(#error_packet{} = E) ->
	E;
sql_torec(#result_packet{rows = []}) ->
	[];
sql_torec(#result_packet{field_list = FL, rows = RL}) ->
	case [sqlrow(FL,list_to_tuple(R)) || R <- RL] of
		[X] ->
			X;
		L ->
			L
	end.

sqlrow(FL,R) ->
	F = fun(Field,Rs) ->
			Val = sqlchecktype(Field#field.type,element(Field#field.seq_num-1,R)),
			Recname = toatom(Field#field.org_table),
			case lists:keyfind(Recname,1,Rs) of
				false ->
					case get({recdef,Recname}) of
						% No default record with this name, ignore
						undefined ->
							Rs;
						Rec ->
							Fieldindex = indexof(toatom(Field#field.org_name),recfields(Rec)),
							case Fieldindex of
								undefined ->
									% io:format("Unknown field returned from SQL DB ~p~n", [Field#field.org_name]),
									Rs;
								_ ->
									[setelement(Fieldindex+1,Rec,Val)|Rs]
							end
					end;
				Rec ->
					Fieldindex = indexof(toatom(Field#field.org_name),recfields(Rec)),
					case Fieldindex of
						undefined ->
							% io:format("Unknown field returned from SQL DB ~p~n", [Field#field.org_name]),
							Rs;
						_ ->
							lists:keystore(Recname,1,Rs, setelement(Fieldindex+1,Rec,Val))
					end
			end
		end,
	case lists:foldl(F,[],FL) of
		[{_,L}] ->
			L;
		[L] ->
			L;
		L ->
			L
	end.

% Adds 'Val' to string values, parses booleans, whatever is needed to prepare values
%  to be used in SQL statements
sqlquote(X) when is_integer(X) ->
	tobin(X);
sqlquote(X) when is_list(X) ->
	[$',sqlescape(X),$'];
sqlquote(<<_/binary>> = X) ->
	[$',sqlescape(X),$'];
sqlquote(true) ->
	sqlquote(1);
sqlquote(false) ->
	sqlquote(0);
sqlquote(undefined) ->
	<<"NULL">>;
sqlquote(null) ->
	<<"NULL">>;
sqlquote(X) when is_atom(X) ->
	[$',sqlescape(tobin(X)),$'];
sqlquote({Y,M,D}) when Y > 1000 ->
	[$',string:right(tolist(Y), 4, $0),$-,string:right(tolist(M), 2, $0),$-,string:right(tolist(D), 2, $0),$'];
sqlquote({H,M,S}) ->
	[$',string:right(tolist(H), 2, $0),$:,string:right(tolist(M), 2, $0),$:,string:right(tolist(S), 2, $0),$'];
sqlquote({{Y,M,D}, {H,MI,S}}) ->
	[$',string:right(tolist(Y), 4, $0),$-,string:right(tolist(M), 2, $0),$-,string:right(tolist(D), 2, $0)," ",
			string:right(tolist(H), 2, $0),$:,string:right(tolist(MI), 2, $0),$:,string:right(tolist(S), 2, $0), $'];
sqlquote(X) when is_float(X) ->
	tobin(X).


% X == $\n; X == $\r;  X == 0; X == 26
-define(ESCCHARS(X), X == $"; X == $'; X == $%; X == $\\; X < $\s).
sqlunescape(X) ->
	re:replace(X, "''", "'", [global, {return, binary}, unicode]).
sqlescape(X) ->
 	re:replace(X, "'", "''", [global, unicode]).

% sqlescape([$\\,X|T],L) when ?ESCCHARS(X) ->
% 	sqlescape(T,[X,$\\|L]);
% sqlescape([X|T],L) when ?ESCCHARS(X) ->
% 	sqlescape(T,[X,$\\|L]);
% sqlescape(<<X,T/binary>>,L) when ?ESCCHARS(X) ->
% 	sqlescape(T,[X,$\\|L]);
% sqlescape(<<X,T/binary>>,L) ->
% 	sqlescape(T,[X|L]);
% sqlescape([X|T],L) ->
% 	sqlescape(T,[X|L]);
% sqlescape(X,L) when X == []; X == <<>> ->
% 	list_to_binary(lists:reverse(L)).

% sqlunescape(L) ->
% 	sqlunescape(L,[]).
% sqlunescape(<<"\\",X,T/binary>>,L) when ?ESCCHARS(X) ->
% 	sqlunescape(T,[X|L]);
% sqlunescape(<<X,T/binary>>,L) ->
% 	sqlunescape(T,[X|L]);
% sqlunescape(<<>>,L) ->
% 	list_to_binary(lists:reverse(L)).

% Checks record field values for validity. If
%   some value has to be integer, try to turn into integer, etc.
% Not used a.t.m., because objtorec also calls sqltypecheck and it will crash
%  there if values are invalid

% sql_checkrec(Rec) ->
% 	RecType = rectypeinfo(Rec),
% 	Size = tuple_size(Rec),
% 	F = fun(F,X) ->
% 			case X < Size of
% 				true ->
% 					sqltypecheck(element(X,RecType), element(X,Rec)),
% 					F(F,X+1);
% 				_ ->
% 					ok
% 			end
% 		end,
% 	F(F,2),
% 	ok.

% From SQL DB to erlang (reverse of sql_quote)
sqlchecktype(?FIELD_TYPE_TINY,Val) ->
	case Val of
		0 ->
			false;
		1 ->
			true;
		_ ->
			Val
	end;
sqlchecktype(_Typ,Val) when is_binary(Val) ->
	tobin(sqlunescape(Val));
sqlchecktype(?FIELD_TYPE_DATETIME,{datetime,D}) ->
	D;
sqlchecktype(?FIELD_TYPE_DATE,{date,D}) ->
	D;
sqlchecktype(?FIELD_TYPE_TIME,{time,D}) ->
	D;
sqlchecktype(_Typ,Val) ->
	Val.

sqltotype(ignore) ->
	undefined;
sqltotype(X) when is_atom(X) ->
	sqltotype({X,undefined});
sqltotype({serial,_}) ->
	"SERIAL";
sqltotype({integer,Def}) ->
	["INTEGER ",sqldef(Def)];
sqltotype({uinteger,Def}) ->
	["INTEGER ",sqldef(Def)];
sqltotype({varchar,Max}) ->
	["VARCHAR(",tolist(Max),")"];
sqltotype({varchar,Max,Def}) ->
	["VARCHAR(",tolist(Max),") ",sqldef(Def)];
sqltotype({boolean,Def}) ->
	["BOOLEAN ",sqldef(Def)];
sqltotype({bool,Def}) ->
	["BOOLEAN ",sqldef(Def)];
sqltotype({ubigint,Def}) ->
	["BIGINT UNSIGNED ",sqldef(Def)];
sqltotype({bigint,Def}) ->
	["BIGINT ",sqldef(Def)];
sqltotype({float,Def}) ->
	["FLOAT ",sqldef(Def)];
sqltotype({datetime,Def}) ->
	["DATETIME ",sqldef(Def)];
sqltotype({date,Def}) ->
	["DATE ",sqldef(Def)];
sqltotype({text,Def}) ->
	["TEXT ",sqldef(Def)];
sqltotype({time,Def}) ->
	["TIME ",sqldef(Def)];
sqltotype({blob,Def}) ->
	["BLOB ",sqldef(Def)];
% sqltotype({password,undefined}) ->
% 	sqltotype({varchar,32});
sqltotype({Name,Max}) when Name == url ->
	sqltotype({varchar,Max});
sqltotype({Name,Max,Def}) when Name == url ->
	sqltotype({varchar,Max,Def}).

sqldef(undefined) ->
	"";
sqldef(notnull) ->
	"NOT NULL";
sqldef(not_null) ->
	"NOT NULL";
sqldef(false) ->
	"DEFAULT FALSE";
sqldef(true) ->
	"DEFAULT TRUE";
sqldef(X) when is_integer(X) ->
	["DEFAULT ",tobin(X)];
sqldef(X) when is_binary(X); is_list(X) ->
	["DEFAULT '",X,"'"].

sqltypedef(serial) ->
	undefined;
sqltypedef(X) when is_atom(X) ->
	sqltypedef({X,undefined});
sqltypedef({integer,Def}) ->
	Def;
sqltypedef({uinteger,Def}) ->
	Def;
sqltypedef({varchar,_Max}) ->
	undefined;
sqltypedef({varchar,_Max,Def}) ->
	Def;
sqltypedef({text,_Max,Def}) ->
	Def;
sqltypedef({text,Def}) ->
	Def;
sqltypedef({boolean,Def}) ->
	Def;
sqltypedef({bool,Def}) ->
	Def;
sqltypedef({ubigint,Def}) ->
	Def;
sqltypedef({bigint,Def}) ->
	Def;
sqltypedef({float,Def}) ->
	Def;
sqltypedef({datetime,Def}) ->
	Def;
sqltypedef({date,Def}) ->
	Def;
sqltypedef({time,Def}) ->
	Def;
sqltypedef({blob,Def}) ->
	Def;
sqltypedef({Name,_Max}) when Name == url; Name == password ->
	undefined;
sqltypedef({Name,_Max,Def}) when Name == url; Name == password ->
	tobin(Def).

% If something is suppose to be of a certain type, convert it to this type
sqltypecheck(ignore,_) ->
	ok;
sqltypecheck(X,V) when is_atom(X) ->
	sqltypecheck({X,undefined},V);
sqltypecheck({integer,_},V) ->
	toint(V);
sqltypecheck({uinteger,_Def},V) ->
	toint(V);
sqltypecheck({varchar,_Max},V) ->
	V;
sqltypecheck({varchar,_Max,_Def},V) ->
	V;
sqltypecheck({text,_},V) ->
	V;
sqltypecheck({text,_,_},V) ->
	V;
sqltypecheck({boolean,_Def},V) ->
	parsebool(V);
sqltypecheck({bool,_Def},V) ->
	parsebool(V);
sqltypecheck({ubigint,_Def},V) ->
	toint(V);
sqltypecheck({bigint,_Def},V) ->
	toint(V);
sqltypecheck({float,_Def},V) ->
	tofloat(V);
sqltypecheck({date,_Dev},V) ->
	[Year,M,D] = split(tobin(V),<<"-">>),
	{toint(Year),toint(M),toint(D)};
sqltypecheck({time,_Dev},V) ->
	[H,M,S] = split(tobin(V),<<":">>),
	{toint(H),toint(M),toint(S)};
sqltypecheck({datetime,_Dev},V) ->
	[Date,Time] = split(tobin(V),<<" ">>),
	{sqltypecheck(date,Date),sqltypecheck(time,Time)};
sqltypecheck({Name,_Max},V) when Name == url ->
	url(V),
	V;
sqltypecheck({Name,_Max,_Def},V) when Name == url ->
	url(V),
	V;
sqltypecheck(_,V) ->
	V.

% Go through record keynames and read accompanying values
%  return string for keys and string for values: {(column1,column2,...), (value1,value2,...)}
sqlnames([Name|Keys],Rec,RecDef,RecType,Ind,L,LV) ->
	case element(Ind,RecType) of
		ignore ->
			sqlnames(Keys,Rec,RecDef,RecType,Ind+1,L,LV);
		_KeyType ->
			Defval = element(Ind,RecDef),
			% Typedef = sqltypedef(KeyType),
			case element(Ind,Rec) of
				undefined ->
					sqlnames(Keys,Rec,RecDef,RecType,Ind+1,L,LV);
				% Record value is same as default, ignore value, unless it is set to notnull in RecType. Throw
				%   exception if notnull.
				% Defval when Typedef ->
				% 	case Typedef of
				% 		notnull ->
				% 			throw({sqlinsert,notnull_value_not_set,{element(1,Rec),Name}});
				% 		_ ->
				% 			sqlnames(Keys,Rec,RecDef,RecType,Ind+1,L,LV)
				% 	end;
				% Typedef ->
				% 	sqlnames(Keys,Rec,RecDef,RecType,Ind+1,L,LV);
				Val ->
					sqlnames(Keys,Rec,RecDef,RecType,Ind+1,[$,,tobin(Name)|L],[sqltypecheck(Defval,Val)|LV])
			end
	end;
sqlnames([],_,_,_,_,[_|Tl],LV) ->
	{lists:reverse(Tl),lists:reverse(LV)};
sqlnames([],_,_,_,_,[],[]) ->
	{[],[]}.

% Returns iolist string for "key = value" from record
rectoset([Name|Keys],Rec,RecDef,RecType,Ind,L) ->
	case element(Ind,RecType) of
		ignore ->
			rectoset(Keys,Rec,RecDef,RecType,Ind+1,L);
		_KeyType ->
			% Defval = element(Ind,RecDef),
			% Typedef = sqltypedef(KeyType),
			case element(Ind,Rec) of
				undefined ->
					rectoset(Keys,Rec,RecDef,RecType,Ind+1,L);
				% Defval ->
				% 	rectoset(Keys,Rec,RecDef,RecType,Ind+1,L);
				% Typedef ->
				% 	rectoset(Keys,Rec,RecDef,RecType,Ind+1,L);
				Val ->
					rectoset(Keys,Rec,RecDef,RecType,Ind+1,[$,,[tobin(Name),$\s,$=,$\s,sqlquote(Val)]|L])
			end
	end;
rectoset([],_,_,_,_,[_|L]) ->
	L.

proptoset(Prop) ->
	proptoset(Prop,[]).
proptoset({Name,Val},L) ->
	proptoset([{Name,Val}],L);
proptoset([{Name,Val}|T],L) ->
	case T of
		[] ->
			Colon = [];
		_ ->
			Colon = $,
	end,
	proptoset(T,[Colon,$\s,[tobin(Name),$=,sqlquote(Val)]|L]);
proptoset([],L) ->
	L.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 						TYPE/FORMAT CONVERSION
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
encode_base64(V) ->
	mochiweb_util:quote_plus(base64:encode(V)).
decode_base64(V) ->
	base64:decode(mochiweb_util:unquote(V)).

decode_percent(<<_/binary>> = B) ->
	decode_percent([],B);
decode_percent(L) ->
	decode_percent([],L).

decode_percent(Bin,<<"%",A,B,R/binary>>) ->
	decode_percent([(dec0(A)*16 + dec0(B))|Bin], R);
decode_percent(Bin,<<"#",_,_,";",R/binary>>) ->
	decode_percent(Bin, R);
decode_percent(B,<<"+",R/binary>>) ->
	decode_percent([$\s|B],R);
decode_percent(B,<<X,R/binary>>) ->
	decode_percent([X|B],R);
decode_percent(B,<<>>) ->
	iolist_to_binary(lists:reverse(B));
decode_percent(L,[$%,$u,A,B,C,D|T]) ->
	{ok, [NA],_} = io_lib:fread("~32u", [A,B,C,D]),
	decode_percent([NA|L], T);
decode_percent(L,[$%,A,B|T]) ->
	{ok, [NA],_} = io_lib:fread("~16u", [A,B]),
	decode_percent([NA|L], T);
% decode_percent(L,[$+|T]) ->
% 	decode_percent([$\s|L], T);
decode_percent(L,[C|T]) ->
	decode_percent([C|L], T);
decode_percent(L,[]) ->
	lists:reverse(L).

-define(PERCENT, 37).  % $\%
-define(FULLSTOP, 46). % $\.
-define(QS_SAFE(C), ((C >= $a andalso C =< $z) orelse
                     (C >= $A andalso C =< $Z) orelse
                     (C >= $0 andalso C =< $9) orelse
                     (C =:= ?FULLSTOP orelse C =:= $- orelse C =:= $~ orelse
                      C =:= $_))).
hexdigit(C) when C < 10 -> $0 + C;
hexdigit(C) when C < 16 -> $A + (C - 10).

encode_percent(Str) ->
	encode_percent(tolist(Str),noplus,[]).
encode_percent(Str,Type) when Type == plus; Type == noplus; Type == notwice ->
	encode_percent(tolist(Str),Type,[]).
encode_percent([], _, Acc) ->
    lists:reverse(Acc);
encode_percent([C | Rest],T, Acc) when ?QS_SAFE(C) ->
    encode_percent(Rest,T, [C | Acc]);
% encode_percent([$\s | Rest], T, Acc) ->
%     encode_percent(Rest, T, [$+ | Acc]);
encode_percent([$%,A,B|Rest],T,Acc) ->
	encode_percent(Rest,T,[B,A,$%|Acc]);
encode_percent([C | Rest], T, Acc) when C =< 255 ->
	<<Hi:4, Lo:4>> = <<C>>,
	encode_percent(Rest, T, [hexdigit(Lo), hexdigit(Hi), ?PERCENT | Acc]);
encode_percent([C|Rest],T,Acc) when C > 255 ->
	<<Hi1:4, Lo1:4,Hi2:4, Lo2:4>> = <<C:16>>,
	encode_percent(Rest, T, [hexdigit(Lo2), hexdigit(Hi2), ?PERCENT,hexdigit(Lo1), hexdigit(Hi1), ?PERCENT | Acc]).

encode_whitespace_percent(Str)->
	encode_whitespace_percent(Str,[]).
encode_whitespace_percent([H|T],Acc) when H =:= 32 ->
	<<Hi:4, Lo:4>> = <<H>>,
	encode_whitespace_percent(T, [hexdigit(Lo), hexdigit(Hi), ?PERCENT | Acc]);
encode_whitespace_percent([H|T],Acc) ->
	encode_whitespace_percent(T, [H | Acc]);
encode_whitespace_percent([],Acc) ->
	lists:reverse(Acc).

prop_to_query({A,B}) ->
	prop_to_query([{A,B}]);
prop_to_query(Props) ->
    Pairs = lists:foldr(
              fun ({K, V}, Acc) ->
					  case V of
						undefined ->
							Acc;
						"" ->
							Acc;
						<<>> ->
							Acc;
						_ ->
							[encode_percent(K) ++ "=" ++ encode_percent(V) | Acc]
						end
              end, [], Props),
    string:join(Pairs, "&").

split_first(B,Split) ->
	binary:split(B,Split).

split(B,Split) ->
	binary:split(B,Split,[global]).

% Shorten is for a short string representation of integers.
% butil:shorten(1000) -> "qi"
% butil:short_toint("qi") -> 1000
-define(ALPHABET,<<"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz">>).

shorten(0) ->
	alphabetchar(0);
shorten(Int) ->
	shorten(Int,[]).
shorten(0,L) ->
	L;
shorten(Int,L) ->
	shorten(Int div byte_size(?ALPHABET),[alphabetchar(Int rem byte_size(?ALPHABET))|L]).

short_toint(Str) ->
	short_toint(0,Str).
short_toint(Int,[H|T]) ->
	short_toint(Int*62 + char_toint(H),T);
short_toint(I,[]) ->
	I.

alphabetchar(X) ->
	binary:at(?ALPHABET,X).
char_toint(X) when X >= $a, X =< $z ->
	X - $a;
char_toint(X) when X >= $A, X =< $Z ->
	X - $A + ($z - $a + 1);
char_toint(X) when X >= $0, X =< $9 ->
	X - $0 + ($z - $a + 1)*2.
% testalphabet() ->
% 	[X = alphabetchar(char_toint(X)) || <<X:8>> <= ?ALPHABET].
% testshort(0) ->
% 	ok;
% testshort(X) ->
% 	X = short_toint(shorten(X)),
% 	testshort(X-1).

int2hex(N) when N =< 16#FF ->
	dec2hex(<<N>>);
int2hex(N) when N =< 16#FFFF ->
	dec2hex(<<N:16>>);
int2hex(N) when N =< 16#FFFFFFFF ->
	dec2hex(<<N:32>>);
int2hex(N) ->
	dec2hex(<<N:64>>).

dec2hex(<<_/binary>> = Bin) ->
    <<<<(hex0(H)), (hex0(L))>> || <<H:4, L:4>> <= Bin>>;
dec2hex(L) ->
	dec2hex(tobin(L)).

hex2dec(<<_/binary>> = Bin) ->
	<<<<(dec0(H)):4, (dec0(L)):4>> || <<H:8, L:8>> <= Bin>>;
hex2dec(L) ->
	hex2dec(tobin(L)).

dec0(X) when X =< $Z, X >= $A ->
	dec0(X+32);
dec0($a) ->	10;
dec0($b) ->	11;
dec0($c) ->	12;
dec0($d) ->	13;
dec0($e) ->	14;
dec0($f) ->	15;
dec0(X) ->	X - $0.

hex0(10) -> $a;
hex0(11) -> $b;
hex0(12) -> $c;
hex0(13) -> $d;
hex0(14) -> $e;
hex0(15) -> $f;
hex0(I) ->  $0 + I.

acc(Fun,Acc,[H|T]) ->
	acc(Fun,Fun(H,Acc),T);
acc(_,Acc,[]) ->
	Acc.

size(<<_/binary>> = B) ->
	byte_size(B);
size([_|_] = L) ->
	length(L);
size(_) ->
	0.

indexof(Val,L) ->
	indexof(Val,L,1).
indexof(Val,[Val|_T],Ind) ->
	Ind;
indexof(V,[_|T],I) ->
	indexof(V,T,I+1);
indexof(_,[],_) ->
	undefined.

mapfind(K,V,[H|L]) ->
	case maps:get(K,H) of
		V ->
			H;
		_ ->
			mapfind(K,V,L)
	end;
mapfind(_,_,[]) ->
	false.

mapstore(Key,Val,[H|L],Map) ->
	case maps:get(Key,H) of
		Val ->
			[Map|L];
		_ ->
			[H|mapstore(Key,Val,L,Map)]
	end;
mapstore(_,_,[],Map) ->
	[Map].

maplistsort(Key,L) ->
	maplistsort(Key,L,asc).
maplistsort(Key,L,asc) ->
	Asc = fun(A,B) -> maps:get(Key,A) =< maps:get(Key,B) end,
	lists:sort(Asc,L);
maplistsort(Key,L,desc) ->
	Desc = fun(A,B) -> maps:get(Key,A) >= maps:get(Key,B) end,
	lists:sort(Desc,L).


% Returns first result of Fun while traversing list, that is not undefined or false
find(Fun,[H|T]) ->
	case Fun(H) of
		undefined ->
			find(Fun,T);
		false ->
			find(Fun,T);
		X ->
			X
	end;
find(Fun,<<F:8,Rem/binary>>) ->
	case Fun(F) of
		undefined ->
			find(Fun,Rem);
		false ->
			find(Fun,Rem);
		X ->
			X
	end;
find(_,<<>>) ->
	false;
find(_,[]) ->
	false.

% Returns item in list, for which Fun(Item) returns true
findtrue(Fun,[H|T]) ->
	case Fun(H) of
		true ->
			H;
		_ ->
			findtrue(Fun,T)
	end;
findtrue(_,[]) ->
	false.

replacetrue(Fun,Val,L) ->
	replacetrue(Fun,Val,L,[]).
replacetrue(Fun,Val,[H|T],L) ->
	case Fun(H) of
		true ->
			replacetrue(Fun,Val,T,[Val|L]);
		_ ->
			replacetrue(Fun,Val,T,[H|L])
	end;
replacetrue(_,_,[],L) ->
	lists:reverse(L).

replace(X,Val,L) ->
	replace(X,Val,L,[]).
replace(X,Val,[H|T],L) ->
	case X == H of
		true ->
			replace(X,Val,T,[Val|L]);
		_ ->
			replace(X,Val,T,[H|L])
	end;
replace(_,_,[],L) ->
	lists:reverse(L).

findres(Fun,Res,[H|T]) ->
	case Fun(H) of
		Res ->
			H;
		_ ->
			findres(Fun,Res,T)
	end;
findres(_,_,[]) ->
	undefined.


store(X,L) ->
	case lists:member(X,L) of
		true ->
			L;
		false ->
			[X|L]
	end.


% Object is any data structure supported by ds_val.
% Goes through list of objects to find one that has {Key,Val} and replaces it.
% If none found, it adds object to list
% Returns new list of objects.
storeobj({Key,Val},Obj,L) ->
	storeobj(Key,Val,Obj,L,[]).
storeobj(Key,Val,Obj,L) ->
	storeobj(Key,Val,Obj,L,[]).
storeobj(Key,Val,Obj,[H|T],L) ->
	case ds_val(Key,H) of
		Val ->
			T ++ [Obj|L];
		_ ->
			storeobj(Key,Val,Obj,T,[H|L])
	end;
storeobj(_,_,Obj,[],L) ->
	[Obj|L].

findobj(Key,Val,[H|T]) ->
	case ds_val(Key,H) of
		Val ->
			H;
		_ ->
			findobj(Key,Val,T)
	end;
findobj(_,_,[]) ->
	false.

remobj(Key,Val,L) ->
	remobj(Key,Val,L,[]).
remobj(Key,Val,[H|T],L) ->
	case ds_val(Key,H) of
		Val ->
			T ++ L;
		_ ->
			remobj(Key,Val,T,[H|L])
	end;
remobj(_,_,[],L) ->
	L.

extrobj(Key,Val,L) ->
	extrobj(Key,Val,L,[]).
extrobj(Key,Val,[H|T],L) ->
	case ds_val(Key,H) of
		Val ->
			{H,T ++ L};
		_ ->
			extrobj(Key,Val,T,[H|L])
	end;
extrobj(_,_,[],_) ->
	false.

objtorec(A,Rec) ->
	Def = recdef(Rec),
	Elements = recfields(Rec),
	RecTypes = rectypeinfo(Rec),
	objtorec(A,Elements,tl(tuple_to_list(RecTypes)),Def,2).
objtorec(A,[H|T],[TH|TT],R,Ind) ->
	case ds_val(tolist(H),A) of
		undefined ->
			objtorec(A,T,TT,R,Ind+1);
		"" ->
			objtorec(A,T,TT,R,Ind+1);
		Val ->
			objtorec(A,T,TT,setelement(Ind,R,sqlescape(sqltypecheck(TH,Val))),Ind+1)
	end;
objtorec(_,[],[],R,_) ->
	R.


matchany([V|_],V) ->
	true;
matchany([_|T],V) ->
	matchany(T,V);
matchany([],_) ->
	false.


% Picks element from list L, depending on md5 of val.
% For any given value, the picked element will always be the same and for different
%  values picked elements will be well spread out. This is generally used for any kind
%  of load balancing.
% L needs to have less or equal than 65535 elements.
consistent_hashing(Val,L) ->
	consistent_hashing(Val,L,0).
consistent_hashing(Val,L,Pos) when is_binary(Val) ->
	<<ValInt:16>> = binary:part(erlang:md5(Val),Pos,2),
	LS = lists:sort(L),
	Len = length(LS),
	Step = 16#ffff div Len,
	Index = ValInt div Step,
	lists:nth(min(Index+1,Len),LS);
consistent_hashing(Val,L,Pos) ->
	consistent_hashing(term_to_binary(Val),L,Pos).

% Sorts L acording to consistent hashing. Sort will be random, but consistent for the same value.
consistent_hashing_sort(Val,L) ->
	consistent_hashing_sort(Val,L,1,[]).
consistent_hashing_sort(Val,L,N,L1) when N > 18 ->
	consistent_hashing_sort(Val,L,0,L1);
consistent_hashing_sort(Val,[_|_] = L,N, Outl) ->
	Element = consistent_hashing(Val,L,N),
	consistent_hashing_sort(Val,lists:delete(Element,L),N+2,[Element|Outl]);
consistent_hashing_sort(_,[],_,Outl) ->
	Outl.

% Takes a list of nodes, a list of interval points (generaly len(Nodes) < len(Int)) and
%   spreads the interval over nodes (by hashing, sorting and then dividing). If you add or remove
%   a node, the segments will not rearange completely but it will rearange proportionaly, so it's not completely optimal
%   if it's for load balancing of db or cache but good enough for a number of purposes.
% butil:segment_interval([a,b,c],[1,2,3,4,5,6,7,8,9,10,11,12,14]).
% > [{b,[9,10,11,12,14]},{c,[5,6,7,8]},{a,[1,2,3,4]}]
% butil:segment_interval([a,b,c,d],[1,2,3,4,5,6,7,8,9,10,11,12,14]).
% > [{b,[10,11,12,14]},{d,[7,8,9]},{c,[4,5,6]},{a,[1,2,3]}]
segment_interval(Nodes,Int) ->
	HN = lists:keysort(1,[{erlang:md5(tobin(N)),N} || N <- Nodes]),
	LenInt = length(Int),
	LenHN = length(HN),
	case LenInt > LenHN of
		true ->
			Chunk = LenInt div LenHN;
		false ->
			Chunk = 1
	end,
	zipsegm(HN,Chunk,Int,[]).
zipsegm([{_,Node}],_,Int,L) ->
	[{Node,Int}|L];
zipsegm([{_,Node}|NT],Chunk,[],L) ->
	zipsegm(NT,Chunk,[],[{Node,[]}|L]);
zipsegm([{_,Node}|NT],Chunk,Int,L) ->
	{This,Rem} = lists:split(Chunk,Int),
	zipsegm(NT,Chunk,Rem,[{Node,This}|L]).


booltoint(true)	 ->
	1;
booltoint(false) ->
	0;
booltoint(X) when is_integer(X) ->
	X.
inttobool(0) ->
	false;
inttobool(1) ->
	true;
inttobool(X) when is_atom(X) ->
	X.

parsebool(true) ->
	true;
parsebool(false) ->
	false;
parsebool(<<"true">>) ->
	true;
parsebool(<<"false">>) ->
	false;
parsebool(X) when is_list(X) ->
	case string:to_lower(X) of
		"true" ->
			true;
		_ ->
			false
	end;
parsebool(X) when is_integer(X) ->
	X /= 0;
parsebool(X) ->
	parsebool(tolist(X)).

tolist(<<_/binary>> = P) ->
	binary_to_list(P);
tolist(P) when is_atom(P) ->
	atom_to_list(P);
tolist(P) when is_integer(P) ->
	integer_to_list(P);
tolist(P) when is_float(P) ->
	float_to_list(P,[{decimals,6},compact]);
tolist(P) when is_list(P) ->
	P.

toio(<<_/binary>> = P) ->
	P;
toio([_|_] = P) ->
	P;
toio(P) when is_atom(P) ->
	atom_to_binary(P,latin1);
toio(P) when is_integer(P) ->
	integer_to_binary(P);
toio(P) when is_float(P) ->
	float_to_binary(P,[{decimals,6},compact]).

tobin(<<_/binary>> = P) ->
	P;
tobin(P) when is_list(P) ->
	% list_to_binary(P);
	iolist_to_binary(P);
tobin(P) when is_atom(P) ->
	atom_to_binary(P,latin1);
tobin(P) when is_integer(P) ->
	integer_to_binary(P);
tobin(P) when is_float(P) ->
	float_to_binary(P,[{decimals,6},compact]).

toatom(P) when is_binary(P) ->
	binary_to_atom(P,latin1);
toatom(P) when is_list(P) ->
	list_to_atom(P);
toatom(P) when is_atom(P) ->
	P.
toint(<<_/binary>> = P) ->
	binary_to_integer(P);
toint([_|_] = P) ->
	list_to_integer(P);
toint(P) when is_integer(P) ->
	P;
toint(P) when is_float(P) ->
	erlang:round(P).
tofloat(P) when is_integer(P) ->
	P / 1;
tofloat(P) when is_float(P) ->
	P;
tofloat(P) when is_binary(P) ->
	binary_to_float(P);
tofloat(P) when is_list(P) ->
	Str = string:join(string:tokens(P,","),"."),
	case string:str(Str,".") of
		0 ->
			tofloat(P ++ ".0");
		_ ->
			list_to_float(Str)
	end;
tofloat(P) ->
	list_to_float(tolist(P)).

intersection(A,B) ->
	intersection(A,B,[]).
intersection([H|T],B,L) ->
	case lists:member(H,B) of
		true ->
			intersection(T,B,[H|L]);
		false ->
			intersection(T,B,L)
	end;
intersection([],_,L) ->
	L.

% replicate(3,a) -> [a,a,a]

replicate(X,N) ->
	replicate(N,X,[]).
replicate(0,_,L) ->
	L;
replicate(N,X,L) ->
	replicate(N-1,X,[X|L]).

to_ip({A,B,C,D}) ->
	lists:concat([A,".",B,".",C,".",D]);
to_ip(T) when tuple_size(T) == 8 ->
	inet_parse:ntoa(T);
to_ip(L) when is_integer(L) ->
	int_to_ip(L);
to_ip(L) ->
	tolist(L).

int_to_ip(IP) when is_list(IP); is_binary(IP) ->
	tolist(IP);
int_to_ip(IP) when is_integer(IP) ->
	<<A:8, B:8, C:8, D:8>> = <<IP:32>>,
	lists:concat([A, ".", B, ".",C, ".", D]).

ip_to_tuple(<<_Int:32/big-unsigned>> = IP) ->
	<<A:8/unsigned,B:8/unsigned,C:8/unsigned,D:8/unsigned>> = IP,
	{A,B,C,D};
ip_to_tuple(<<_Int:16/binary>> = IP) ->
	<<A:16/unsigned,B:16/unsigned,C:16/unsigned,D:16/unsigned,
	E:16/unsigned,F:16/unsigned,G:16/unsigned,H:16/unsigned>> = IP,
	{A,B,C,D,E,F,G,H};
ip_to_tuple(IP) when is_list(IP); is_binary(IP) ->
	{ok,O} = inet_parse:address(tolist(IP)),
	O;
ip_to_tuple(IP) when is_integer(IP) ->
	ip_to_tuple(int_to_ip(IP));
ip_to_tuple(IP) when is_tuple(IP) ->
	IP.

ip_is_lan(IP) ->
	case ip_to_tuple(IP) of
		{127,0,0,_} ->
			true;
		{172,B,_,_} when B >= 16, B =< 31 ->
			true;
		{192,168,_,_} ->
			true;
		{10,_,_,_} ->
			true;
		_ ->
			false
	end.


ip_to_int(IP) when is_integer(IP) ->
	IP;
ip_to_int("::ffff:"++IP) ->
	ip_to_int(IP);
ip_to_int(<<"::ffff:",IP/binary>>) ->
	ip_to_int(IP);
ip_to_int("::FFFF:"++IP) ->
	ip_to_int(IP);
ip_to_int(<<"::FFFF:",IP/binary>>) ->
	ip_to_int(IP);
ip_to_int({A,B,C,D}) ->
	<<Int:32/integer-unsigned>> = <<(toint(A)):8/integer-unsigned, (toint(B)):8/integer-unsigned,
					(toint(C)):8/integer-unsigned, (toint(D)):8/integer-unsigned>>,
	Int;
ip_to_int({A,B,C,D,E,F,G,H}) ->
	<<Int:128/integer-unsigned>> = <<(toint(A)):16/integer-unsigned, (toint(B)):16/integer-unsigned,
					(toint(C)):16/integer-unsigned, (toint(D)):16/integer-unsigned,(toint(E)):16/integer-unsigned,
					(toint(F)):16/integer-unsigned,(toint(G)):16/integer-unsigned,(toint(H)):16/integer-unsigned>>,
	Int;
ip_to_int(IP) ->
	case inet_parse:address(tolist(IP)) of
		{ok,{_,_,_,_} = T} ->
			ip_to_int(T);
		{ok,{_,_,_,_,_,_,_,_} = T} ->
			ip_to_int(T);
		X ->
			exit({invalidip,X})
	end.

% case string:tokens(tolist(IP), "., ") of
% 		[A, B, C, D|_] ->
% 			<<Int:32/integer-unsigned>> = <<(toint(A)):8/integer-unsigned, (toint(B)):8/integer-unsigned,
% 	        		(toint(C)):8/integer-unsigned, (toint(D)):8/integer-unsigned>>,
% 			Int;
% 		_ ->
% 			IPS = tolist(IP),
% 			case string:tokens(tolist(IP), ":") of
% 				[_I,_J,_K,_L,_E,_F,_G,_H|_] ->
% 					inet_parse:
% 					ok;
% 				_ ->
% 					exit({unrecognized_ip,IP})
% 			end
% 	end,


async_set_sockopt(ListSock, CliSocket) ->
    true = inet_db:register_socket(CliSocket, inet_tcp),
    case prim_inet:getopts(ListSock, [active, nodelay, keepalive, delay_send, priority, tos]) of
    {ok, Opts} ->
        case prim_inet:setopts(CliSocket, Opts) of
        ok    -> ok;
        Error -> gen_tcp:close(CliSocket), Error
        end;
    Error ->
        gen_tcp:close(CliSocket), Error
    end.

% Pick N servers from different subnets from a list of IPs. If N higher than list of IPs, you just get the list of IPs back.
% Return type is IP in integer
pick_diverse_ip(N,RL) ->
	pick_ips(N,[],group_ips(RL),[]).

pick_ips(0,_,_,Ipl) ->
	Ipl;
pick_ips(N,L,[G|T],Ipl) ->
	case G of
		[H] ->
			pick_ips(N-1,L,T,[H|Ipl]);
		_ ->
			pick_ips(N-1,[tl(G)|L],T,[hd(G)|Ipl])
	end;
pick_ips(N,[_|_] = L,[],Ipl) ->
	pick_ips(N,[],L,Ipl);
pick_ips(_,[],[],Ipl) ->
	Ipl.

% Picks an IP from the list, that is in a different subnet
pick_distant_ip(Ip,L) ->
	find(fun(X) -> case abs(ip_to_int(X) - ip_to_int(Ip)) > 255 of true -> X; false -> false end end,L).

group_ips(RL) ->
	L = sparsemap(fun(H) -> case ip_to_int(H) of 0 -> undefined; X -> X end end, RL),
	% GL is IPs grouped by closeness of IP (list of lists)
	acc(fun(Ip,Acc) -> group_ip(Ip,[],Acc) end,[],L).

% Place IP in group
group_ip(Ip,L,[[First|_] = H|T]) ->
	case abs(Ip-First) < 255 of
		true ->
			[[Ip|H]|L] ++ T;
		false ->
			group_ip(Ip,[H|L],T)
	end;
group_ip(Ip,L,[]) ->
	[[Ip]|L].

% If call keygroup(2,L)
% Convert from:
% L = [{"VTS","03","0.VOB"},
%  {"VTS","03","1.VOB"},
%  {"VTS","04","0.VOB"},
%  {"VTS","04","1.VOB"},
%  {"VTS","05","0.VOB"},
%  {"VTS","05","1.VOB"},
%  {"VTS","06","0.VOB"},
%  {"VTS","06","1.VOB"}]
% To:
% [{"06",[{"VTS","06","1.VOB"},{"VTS","06","0.VOB"}]},
%  {"05",[{"VTS","05","1.VOB"},{"VTS","05","0.VOB"}]},
%  {"04",[{"VTS","04","1.VOB"},{"VTS","04","0.VOB"}]},
%  {"03",[{"VTS","03","1.VOB"},{"VTS","03","0.VOB"}]},
% It groups by Index element of tuples in list
keygroup(Index,L) ->
	keygroup(Index,L,[]).
keygroup(Index,[H|T],L) ->
	case lists:keyfind(element(Index,H),1,L) of
		false ->
			keygroup(Index,T,[{element(Index,H),[H]}|L]);
		{Key,KL} ->
			keygroup(Index,T,[{Key,[H|KL]}|lists:keydelete(Key,1,L)])
	end;
keygroup(_,[],L) ->
	L.

% Like keygroup but with fun instead of element index.
% Fun returns key for every element in L.
group(Fun,L) ->
	group(Fun,L,[]).
group(Fun,[H|T],L) ->
	Key = Fun(H),
	case lists:keyfind(Key,1,L) of
		false ->
			group(Fun,T,[{Key,[H]}|L]);
		{Key,KL} ->
			group(Fun,T,[{Key,[H|KL]}|lists:keydelete(Key,1,L)])
	end;
group(_,[],L) ->
	L.

ceiling(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

md5(X) ->
	dec2hex(erlang:md5(X)).
sha256(X) ->
	dec2hex(crypto:hash(sha256,X)).
uuid(X) ->
	uuid_fromhash(dec2hex(crypto:hash(sha256,X))).
uuid_fromhash(SX) ->
	<<A:8/binary,B:4/binary,C:4/binary,D:4/binary,E:12/binary,_/binary>> = SX,
	<<A/binary,"-",B/binary,"-",C/binary,"-",D/binary,"-",E/binary>>.

% pbkdf2
% 	hash(String, [1,2,3,$.,$a], 2000, 32).
hash(String, Salt, Iterations, OutKeyLen) ->
	NBlocks = ceiling(OutKeyLen / 20),
	<<Res:OutKeyLen/binary, _/binary>> = cipher(1, NBlocks, <<>>, tobin(Salt), tobin(String), Iterations),
	Res.

cipher(BlockIndex, MaxBlock,Out, Salt, String, Iterations) when BlockIndex =< MaxBlock ->
	B = crypto:hmac(sha256,<<Salt/binary, BlockIndex:32>>, String),
	Crypted = xorloop(Iterations, B, B, String),
	cipher(BlockIndex+1, MaxBlock, <<Out/binary, Crypted/binary>>, Salt, String, Iterations);
cipher(_, _, Out, _, _, _) ->
	Out.

xorloop(N, Running, B, String) when N > 1 ->
	NB = crypto:hmac(sha256,B, String),
	xorloop(N-1, xorbinary(Running, NB), NB, String);
xorloop(_, Out,_,_) ->
	Out.

xorbinary(A,B) ->
	<<A1:256>> = A,
	<<B1:256>> = B,
	<<Bin/binary>> = <<(A1 bxor B1):256>>,
	Bin.


% Generic data structure functions for: proplist,dict,gb_tree,set,process dictionary,ets
%  ds_val and ds_vals works also with mochiweb_req and yaws #arg{}
ds_new(What) ->
	case What of
		dict ->
			dict:new();
		gb_tree ->
			gb_trees:empty();
		set ->
			sets:new();
		sets ->
			sets:new();
		proplist ->
			[];
		procdict ->
			procdict;
		maps ->
			maps:new();
		map ->
			maps:new()
	end.

ds_add(K,V,procdict) ->
	put(K,V),
	procdict;
ds_add(K,V,[]) ->
	[{K,V}];
ds_add(K,V,{_,_} = T) ->
	gb_trees:insert(K,V,T);
ds_add(K,V,[{X,_}|_] = L) ->
	[{totype(type(X),K),V}|L];
ds_add(K,V,T) when is_tuple(T) ->
	case element(1,T) of
		dict ->
			dict:store(K,V,T);
		bkdreq ->
			T#bkdreq{params = ds_add(K,V,T#bkdreq.params)}
	end;
ds_add({A,K},V,application) ->
	application:set_env(A,K,V);
ds_add(K,V,M) when is_map(M) ->
	maps:put(K,V,M);
ds_add(K,V,T) when is_integer(T); is_atom(T); is_reference(T) ->
	ets:insert(T,{K,V}),
	T.

ds_add(V,L) when is_list(L) ->
	[V|L];
ds_add(V,T) when is_tuple(T) ->
	case element(1,T) of
		set ->
			sets:add_element(V,T)
	end;
ds_add(V,T) when is_integer(T); is_atom(T); is_reference(T) ->
	ets:insert(T,V),
	T.

ds_rem(K,procdict) ->
	erase(K);
ds_rem(K,{_,_} = T) ->
	gb_trees:delete_any(K,T);
ds_rem(_,[]) ->
	[];
ds_rem(K,[{X,_}|_] = L) when is_list(L) ->
	proplists:delete(totype(type(X),K),L);
ds_rem(K,T) when is_tuple(T) ->
	case element(1,T) of
		dict ->
			dict:erase(K,T);
		set ->
			sets:del_element(K,T);
		bkdreq ->
			T#bkdreq{params = ds_rem(K,T#bkdreq.params)}
	end;
ds_rem(K,M) when is_map(M) ->
	maps:remove(K,M);
ds_rem(K,T) when is_integer(T); is_atom(T); is_reference(T) ->
	ets:delete(T,K),
	T.

ds_val(K,procdict) ->
	get(K);
ds_val(K, {mochiweb_request,_} = A) ->
	pgvar(K,A);
ds_val(K,{_,_} = T) ->
	case gb_trees:lookup(K,T) of
		none ->
			undefined;
		{value,V} ->
			V
	end;
ds_val(_,[]) ->
	undefined;
ds_val(K,[{X,_}|_] = L) ->
	case lists:keyfind(totype(type(X),K),1,L) of
		false ->
			undefined;
		{_,V} ->
			V
	end;
% ds_val(K,[{X,_,_}|_] = L) ->
% 	xmlvals(totype(type(X),K),L);
ds_val(K,T) when is_tuple(T) ->
	case element(1,T) of
		dict ->
			case dict:find(K,T) of
				{ok,V} ->
					V;
				error ->
					undefined
			end;
		bkdreq ->
			case ds_val(K,T#bkdreq.params) of
				undefined ->
					"";
				V ->
					V
			end;
		_ ->
			pgvar(T,K)
	end;
ds_val({A,K},application) ->
	case application:get_env(A,K) of
		undefined ->
			undefined;
		{ok,V} ->
			V
	end;
ds_val(K,M) when is_map(M) ->
	maps:get(K,M,undefined);
ds_val(K,T) when is_integer(T); is_atom(T); is_reference(T) ->
	case ets:lookup(T,K) of
		[{_,V}] ->
			V;
		[Res] ->
			Res;
		_ ->
			undefined
	end.

ds_val(K,T,Def) ->
	case ds_val(K,T) of
		undefined ->
			Def;
		[] when is_tuple(T), element(1,T) /= dict ->
			Def;
		X ->
			X
	end.

ds_size(procdict) ->
	length(get());
ds_size(T) when is_list(T) ->
	length(T);
ds_size(T) when is_tuple(T) ->
	case element(1,T) of
		dict ->
			dict:size(T);
		set ->
			sets:size(T);
		bkdreq ->
			ds_size(T#bkdreq.params);
		_ when tuple_size(T) == 2 ->
			gb_trees:size(T)
	end;
ds_size(M) when is_map(M) ->
	maps:size(M);
ds_size(T) when is_integer(T); is_atom(T); is_reference(T) ->
	ets:info(T,size).

ds_tolist(procdict) ->
	get();
ds_tolist({_,_} = T) ->
	gb_trees:to_list(T);
ds_tolist(T) when is_list(T) ->
	T;
ds_tolist(T) when is_tuple(T) ->
	case element(1,T) of
		dict ->
			dict:to_list(T);
		set ->
			sets:to_list(T);
		bkdreq ->
			ds_tolist(T#bkdreq.params)
	end;
ds_tolist(M) when is_map(M) ->
	maps:to_list(M);
ds_tolist(T) when is_integer(T); is_atom(T); is_reference(T) ->
	ets:tab2list(T).

ds_vals(Keys,DS) ->
	[ds_val(K,DS) || K <- Keys].
ds_vals(Keys,DS,Def) ->
	ds_vals(Keys,DS,Def,[]).
ds_vals([HK|TK],DS,[HD|TD],L) ->
	ds_vals(TK,DS,TD,[ds_val(HK,DS,HD)|L]);
ds_vals([],_,_,L) ->
	lists:reverse(L).

% Does not have to be a flat list.
ds_addvals([[{_,_}|_] = H|T],DS) ->
	ds_addvals(T,ds_addvals(H,DS));
ds_addvals([[]|T],DS) ->
	ds_addvals(T,DS);
ds_addvals([{K,V}|T],DS) ->
	ds_addvals(T,ds_add(K,V,DS));
ds_addvals([H|T],DS) ->
	ds_addvals(T,ds_add(H,DS));
ds_addvals([],DS) ->
	DS.

ds_update(K,V,[_|_] = Obj) ->
	case lists:keyfind(K,1,Obj) of
		false ->
			[{K,V}|Obj];
		_ ->
			lists:keyreplace(K,1,Obj,{K,V})
	end;
ds_update(K,V,Obj) ->
	butil:ds_add(K,V,Obj).
ds_update([X|T], Obj) when is_list(X) ->
	ds_update(T,ds_update(X,Obj));
ds_update([{K,V}|T], Obj) ->
	ds_update(T,ds_update(K,V,Obj));
ds_update([],O) ->
	O.

type(<<_/binary>> = _) ->
	binary;
type(X) when is_list(X) ->
	list;
type(X) when is_integer(X) ->
	integer;
type(X) when is_float(X) ->
	float;
type(X) when is_atom(X) ->
	atom;
type(_) ->
	unknown.
totype(atom,V) ->
	toatom(V);
totype(binary,V) ->
	tobin(V);
totype(list,V) ->
	tolist(V);
totype(integer,V) ->
	toint(V);
totype(float,V) ->
	tofloat(V);
totype(unknown,V) ->
	V.

add_if_missing(X,L) ->
	case lists:member(X,L) of
		false ->
			[X|L];
		true ->
			L
	end.

sparsemap(F,L) ->
	sparsemap(F,L,[]).
sparsemap(F,[H|T],L) ->
	case F(H) of
		undefined ->
			sparsemap(F,T,L);
		X ->
			sparsemap(F,T,[X|L])
	end;
sparsemap(_,[],L) ->
	lists:reverse(L).


bin_chunk(Bin,Size) ->
	bin_chunk(Bin,Size,[]).
bin_chunk(<<>>,_,L) ->
	lists:reverse(L);
bin_chunk(Bin,Size,L) ->
	case Bin of
		<<C:Size/binary,R/binary>> ->
			bin_chunk(R,Size,[C|L]);
		_ ->
			bin_chunk(<<>>, Size, [Bin|L])
	end.

iolist_join([],_) ->
	[];
iolist_join(L,El) ->
	iolist_join(L,El,[]).
iolist_join([H|T],El,L) ->
	iolist_join(T,El,[El,H|L]);
iolist_join([],_,[_|L]) ->
	lists:reverse(L).

binary_join(L,El) ->
	binary_join(L,tobin(El),<<>>).
binary_join([H],_El,L) ->
	<<L/binary,H/binary>>;
binary_join([H|T],El,L) ->
	binary_join(T,El,<<L/binary,H/binary,El/binary>>).

lists_at(Element,L) ->
	lists_at(Element,L,1).
lists_at(E,[E|_],N) ->
	N;
lists_at(E,[_|T],N) ->
	lists_at(E,T,N+1);
lists_at(_,[],_) ->
	undefined.

lists_add(X,L) ->
	case lists:member(X,L) of
		true ->
			L;
		false ->
			[X|L]
	end.

lists_add_all([H|T],L) ->
	lists_add_all(T,lists_add(H,L));
lists_add_all([],L) ->
	L.

lists_split_at(X,L) ->
	lists_split_at(X,L,[]).
lists_split_at(X,[X|T],L) ->
	{lists:reverse(L),T};
lists_split_at(X,[H|T],L) ->
	lists_split_at(X,T,[H|L]);
lists_split_at(_,[],L) ->
	{lists:reverse(L),[]}.

% rpc(Node,{Mod,Func,Args}) ->
% 	rpc(Node,Mod,Func,Args).
% rpc(Node,Mod,Func,Args) ->
% 	{Pid,Mon} = spawn_monitor(fun() -> call_rpc(Node,Mod,Func,Args) end),
% 	receive
% 		{'DOWN',MOn,_,Pid,Result} ->
% 			Result
% 	end.
% call_rpc(Node,Mod,Func,Args) ->
% 	spawn_link(Node,?MODULE,remote_exec_rpc,[Mod,Func,Args]),
% 	% Do nothing
% 	Ref = make_ref(),
% 	receive
% 		Ref ->
% 			ok
% 	end.
% remote_exec_rpc(Mod,Func,Args) ->
% 	exit(apply(Mod,Func,Args)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 						Very simple http client
% 		It supports redirects and can work with or without content-length.
% 		Connections are always closed after received http body (and it uses connection: close header)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
http(Addr) ->
	http(Addr,[],get,<<>>,[]).
http(Addr,Headers) ->
	http(Addr,Headers,get).
http(Addr,Headers,Method) ->
	http(Addr,Headers,Method,<<>>,[]).
http(Addr,Headers,Method,Body) ->
	http(Addr,Headers,Method,Body,[]).
http(Addr,Headers,Method,Body,ConnOpts) ->
	Me = self(),
	PID = spawn(fun() ->  http_ex(Me,Addr,Headers,Method,Body,ConnOpts) end),
	Mon = erlang:monitor(process,PID),
	case erlang:is_process_alive(PID) of
		true ->
			receive
				{'DOWN', Mon, _, PID, Reason} ->
					Reason
			end;
		false ->
			{error,failed}
	end.

% Will execute http and reply with content on the same process.
% If post, you have to set content-length header yourself and function will return {get_body,Param}
%  Return values: {http_headers,Param,Status,HeaderList} |
%									{http_chunk,Param,Bin} |
% 								{http_done,Bin} |
% 								{error,Error} |
% 								{get_body,Param}
% Example:
% Post data:
% uploadfile(Id) ->
% 	X = util:http_callback("http://upload.to/some/place",	[{"Content-Length","100"}],post,<<>>),
% 	uploadfile(0,X).
% uploadfile(N,{get_body,P}) when N > 0, N < 100, N rem 10 == 0 ->
% 	util:http_ex_rec(P,close),
% uploadfile(N,{get_body,P}) when N < 100 ->
% 	X = util:http_ex_rec(P,<<N:8>>),
% 	uploadfile(N+1,X);
% uploadfile(_,{get_body,P}) ->
% 	httpasync(util:http_ex_rec(P)).
%
%  Read response:
% httpasync() ->
% 	X = util:http_callback("http://www.google.com"),
% 	httpasync(X).
% httpasync(http_done) ->
% 	io:format("Httpdone ~p~n", [byte_size(Bin)]);
% httpasync({http_chunk,P,Bin}) ->
% 	io:format("Httpchunk ~p~n", [byte_size(Bin)]),
% 	httpasync(util:http_ex_rec(P));
% httpasync({http_headers,P,S,L}) ->
% 	io:format("Headers ~p ~p~n", [S,L]),
% 	httpasync(util:http_ex_rec(P));
% httpasync({error,X}) ->
% 	io:format("Http error ~p", [X]).

http_callback(Addr) ->
	http_callback(Addr,[],get,<<>>,[]).
http_callback(Addr,Headers) ->
	http_callback(Addr,Headers,get).
http_callback(Addr,Headers,Method) ->
	http_callback(Addr,Headers,Method,<<>>,[]).
http_callback(Addr,Headers,Method,Body) ->
	http_callback(Addr,Headers,Method,Body,[]).
http_callback(Addr,Headers,Method,Body,ConnOpts) ->
	http_ex(undefined,Addr,Headers,Method,Body,[{callback,true}|ConnOpts]).

% Returns PID of process executing HTTP request.
% Once HTTP is done, {'DOWN',Monitor,_,PID,Result} is returned.
http_async(Addr) ->
	http_async(Addr,[],get,<<>>,[]).
http_async(Addr,Headers) ->
	http_async(Addr,Headers,get).
http_async(Addr,Headers,Method) ->
	http_async(Addr,Headers,Method,<<>>,[]).
http_async(Addr,Headers,Method,Body) ->
	http_async(Addr,Headers,Method,Body,[]).
http_async(Addr,Headers,Method,Body,ConnOpts) ->
	Me = self(),
	PID = spawn(fun() ->  http_ex(Me,Addr,Headers,Method,Body,ConnOpts) end),
	_Mon = erlang:monitor(process,PID),
	case erlang:is_process_alive(PID) of
		true ->
			PID;
		false ->
			{error,failed}
	end.

% {Host,Port,Username,Password,Path,IfSsl} = url("http://www.something.com").
url(B1) ->
	case tobin(B1) of
		<<"http://",B/binary>> ->
			Ssl = false;
		<<"https://",B/binary>> ->
			Ssl = true;
    <<"ftp://",B/binary>> ->
      Ssl = ftp;
		B ->
			Ssl = false
	end,
	Domport = fun(DM) ->
				case split_first(DM,<<":">>) of
					[Dom] ->
						case Ssl of
							true ->
								[Dom,443];
							false ->
								[Dom,80];
              ftp ->
                [Dom,21]
						end;
					[Dom,Port] ->
						[Dom,toint(Port)]
				end
			end,
	Userpass = fun(DomainPort,DM) ->
					case split_first(DM,<<"@">>) of
						[_] ->
							DomainPort(DM) ++ [undefined, undefined];
						[Uspw,Rem] ->
							DomainPort(Rem) ++ split_first(Uspw,<<":">>)
					end
			end,
	case split_first(B,<<"/">>) of
		[Dom,Path] ->
			list_to_tuple(Userpass(Domport,Dom) ++ [<<"/",Path/binary>>,Ssl]);
		[Dom] ->
			list_to_tuple(Userpass(Domport,Dom) ++ [<<"/">>,Ssl])
	end.


httphead_body({Host,_Port,Us,Pw,Path,_Ssl},Headers1,Method,Body) ->
	case Method of
		get ->
			Methb = <<"GET ">>;
		post ->
			Methb = <<"POST ">>;
		delete ->
			Methb = <<"DELETE ">>;
		put ->
			Methb = <<"PUT ">>;
		head ->
			Methb = <<"HEAD ">>;
		options ->
			Methb = <<"OPTIONS ">>
	end,
	case Us of
		undefined ->
			Auth = "";
		_ ->
			Auth = ["Authorization: Basic ",base64:encode(iolist_to_binary([Us,":",Pw])),"\r\n"]
	end,
	case Body of
		[{_,_}|_] ->
			Headers = [{<<"Content-type">>,<<"application/x-www-form-urlencoded">>}|Headers1],
			Body1 = iolist_join([[K,"=",V] || {K,V} <- Body],"&");
		Body1 ->
			Headers = Headers1
	end,
	case Method of
		get ->
			Body2 = Body1,
			Contentlen = <<>>;
		_ ->
			case Body1 of
				callback ->
					Body2 = <<>>,
					Contentlen = <<>>;
				{fromfile,Pth} ->
					Body2 = <<>>,
					Contentlen = <<"Content-length: ", (tobin(filelib:file_size(Pth)))/binary,"\r\n">>;
				Body2 ->
					Contentlen = <<"Content-length: ", (tobin(iolist_size(Body1)))/binary,"\r\n">>
			end
	end,
	IOHeaders1 = [[toio(K), ": ", toio(V),<<"\r\n">>] || {K,V} <- Headers, K /= host],
	case lists:keyfind(host,1,Headers) of
		{host,Actualhost} ->
			IOHeaders = [[<<"Host: ">>,Actualhost,<<"\r\n">>]|IOHeaders1];
		_ ->
			IOHeaders = [[<<"Host: ">>,Host,<<"\r\n">>]|IOHeaders1]
	end,
	[Methb,Path,<<" HTTP/1.1\r\n">>,
								IOHeaders,
								Contentlen,
								<<"Date: ">>,httpd_util:rfc1123_date(),<<"\r\n">>,
							Auth,
							% <<"Host: ">>,Host,<<"\r\n">>,
						    <<"Connection: close\r\n">>,<<"\r\n">>,Body2].

-record(httpr,{homeproc,sock, method = get, body = <<>>, recv_timeout = 60000, body_size = 0, headers = [], headersin = [],
			   status = 0, ssl, postbody = <<>>,connopts = [],chunkdest, tofile, host,port,us,pw,
			   tofilename, filesize = 0,callback = false,chunked = 0,contimeout = 3000,fromfile,followredirect = true}).

extract_param([H|T],P,L) ->
	case H of
		{timeout,Timeout} ->
			extract_param(T,P#httpr{contimeout = Timeout},L);
		{recv_timeout,R} ->
			extract_param(T,P#httpr{recv_timeout = R},L);
		{tofile,Tofilename} ->
			case file:open(Tofilename,[binary,raw,write]) of
				{ok,Tofile} ->
					extract_param(T,P#httpr{tofilename = Tofilename,tofile = Tofile},L);
				Err ->
					io:format("Unable to open ~s~n", [Tofilename]),
					exit(Err)
			end;
		{fromfile,File} ->
			extract_param(T,P#httpr{fromfile = File},L);
		{callback,CB} ->
			extract_param(T,P#httpr{callback = CB},L);
		{followredirect,X} ->
			extract_param(T,P#httpr{followredirect = X},L);
		_ ->
			extract_param(T,P,[H|L])
	end;
extract_param([],P,L) ->
	{P,L}.

http_ex(Home,Addr,Headers,Method,Body,ConnOpts) when is_list(Addr); is_binary(Addr) ->
	http_ex(Home,url(Addr),Headers,Method,Body,ConnOpts);
http_ex(Home,{Host,Port,Path},Headers,Method,Body,ConnOpts) ->
	http_ex(Home,{Host,Port,Path,false},Headers,Method,Body,ConnOpts);
http_ex(Home,{Host,Port,Path,Ssl},Headers1,Method,Body,ConnOpts1) ->
	http_ex(Home,{Host,Port,undefined,undefined,Path,Ssl},Headers1,Method,Body,ConnOpts1);
http_ex(Home,{Host,Port,Us,Pw,Path,Ssl},Headers,Method,Body1,ConnOpts1) ->
	case is_pid(Home) of
		true ->
			erlang:monitor(process,Home);
		false ->
			ok
	end,
	{P1,ConnOpts} = extract_param(ConnOpts1,#httpr{},[]),
	case P1#httpr.fromfile of
		undefined when Method == post, P1#httpr.callback ->
			Body = callback;
		undefined ->
			Body = Body1;
		_ ->
			Body = {fromfile,P1#httpr.fromfile}
	end,
	Httpheadbody = httphead_body({Host,Port,Us,Pw,Path,Ssl},Headers,Method,Body),
	P = P1#httpr{homeproc = Home,ssl = Ssl, method = Method, host = Host,port = Port,
			us = Us, pw = Pw,
			headersin = Headers, postbody = Body, connopts = ConnOpts1},
	case Ssl of
		true ->
			case ssl:connect(tolist(Host),toint(Port),[{active,once},{keepalive,true},binary,{packet,http_bin}|ConnOpts],P#httpr.contimeout) of
				{ok,Sock} ->
					case ssl:send(Sock,Httpheadbody) of
						ok when P#httpr.callback, Method == post ->
							{get_body,P#httpr{sock = Sock}};
						ok when P#httpr.callback, P#httpr.fromfile == undefined ->
							http_ex_rec(P#httpr{sock = Sock});
						ok when P#httpr.fromfile == undefined ->
							exit(http_ex_rec(P#httpr{sock = Sock}));
						ok ->
							{ok,Info} = prim_file:read_file_info(P#httpr.fromfile),
							Sendfun = fun(Fun,F) ->
										case file:read(F,1024*1024) of
											{ok,Fileb} ->
												{ok,Info2} = prim_file:read_file_info(P#httpr.fromfile),
												case Info2#file_info.mtime == Info#file_info.mtime of
													true ->
														ok = ssl:send(Sock,Fileb);
													false ->
														exit(filechanged)
												end,
												Fun(Fun,F);
											eof ->
												ok
										end
									end,
							{ok,F} = file:open(P#httpr.fromfile,[binary,read,raw]),
							Sendfun(Sendfun,F),
							case P#httpr.callback of
								true ->
									http_ex_rec(P#httpr{sock = Sock});
								_ ->
									exit(http_ex_rec(P#httpr{sock = Sock}))
							end;
						X when P#httpr.callback ->
							X;
						X ->
							exit(X)
					end;
				X when P#httpr.callback ->
					X;
				X ->
					exit(X)
			end;
		false ->
			case gen_tcp:connect(tolist(Host),toint(Port),[{active,once},{keepalive,true},{packet,http_bin},binary|ConnOpts],P#httpr.contimeout) of
				{ok,Sock} ->
					case gen_tcp:send(Sock,Httpheadbody) of
						ok when P#httpr.callback, Method == post ->
							{get_body,P#httpr{sock = Sock}};
						ok when P#httpr.callback,P#httpr.fromfile == undefined ->
							http_ex_rec(P#httpr{sock = Sock});
						ok when P#httpr.fromfile == undefined ->
							exit(http_ex_rec(P#httpr{sock = Sock}));
							% exit(http_ex_rec(#httpr{ssl = Ssl, sock = Sock,method = Method, recv_timeout = RecvTimeout,
							% 						headersin = Headers, postbody = Body, connopts = ConnOpts1, callback = Callback,
							% 						tofile = Tofile, tofilename = Tofilename}));
						ok ->
							{ok,Info} = prim_file:read_file_info(P#httpr.fromfile),
							Sendfun = fun(Fun,F) ->
										case file:read(F,1024*1024) of
											{ok,Fileb} ->
												{ok,Info2} = prim_file:read_file_info(P#httpr.fromfile),
												case Info2#file_info.mtime == Info#file_info.mtime of
													true ->
														ok = gen_tcp:send(Sock,Fileb);
													false ->
														exit(filechanged)
												end,
												Fun(Fun,F);
											eof ->
												ok
										end
									end,
							{ok,F} = file:open(P#httpr.fromfile,[binary,read,raw]),
							Sendfun(Sendfun,F),
							case P#httpr.callback of
								true ->
									http_ex_rec(P#httpr{sock = Sock});
								_ ->
									exit(http_ex_rec(P#httpr{sock = Sock}))
							end;
						X when P#httpr.callback ->
							X;
						X ->
							exit(X)
					end;
				X when P#httpr.callback ->
					X;
				X ->
					exit(X)
			end
	end.
http_ex_rec(P,close) ->
	case P#httpr.ssl of
		true ->
			ssl:close(P#httpr.sock);
		false ->
			gen_tcp:close(P#httpr.sock)
	end;
http_ex_rec(P,Body) ->
	case P#httpr.ssl of
		true ->
			case ssl:send(P#httpr.sock,Body) of
				ok ->
					{get_body,P};
				E ->
					E
			end;
		false ->
			case gen_tcp:send(P#httpr.sock,Body) of
				ok ->
					{get_body,P};
				E ->
					E
			end
	end.
http_ex_rec(#httpr{callback = done} = _P) ->
	http_done;
http_ex_rec(P) ->
	case P#httpr.ssl of
		true ->
			ssl:setopts(P#httpr.sock, [{active, once}]);
		false ->
			inet:setopts(P#httpr.sock,[{active, once}])
	end,
	Sock = P#httpr.sock,
	Close = fun() ->
		 		case P#httpr.ssl of
					true -> ssl:close(P#httpr.sock);
					false -> gen_tcp:close(P#httpr.sock)
				end
			end,
	receive
		{_,Sock,<<_/binary>>} when P#httpr.status >= 300, P#httpr.status < 400 ->
			Close(),
			case P#httpr.followredirect of
				true ->
					Loc = ds_val('Location',P#httpr.headers),
					http_ex(P#httpr.homeproc,http_redirect(P,Loc),P#httpr.headersin,P#httpr.method,P#httpr.postbody,P#httpr.connopts);
				false ->
					{ok,tolist(P#httpr.status),P#httpr.headers,<<>>}
			end;
		{_,Sock,<<_/binary>> = Bin} ->
			case true of
				_ when  P#httpr.tofile /= undefined ->
					ok = file:write(P#httpr.tofile,Bin),
					Size = P#httpr.filesize + byte_size(Bin),
					case Size >= P#httpr.body_size of
						true when P#httpr.body_size > 0 ->
							{ok,tolist(P#httpr.status),P#httpr.headers,<<>>};
						_ ->
							http_ex_rec(P#httpr{filesize = Size})
					end;
				_ when P#httpr.callback ->
					Size = P#httpr.filesize + byte_size(Bin),
					case Size >= P#httpr.body_size of
						true when P#httpr.body_size > 0 ->
							{http_chunk,P#httpr{callback = done},Bin};
						_ ->
							{http_chunk,P#httpr{filesize = Size},Bin}
					end;
				_ ->
					Body = <<(P#httpr.body)/binary,Bin/binary>>,
					Bodysize = iolist_size(Body),
					case Bodysize < P#httpr.body_size of
						false when P#httpr.body_size > 0 ->
							{ok,tolist(P#httpr.status),P#httpr.headers,Body};
						_ ->
							http_ex_rec(P#httpr{body = Body})
					end
			end;
		{_,Sock,http_eoh} ->
			case P#httpr.ssl of
				true ->
					ssl:setopts(P#httpr.sock,[{packet,0}]);
				false ->
					inet:setopts(P#httpr.sock,[{packet,0}])
			end,
			case P#httpr.callback of
				false ->
					http_ex_rec(P);
				true when P#httpr.status >= 300, P#httpr.status < 400 ->
					http_ex_rec(P);
				_ ->
					{http_headers,P,P#httpr.status,P#httpr.headers}
			end;
		{_,Sock,{http_response,_,Stat,_}} ->
			http_ex_rec(P#httpr{status = Stat});
		{_,Sock,{http_header,_,'Content-Length',_,Val}} ->
			http_ex_rec(P#httpr{headers = [{'Content-Length',Val}|P#httpr.headers], body_size = toint(Val)});
		{_,Sock,{http_header,_,'Transfer-Encoding',_,<<"chunked">>}} ->
			http_ex_rec(P#httpr{headers = [{'Transfer-Encoding',<<"chunked">>}|P#httpr.headers], body_size = 0,
														chunked = P#httpr.chunked+1});
		{_,Sock,{http_header,_,Key,_,Val}} ->
			http_ex_rec(P#httpr{headers = [{Key,Val}|P#httpr.headers]});
		{X,Sock} when X == error; X == tcp_closed; X == ssl_closed ->
			case P#httpr.status of
				Stat when Stat >= 300, Stat < 400 ->
					Close(),
					case P#httpr.followredirect of
						true ->
							{'Location',Loc} = lists:keyfind('Location',1,P#httpr.headers),
							http_ex(P#httpr.homeproc,http_redirect(P,Loc),P#httpr.headersin,P#httpr.method,P#httpr.postbody,P#httpr.connopts);
						false ->
							{ok,tolist(P#httpr.status),P#httpr.headers,<<>>}
					end;
				_ when P#httpr.tofile /= undefined, P#httpr.body_size > 0 ->
					file:delete(P#httpr.tofilename),
					{error,incomplete_receive};
				_ when P#httpr.callback, P#httpr.body_size > 0 ->
					{error,incomplete_receive};
				_ when P#httpr.tofile /= undefined ->
					{ok,tolist(P#httpr.status),P#httpr.headers,<<>>};
				_ when P#httpr.callback ->
					http_done;
				_ ->
					% If server sends chunked, and it is behind nginx, nginx will add another level of chunked.
					% There will be two transfer-encoding: chunked headers, so unchunk as many times as there are
					%  chunked headers.
					Body = loop_chunked(P#httpr.chunked,P#httpr.body),
					{ok,tolist(P#httpr.status),P#httpr.headers,Body}
			end;
		{'DOWN', _Mon, _, PID, _Reason} when P#httpr.homeproc == PID ->
			exit(destination_dead);
		{_,S1,_} when S1 /= Sock ->
			http_ex_rec(P);
		{_,S1} when S1 /= Sock ->
			http_ex_rec(P);
		X ->
			X
		% after P#httpr.recv_timeout ->
		after P#httpr.recv_timeout ->
			{error,timeout_receive}
	end.

http_redirect(_P,"http://"++_ = Pth) ->
	Pth;
http_redirect(_P,"https://"++_ = Pth) ->
	Pth;
http_redirect(P,Pth) ->
	{P#httpr.host,P#httpr.port,P#httpr.us,P#httpr.pw,Pth,P#httpr.ssl}.

loop_chunked(0,Bin) ->
	Bin;
loop_chunked(N,Bin) ->
	loop_chunked(N-1,parse_chunked_bin(Bin)).

parse_chunked_bin(Bin) ->
	parse_chunked_bin(Bin,[]).
parse_chunked_bin(Bin,Chunks) ->
	case split_first(Bin,<<"\r\n">>) of
		[Sizehex|[ChunkBody]] ->
			Size = hexlist_to_integer(Sizehex),
			case Size of
				0 ->
					iolist_to_binary(lists:reverse(Chunks));
				_ ->
					<<Chunk:Size/binary,"\r\n",Rem/binary>> = ChunkBody,
					parse_chunked_bin(Rem,[Chunk|Chunks])
			end;
		_ ->
			Bin
	end.

msToDate(Milliseconds) ->
   BaseDate      = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
   Seconds       = BaseDate + (Milliseconds div 1000),
   { Date,_Time} = calendar:gregorian_seconds_to_datetime(Seconds),
   Date.

is_proplist(List) ->
    is_list(List) andalso
        lists:all(fun({_, _}) -> true;
                     (_)      -> false
                  end,
                  List).
