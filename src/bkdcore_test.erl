% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_test).
-include_lib("../include/bkdcore.hrl").
-compile(export_all).

%
%	demo http response module
%

out(A) ->
	% any init code here
	butil:out(A,fun out/2).

out("",_) ->
	#pgr{status=200,content="hello world"};
out(_,_) ->
	#pgr{status=404,content="not found :-)"}.
