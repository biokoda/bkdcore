% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-ifndef(BKDCORE).
-define(BKDCORE,"").

-include_lib("../include/yaws_api.hrl").
-record(pgr,{cookies = [], headers = [], content = [], status = 200, mime = "text/html"}).
% Used to transmit form error information in cookies. A script sets the cookie for 60s, redirects to page
%  which displays the information. That page kills the cookie once it displays.
% Safer than term_to_binary/binary_to_term - impossible to attack the webserver with too many atoms.
%  docid = error name (name of item for error)
-record(pgerr, {docid, msg, param}).
-record(bkdreq, {path="",params="",headers=[]} ).

-endif.