% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-compile([{parse_transform, lager_transform}]).
-define(DBG(F),lager:debug(F)).
-define(DBG(F,A),lager:debug(F,A)).
-define(INF(F),lager:info(F)).
-define(INF(F,A),lager:info(F,A)).
-define(ERR(F),lager:error(F)).
-define(ERR(F,A),lager:error(F,A)).
