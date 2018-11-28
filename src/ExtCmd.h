/*
 * ExtCmd.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _EXT_CMD_H_
#define _EXT_CMD_H_

#pragma once

#include <string>

#include "flow/flow.h"

#include "ExtMsg.h"
#include "ExtStructs.h"
#include "IDispatched.h"

#include "QLContext.h"

struct ExtCmd : IDispatched<ExtCmd,
                            std::string,
                            std::function<Future<Reference<ExtMsgReply>>(Reference<ExtConnection>,
                                                                         Reference<ExtMsgQuery>,
                                                                         Reference<ExtMsgReply>)>> {
	static Future<Reference<ExtMsgReply>> call(std::string const& cmd,
	                                           Reference<ExtConnection> nmc,
	                                           Reference<ExtMsgQuery> query,
	                                           Reference<ExtMsgReply> reply) {
		return dispatch(cmd)(nmc, query, reply);
	}
};

#define REGISTER_CMD(Cmd, Key)                                                                                         \
	const char* Cmd::name = Key;                                                                                       \
	REGISTER_COMMAND(ExtCmd, Cmd, name, call);

Future<Reference<UnboundCollectionContext>> getCollectionContextForCommand(Reference<ExtConnection> ec,
                                                                           Reference<ExtMsgQuery> query,
                                                                           Reference<DocTransaction> dtr,
                                                                           bool allowSystemNamespace);

#endif /* _EXT_CMD_H_ */
