#!/bin/bash -x
#
# uninstall-FoundationDB-Document-Layer.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

rm -f /usr/local/libexec/{fdbdoc,fdbdocmonitor}
rm -f /usr/local/foundationdb/uninstall-FoundationDB-Document-Layer.sh
launchctl unload /Library/LaunchDaemons/com.foundationdb.fdbdocmonitor.plist >/dev/null 2>&1 || :
rm -f /Library/LaunchDaemons/com.foundationdb.fdbdocmonitor.plist
rm -rf /var/db/receipts/fdb-document-layer.*
rm -rf /usr/local/foundationdb/document
rm -rf /usr/local/etc/foundationdb/document

set +x
