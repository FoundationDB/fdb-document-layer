#!/bin/bash
#
# buildpkg.sh
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

set -e

umask 0022

PKGFILE=$1
VERSION=$2
RELEASE=$3
FDBDOC_BIN_LOCATION=$4
FDBMONITOR_LOCATION=$5

TEMPDIR=$(mktemp -d -t fdbdoc-pkg)

dos2unix()
{
    tr -d '\r' < $1 > $2
}

mkdir -p -m 0755 $TEMPDIR/usr/local/foundationdb/document
mkdir -p -m 0755 $TEMPDIR/usr/local/etc/foundationdb/document
mkdir -p -m 0755 $TEMPDIR/usr/local/libexec
mkdir -p -m 0755 $TEMPDIR/Library/LaunchDaemons
mkdir -p -m 0700 $TEMPDIR/usr/local/foundationdb/document/logs

install -m 0755 packaging/osx/uninstall-FoundationDB-Document-Layer.sh $TEMPDIR/usr/local/foundationdb/document
dos2unix README.md $TEMPDIR/usr/local/foundationdb/document/README.md
chmod 0644 $TEMPDIR/usr/local/foundationdb/document/README.md

install -m 0644 packaging/osx/document.conf.osx $TEMPDIR/usr/local/etc/foundationdb/document/document.conf.new
install -m 0755 $FDBMONITOR_LOCATION $TEMPDIR/usr/local/libexec/fdbdocmonitor
install -m 0755 $FDBDOC_BIN_LOCATION $TEMPDIR/usr/local/libexec/fdbdoc
install -m 0644 packaging/osx/com.foundationdb.fdbdocmonitor.plist $TEMPDIR/Library/LaunchDaemons

pkgbuild --root $TEMPDIR --identifier fdb-document-layer --version $VERSION.$RELEASE --scripts packaging/osx/scripts fdb-document-layer.pkg

rm -rf $TEMPDIR

productbuild --distribution packaging/osx/Distribution.xml --resources packaging/osx/resources --package-path . $PKGFILE

rm fdb-document-layer.pkg
