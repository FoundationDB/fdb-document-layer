#!/bin/bash
#
# builddebs.sh
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

umask 0022

OUTPUT_PATH=$1
VERSION=$2
RELEASE=$3
FDBDOC_BIN_LOCATION=$4
FDBMONITOR_BIN_LOCATION=$5

TEMPDIR=$(mktemp -d)

m4 -DVERSION=$VERSION -DRELEASE=$RELEASE packaging/deb/fdb-document-layer.control.in > packaging/deb/DEBIAN/control
cp -a packaging/deb/DEBIAN $TEMPDIR/DEBIAN
rm packaging/deb/DEBIAN/control
chmod 0755 $TEMPDIR/DEBIAN/{pre,post}*
chmod 0644 $TEMPDIR/DEBIAN/conffiles

mkdir -p -m 0755 $TEMPDIR/etc/foundationdb/document
mkdir -p -m 0755 $TEMPDIR/etc/init.d
mkdir -p -m 0755 $TEMPDIR/usr/sbin
mkdir -p -m 0755 $TEMPDIR/var/log/foundationdb/document
mkdir -p -m 0755 $TEMPDIR/usr/lib/foundationdb/document
mkdir -p -m 0755 $TEMPDIR/usr/share/doc/fdb-document-layer

install -m 0644 packaging/document.conf $TEMPDIR/etc/foundationdb/document/document.conf
install -m 0755 packaging/deb/fdb-document-layer-init $TEMPDIR/etc/init.d/fdb-document-layer
install -m 0755 $FDBDOC_BIN_LOCATION $TEMPDIR/usr/sbin/fdbdoc
install -m 0755 $FDBMONITOR_BIN_LOCATION $TEMPDIR/usr/lib/foundationdb/document/fdbdocmonitor
dos2unix -q -n README.md $TEMPDIR/usr/share/doc/fdb-document-layer/README.md
chmod 0644 $TEMPDIR/usr/share/doc/fdb-document-layer/README.md

echo "Installed-Size:" $(du -sx --exclude DEBIAN $TEMPDIR | awk '{print $1}') >> $TEMPDIR/DEBIAN/control

fakeroot dpkg-deb --build $TEMPDIR $OUTPUT_PATH

rm -r $TEMPDIR
