#!/bin/bash
#
# buildrpms.sh
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

OUTPUT_PATH=$1
VERSION=$2
RELEASE=$3
FDBDOC_BIN_LOCATION=$4
FDBMONITOR_BIN_LOCATION=$5

umask 0022

TEMPDIR=$(mktemp -d)
INSTDIR=$(mktemp -d)

trap "rm -rf $TEMPDIR $INSTDIR" EXIT

mkdir -p $TEMPDIR/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
echo "%_topdir $TEMPDIR" > $TEMPDIR/macros

mkdir -p -m 0755 $INSTDIR/etc/foundationdb/document
mkdir -p -m 0755 $INSTDIR/etc/rc.d/init.d
mkdir -p -m 0755 $INSTDIR/lib/systemd/system
mkdir -p -m 0755 $INSTDIR/usr/sbin
mkdir -p -m 0755 $INSTDIR/var/log/foundationdb/document
mkdir -p -m 0755 $INSTDIR/usr/lib/foundationdb/document
mkdir -p -m 0755 $INSTDIR/usr/share/doc/fdb-document-layer

install -m 0644 packaging/document.conf $INSTDIR/etc/foundationdb/document/document.conf
install -m 0755 packaging/rpm/fdb-document-layer-init $INSTDIR/etc/rc.d/init.d/fdb-document-layer
install -m 0644 packaging/rpm/fdb-document-layer.service $INSTDIR/lib/systemd/system/fdb-document-layer.service
install -m 0755 $FDBDOC_BIN_LOCATION $INSTDIR/usr/sbin/fdbdoc
install -m 0755 $FDBMONITOR_BIN_LOCATION $INSTDIR/usr/lib/foundationdb/document/fdbdocmonitor
dos2unix -q -n README.md $INSTDIR/usr/share/doc/fdb-document-layer/README.md
chmod 0644 $INSTDIR/usr/share/doc/fdb-document-layer/README.md

(cd $INSTDIR ; tar -czf $TEMPDIR/SOURCES/install-files.tar.gz *)

m4 -DBDVERSION=$VERSION -DBDRELEASE=$RELEASE.el6 -DRHEL6 packaging/rpm/fdb-document-layer.spec.in > $TEMPDIR/SPECS/fdb-document-layer.el6.spec
m4 -DBDVERSION=$VERSION -DBDRELEASE=$RELEASE.el7 packaging/rpm/fdb-document-layer.spec.in > $TEMPDIR/SPECS/fdb-document-layer.el7.spec

fakeroot rpmbuild --quiet --define "%_topdir $TEMPDIR" -bb $TEMPDIR/SPECS/fdb-document-layer.el6.spec
fakeroot rpmbuild --quiet --define "%_topdir $TEMPDIR" -bb $TEMPDIR/SPECS/fdb-document-layer.el7.spec

cp $TEMPDIR/RPMS/x86_64/*.rpm $OUTPUT_PATH
