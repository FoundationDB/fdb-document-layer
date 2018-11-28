#!/usr/bin/python
#
# setup_mongo.py
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
# MongoDB is a registered trademark of MongoDB, Inc.
#

from pymongo import MongoClient, errors
import pymongo
import argparse
import preload_database


def init_replica_set(shard_port, shard_addresses, index):
    repl_set = {}

    try:
        shard = MongoClient(shard_addresses[0], shard_port)
        members = []

        for shard_id in range(len(shard_addresses)):
            members.append({"_id": shard_id, "host": shard_addresses[shard_id] + ":" + str(shard_port)})

        repl_set = {"_id": "rs" + str(index) + ".0", "members": members}
        shard.admin.command('replSetInitiate', repl_set)
        print 'Replica set initialized with: '
        print repl_set

    except errors.OperationFailure as e:
        if 'already initialized' in str(e.message):
            print 'Replica set already initialized, continuing.'
        else:
            raise e

    return repl_set


def add_shard(mongos, replSet):
    try:
        mongos.admin.command('addShard', replSet['_id'] + "/" + replSet['members'][0]['host'])
        print 'Shard added.'
    except errors.OperationFailure as e:
        if 'duplicate key' in str(e.message):
            print 'Shard already added, continuing.'
        elif 'exists in another' in str(e.message):
            print 'Shard already added and enabled for DB, continuing.'
        else:
            raise e


def enable_sharding_on_d_b(mongos, db_name):
    try:
        mongos.admin.command('enableSharding', db_name)
        print 'Sharding enabled on DB.'
    except errors.OperationFailure as e:
        if 'already enabled' in str(e.message):
            print 'Sharding already enabled on DB, continuing.'
        else:
            raise e


def enable_sharding_on_collection(mongos, db_name, collection_name):
    try:
        collection = mongos[db_name][collection_name]
        collection.ensure_index([("_id", pymongo.HASHED)])
        mongos.admin.command('shardCollection', db_name + "." + collection_name, key={"_id": "hashed"})
        print 'Sharded collection.'
    except errors.OperationFailure as e:
        if 'already sharded' in str(e.message):
            print 'Collection already sharded, continuing.'
        else:
            raise e


def group(lst, n):
    for i in range(0, len(lst), n):
        val = lst[i:i + n]
        if len(val) == n:
            yield tuple(val)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--shard-port', type=int, default=27018)
    parser.add_argument('-m', '--mongos-port', type=int, default=27017)
    parser.add_argument('-d', '--db_name', default='test')
    parser.add_argument('-c', '--collection', default='test')
    parser.add_argument('-a', '--addresses', nargs='+', help="shard addresses (also used for mongos)")
    parser.add_argument('-l', '--load-data', default=False, help="Load seed data")
    parser.add_argument('--subnet', help="used to calculate shard addresses")
    parser.add_argument('--address-count', type=int, help="used to calculate shard addresses")
    ns = vars(parser.parse_args())

    shard_port = 27018
    mongos_port = 27017
    db_name = 'test'
    collection_name = 'abc'
    shard_addresses = ns['addresses']

    if ns['addresses'] is None and ns['subnet'] is not None and ns['address_count']:
        shard_addresses = []
        for index in range(1, ns['address_count'] + 1):
            shard_addresses.append(ns['subnet'] + str(index))

    grouped_shard_addresses = list(group(shard_addresses, 3))

    if len(grouped_shard_addresses) > 0:
        mongos = MongoClient(shard_addresses[0], mongos_port)

        for index in range(len(grouped_shard_addresses)):
            replSet = init_replica_set(shard_port, grouped_shard_addresses[index], index)

            print("Giving the replica set a few seconds to initialize...")
            import time

            time.sleep(10)

            add_shard(mongos, replSet)

        enable_sharding_on_d_b(mongos, db_name)
        enable_sharding_on_collection(mongos, db_name, collection_name)

        if (ns['load_data']):
            preload_database.preload_database({
                "host": shard_addresses[0],
                "port": mongos_port,
                "collection": collection_name,
                "number": 1,
                "no_numeric_fieldnames": True,
                "no_nulls": True,
                "big_documents": True
            })
    else:
        print "Incorrected or missing shard addresses - exiting.."
