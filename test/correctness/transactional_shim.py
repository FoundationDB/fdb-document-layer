#
# transactional_shim.py
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

import inspect
import pymongo
import gen


def maybe_small_batch(func):
    def func_wrapper(*args, **kwargs):
        ret = func(*args, **kwargs)
        if hasattr(ret, 'batch_size') and gen.global_prng.random() < 0.10:
            ret.batch_size(2)
            return ret
        else:
            return ret

    return func_wrapper


TRANSACTIONS_ENABLED = False


def maybe_transactional(func):
    index = inspect.getargspec(func).args.index("collection")

    def func_wrapper(*args, **kwargs):
        coll = args[index]
        db = coll.database

        if isinstance(db, pymongo.database.Database) and \
                gen.generator_options.use_transactions and \
                TRANSACTIONS_ENABLED and \
                gen.global_prng.random() < 0.20:
            committed = False
            db.command("beginTransaction")
            retries = 0
            ret = None
            while not committed:
                try:
                    ret = func(*args, **kwargs)
                    db.command("commitTransaction")
                    committed = True
                except pymongo.errors.OperationFailure as e:
                    # print e.details
                    # Ideally we should check the error type and retry only if it can be retried.
                    retries += 1
                    if retries > 3:
                        db.command("rollbackTransaction")
                        raise e
                    db.command("beginTransaction", retry=True)
            return ret
        else:
            return func(*args, **kwargs)

    return func_wrapper


def generate_function(name):
    @maybe_small_batch
    @maybe_transactional
    def _gen_func(collection, *args, **kwargs):
        return getattr(collection, name)(*args, **kwargs)

    ret = _gen_func

    return ret


for name in ['find', 'remove', 'insert', 'update', 'drop_indexes', 'ensure_index']:
    f = generate_function(name)
    f.__name__ = name
    globals()[name] = f
