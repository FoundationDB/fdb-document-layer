#!/usr/bin/python
#
# preload_database.py
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

# -*- coding: utf-8 -*-

import pymongo
import argparse
import gen
import random
import bson.objectid
import datetime

d = {
    "name":
    "Alcybiades",
    "hats": ["stetson", "porkpie", "kufi", "tuque"],
    "age":
    2464,
    "full name":
    u"Ἀλκιβιάδης Κλεινίου Σκαμβωνίδης",
    "inventory": [{
        "id":
        bson.objectid.ObjectId("5234cc89687ea597eabee675"),
        "code":
        "xyz",
        "tags": ["school", "book", "bag", "headphone", "appliance"],
        "qty": [{
            "size": "S",
            "num": 10,
            "color": "blue"
        }, {
            "size": "M",
            "num": 45,
            "color": "blue"
        }, {
            "size": "L",
            "num": 100,
            "color": "green"
        }]
    },
                  {
                      "id":
                      bson.objectid.ObjectId("5234cc8a687ea597eabee676"),
                      "code":
                      "abc",
                      "tags": ["appliance", "school", "book"],
                      "qty": [{
                          "size": "6",
                          "num": 100,
                          "color": "green"
                      }, {
                          "size": "6",
                          "num": 50,
                          "color": "blue"
                      }, {
                          "size": "8",
                          "num": 100,
                          "color": "brown"
                      }]
                  },
                  {
                      "id":
                      bson.objectid.ObjectId("5234ccb7687ea597eabee677"),
                      "code":
                      "efg",
                      "tags": ["school", "book"],
                      "qty": [{
                          "size": "S",
                          "num": 10,
                          "color": "blue"
                      }, {
                          "size": "M",
                          "num": 100,
                          "color": "blue"
                      }, {
                          "size": "L",
                          "num": 100,
                          "color": "green"
                      }]
                  },
                  {
                      "id": bson.objectid.ObjectId("52350353b2eff1353b349de9"),
                      "code": "ijk",
                      "tags": ["electronics", "school"],
                      "qty": [{
                          "size": "M",
                          "num": 100,
                          "color": "green"
                      }]
                  }],
    "big_field":
    """RDGPHUVHrOkIPSw7WuV7N7P4wHfBv0HiY2tukKs7FdefYupcSmaoXkmahxljtsyFIjzmtMl95szUwPL7CfjFCFmqypqAUXOvJDpTcysw4pDfrJ7r
                     kDT1CjcJ2W9Rw5PzrfyLzjPfUVl9r8oDxKLkTpo01prTZ8kxeDfEuC4tFUvixK45YeDW0WK8mKD68NXxmzvIUdR9mh63Iv5n5fnVQC95eNG0KQm3
                     g0Wb7M6YlbFIagU2OFtoyZ5gj4ZiJ5bTo6uEBo85EY8ucXpBTj4nuR1tebrDmniiR1FFsPRuwADL2tFllkpuruFhjaqwXEG6m6aj6VeZGJmgzRXP
                     oILNYgSl9oz1OkitqUC5x34im8XC39ICpt7EdcWpGjNINEWSEA4arHxUtGFFTGXg75rojPiNPFIFjgzvUlTw7aQXN3YXDWoSAxOJ0QjQQ2oMx44m
                     lW09YwbZT7VJ2KurjJfOiiPS7cufcKOEfr67dCm8DemGmY8BkZrb9tzuJijVRoeUmvSb3cixpL9bbw6oMLcqUkuPVoIk67W0xyPgRwXGrD2jngC1
                     SqX1wa4z211vjCJg40p4DaRvlcmmhNqdksjiBQgxcDuL5E6QMC7zxm0LXJJDP0ZJEtyBDbnuQBtJxQnJWIUtD5ZgIDvUr9LDOQaeOOaxm5N6MIUU
                     uAmvRSMgTdennFotHOzAkpLWhx6e99xad6uG6kSIQY9zzNRL1IlS3VcaxmqpO8S4Tgm7soKk1nUELPZ36zGSAxED3Te1PkLSkpkqfTrxgBVcryjM
                     aWOFiAtIdoc97Y7FXWMq1HVG2U6OVW38NtM4My6t7zMW9CVOTRAoUeURtdeJfCbfkk4xots5KS8b1hOYXiXLAa1rqsVLvwYOdJvzCuzgeMv8WBf0
                     j0goi6kJeuui2yFbePpDdP6ezMjrpBvm9c4Sy2MbaoKIE3veiqNBibxM0D4abMdaXOnxTldE2AJw6HDTJDTPtvgUTw05RHgc5juJCeVtHbRfjp2s
                     izKJnyMifLunRUJyg8sqXYPBzWWuDwHKs7D9SgRay4lBmmOJspTfEvyB7UZ3lQTkAD8Cdlc8Mwk1YXeyaTCHQPkQNdEp2sMXyhXOZGtjkq32goKe
                     hQQuLbPnQhyX1ta5UDJDEnLrZuP38lcLcjhFdIXUnJjmDjxjlD47IR5HJkbw8BnZ4K6IBWPBXz0vUzQLAoB3X56uaPNSyBthMrhEIR78bawr5A8a
                     lV4vQsGx2SHsQeb90DRpwmiRmcKySagg5d2M7WNygEh3FmOXKmBMqz7SWpawAacgLekLV5uCBPwVfzwPEdRKp9kTnau1duidJB9kGE33KsCiGKFt
                     Lx6lQVO4xdTix4PhZ4fnKjLvjxqtJJEb3QynXcNQDv2h9c9cMQuqkvx0JEoBXJJSSfYILVNNRi6jEZ8VzBcQbmgcsvB8ibjtJ78zvOXfzXXWjoHz
                     5EYy83hx39CmSWSw7J6eqPDsMgQKAJuQY26r6l3kdDrA""",
    "buddies": [{
        "id":
        bson.objectid.ObjectId("51e062189c6ae665454e301d"),
        "name": {
            "first": "Dennis",
            "last": "Ritchie"
        },
        "birth":
        datetime.datetime(1941, 9, 9),
        "death":
        datetime.datetime(2011, 10, 12),
        "contribs": ["UNIX", "C"],
        "awards": [{
            "award": "Turing Award",
            "year": 1983,
            "by": "ACM"
        }, {
            "award": "National Medal of Technology",
            "year": 1998,
            "by": "United States"
        }, {
            "award": "Japan Prize",
            "year": 2011,
            "by": "The Japan Prize Foundation"
        }]
    },
                {
                    "id":
                    8,
                    "name": {
                        "first": "Yukihiro",
                        "aka": "Matz",
                        "last": "Matsumoto"
                    },
                    "birth":
                    datetime.datetime(1965, 4, 14),
                    "contribs": ["Ruby"],
                    "awards": [{
                        "award": "Award for the Advancement of Free Software",
                        "year": "2011",
                        "by": "Free Software Foundation"
                    }]
                },
                {
                    "id":
                    6,
                    "name": {
                        "first": "Guido",
                        "last": "van Rossum"
                    },
                    "birth":
                    datetime.datetime(1956, 1, 31),
                    "contribs": ["Python"],
                    "awards": [{
                        "award": "Award for the Advancement of Free Software",
                        "year": 2001,
                        "by": "Free Software Foundation"
                    }, {
                        "award": "NLUUG Award",
                        "year": 2003,
                        "by": "NLUUG"
                    }]
                }]
}


def preload_database(ns):
    gen.generator_options.numeric_fieldnames = ns['no_numeric_fieldnames']
    gen.generator_options.test_nulls = ns['no_nulls']

    instance = random.random()

    client = pymongo.MongoClient(ns['host'], ns['port'])
    if (ns['collection'] == ''):
        collection = client['test']['performance' + str(instance)[2:]]
    else:
        collection = client['test'][ns['collection']]

    collection.remove()

    i = 0
    while i < ns['number']:
        docs = []
        for j in range(0, min(100, ns['number'] - i)):
            i += 1
            if ns['big_documents']:
                doc = d.copy()
            else:
                doc = gen.random_document(False)
                doc["boo"] = i
            doc["_id"] = str(i)
            docs.append(doc)
        collection.insert(docs, safe=False)
        print ("Inserted " + str(i))

    # collection.insert(docs)

    # print [i for i in collection.find()]
    print ("Inserted " + str(ns['number']) + " documents")
    print ("Database: test")
    print ("Collection: " + ('performance' + str(instance)[2:] if ns['collection'] == '' else ns['collection']))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--host', default='localhost')
    parser.add_argument('-p', '--port', type=int, default=27019)
    parser.add_argument('-n', '--number', type=int, default=300)
    parser.add_argument('-c', '--collection', default='')
    parser.add_argument('-b', '--big-documents', default=False, action="store_true")
    parser.add_argument(
        '--no-numeric-fieldnames',
        default=True,
        action="store_false",
        help="disable use of numeric fieldnames in subobjects")
    parser.add_argument('--no-nulls', default=True, action="store_false", help="disable generation of null values")

    ns = vars(parser.parse_args())
    preload_database(ns)
