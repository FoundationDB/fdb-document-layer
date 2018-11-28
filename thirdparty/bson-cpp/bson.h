/** @file bson.h
    BSON classes
*/

/*
 *    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/**
   bo and its helpers

   "BSON" stands for "binary JSON" -- ie a binary way to represent objects that
   would be represented in JSON (plus a few extensions useful for databases &
   other languages).

   http://www.bsonspec.org/
*/

#pragma once

#include "bsonassert.h"

#include "bsontypes.h"
#include "oid.h"
#include "bsonelement.h"
#include "bsonobj.h"
#include "bsonmisc.h"
#include "bsonobjbuilder.h"
#include "bsonobjiterator.h"
#include "bson-inl.h"
#include "bson_db.h"