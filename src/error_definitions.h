/*
 * error_definitions.h
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
 *
 * MongoDB is a registered trademark of MongoDB, Inc.
 */

#ifdef DOCLAYER_ERROR

DOCLAYER_ERROR(invalid_bitwise_update, 9016, "Unknown or invalid $bit operation");

DOCLAYER_ERROR(invalid_bitwise_applicand, 10138, "$bit cannot update a value of non-integral type");
DOCLAYER_ERROR(invalid_bitwise_parameter, 10139, "$bit field must be a number");
DOCLAYER_ERROR(inc_applied_to_non_number, 10140, "Cannot apply $inc modifier to non-number");
DOCLAYER_ERROR(push_non_array, 10141, "Cannot apply $push/$pushAll modifier to non-array");
DOCLAYER_ERROR(pull_non_array, 10142, "Cannot apply $pull/$pullAll modifier to non-array");
DOCLAYER_ERROR(pop_non_array, 10143, "Cannot apply $pop modifier to non-array");
DOCLAYER_ERROR(subfield_of_simple_type, 10145, "Cannot set subfield of simple type");
DOCLAYER_ERROR(mixed_operator_literal_update,
               10147,
               "update must either include only operator expressions or only field:value expressions");
DOCLAYER_ERROR(operator_fieldname_in_object_key, 10148, "Query operators must not be members of object keys");
DOCLAYER_ERROR(malformed_logical_query_operator, 10149, "Logical query operators must originate from the top level");
DOCLAYER_ERROR(field_name_duplication_with_mods, 10150, "Field name duplication not allowed with modifiers");
DOCLAYER_ERROR(conflicting_mods_in_update, 10151, "have conflicting mods in update");
DOCLAYER_ERROR(inc_applied_with_non_number, 10152, "Modifier $inc allowed for numbers only");
DOCLAYER_ERROR(literal_multi_update, 10158, "multi update only works with $ operators");
DOCLAYER_ERROR(invalid_query_operator, 10159, "One or more query operators was used incorrectly");
DOCLAYER_ERROR(append_to_array_with_string_field,
               10348,
               "can't reference array element using non-numeric string field name");

DOCLAYER_ERROR(duplicated_key_field, 11000, "Duplicated value not allowed by unique index");
DOCLAYER_ERROR(unsupported_cmd, 11001, "unsupported command");
DOCLAYER_ERROR(unsupported_cmd_option, 11002, "unsupported command option");

DOCLAYER_ERROR(write_system_namespace, 12050, "you can't write directly to a system namespace");
DOCLAYER_ERROR(add_to_set_non_array, 12591, "Cannot apply $addToSet modifier to non-array");

DOCLAYER_ERROR(bad_rename_target, 13494, "$rename target must be a string");

DOCLAYER_ERROR(fieldname_with_dollar, 15896, "field name may not start with $");
DOCLAYER_ERROR(collection_with_dollar, 15897, "collection name may not start with $");
DOCLAYER_ERROR(database_with_dollar, 15898, "database name may not start with $");
DOCLAYER_ERROR(bad_find_and_modify, 15899, "invalid find and modify command");

DOCLAYER_ERROR(replace_with_id, 16836, "The _id field cannot be changed");
DOCLAYER_ERROR(mul_applied_to_non_number, 16837, "Cannot apply $mul to a value of non-numeric type");
DOCLAYER_ERROR(inc_or_mul_with_non_number, 16840, "Cannot increment with non-numeric argument")

DOCLAYER_ERROR(bad_regex_input, 17286, "$regex operator must take string or regex object");
DOCLAYER_ERROR(bad_sort_specifier, 17287, "Invalid sort specification passed to operation")
DOCLAYER_ERROR(invalid_projection, 17288, "Invalid projection object passed to operation")

DOCLAYER_ERROR(generic_invalid_parameter, 20000, "invalid parameter to operation");
DOCLAYER_ERROR(generic_missing_value, 20001, "value is missing");
DOCLAYER_ERROR(not_implemented, 20002, "not implemented");
DOCLAYER_ERROR(nonempty_index_construction, 20003, "tried to build an index on existing data");
DOCLAYER_ERROR(multiple_index_construction, 20004, "tried to create multiple indexes in one command");
DOCLAYER_ERROR(unique_index_background_construction, 20005, "tried to create unique indexes in background");
DOCLAYER_ERROR(empty_set_on_insert, 20009, "$setOnInsert is empty");
DOCLAYER_ERROR(cant_modify_id, 20010, "You may not modify '_id' in an update");

DOCLAYER_ERROR(update_operator_empty_parameter,
               26840,
               "Update operator has empty object for parameter. You must specify a field.");

DOCLAYER_ERROR(wire_protocol_mismatch, 29966, "Wire protocol mismatch. Bad message received.");
DOCLAYER_ERROR(no_index_name, 29967, "No index name specified");
DOCLAYER_ERROR(unsupported_index_type, 29969, "Document Layer does not support this index type, yet.");

DOCLAYER_ERROR(no_transaction_in_progress, 29980, "No transaction in progress.");
DOCLAYER_ERROR(no_symbol_type, 29981, "The Document Layer does not support the deprecated BSON `symbol` type.");
DOCLAYER_ERROR(compound_id_index, 29982, "You may not build a compound index including _id.");
DOCLAYER_ERROR(doclayer_metadata_changed,
               29983,
               "The Document Layer root metadata has changed or been deleted. You must restart the layer.");
DOCLAYER_ERROR(collection_metadata_changed, 29984, "Collection metadata changed during operation.");
DOCLAYER_ERROR(no_mongo_timestamps,
               29985,
               "The Document Layer does not support the MongoDB internal timestamp format.");
DOCLAYER_ERROR(no_old_binary, 29986, "The Document Layer does not support the deprecated (subtype 2) binary format.");
DOCLAYER_ERROR(index_wrong_build_id, 29987, "Attempting to change status of a different index build");
DOCLAYER_ERROR(metadata_changed_nonisolated,
               29988,
               "Collection metadata changed during non-isolated execution of plan");
DOCLAYER_ERROR(timeout_must_be_number, 29989, "Transaction timeouts must be specified with numeric types");
DOCLAYER_ERROR(no_mixed_compound_index, 29990, "Mixed order compound indexes are not supported");
DOCLAYER_ERROR(bad_index_specification, 29991, "Index must be 1 for ascending or -1 for descending");
DOCLAYER_ERROR(multikey_index_cartesian_explosion, 29992, "Multi-multikey index size exceeds maximum value.");
DOCLAYER_ERROR(index_name_taken, 29993, "There is an index with this name and a different key spec");
DOCLAYER_ERROR(high_version, 29994, "New storage format version");
DOCLAYER_ERROR(low_version, 29995, "Old storage format version");
DOCLAYER_ERROR(bad_version, 29996, "Unrecognized version key format");
DOCLAYER_ERROR(bad_dispatch, 29997, "Command or operator not found");
DOCLAYER_ERROR(index_already_exists, 29998, "Index already exists");
DOCLAYER_ERROR(no_array_id, 29999, "You may not use an array for a document id");

#undef DOCLAYER_ERROR
#endif
