/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include <thrift/lib/cpp2/transcode/JsonInterpreter.h>

#include <thrift/lib/cpp2/transcode/InterpreterInternal.h>
#include <thrift/lib/cpp2/transcode/Intrinsics.h>
#include <thrift/lib/cpp2/transcode/ReadHelpers.h>

#include <folly/CppAttributes.h>
#include <folly/Likely.h>
#include <folly/Range.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

namespace apache::thrift::transcode::detail {
namespace {

bool jsonMapReadsObjectForm(const MapOp& op) {
  const auto* key = std::get_if<ScalarOp>(op.key.get());
  return key != nullptr &&
      ((key->valueKind == ValueKind::Bytes &&
        key->readFn == ReadFn::ParseQuotedString) ||
       key->valueKind == ValueKind::Enum);
}

bool jsonMapWritesObjectForm(const MapOp& op) {
  const auto* key = std::get_if<ScalarOp>(op.key.get());
  return key != nullptr &&
      ((key->valueKind == ValueKind::Bytes &&
        key->writeFn == WriteFn::WriteQuotedString) ||
       key->valueKind == ValueKind::Enum);
}

void writeJsonObjectMapKey(
    TranscodeCursor* c, const ScalarOp& keyOp, const std::string& key) {
  if (keyOp.valueKind == ValueKind::Enum) {
    int64_t enumValue = 0;
    const auto* enumNames =
        keyOp.enumNames != nullptr ? &keyOp.enumNames->values : nullptr;
    if (!thrift_transcode_parse_json_object_enum_key(
            key, enumNames, enumValue) ||
        !intFits(keyOp.valueKind, enumValue)) [[unlikely]] {
      detail::setError(c, 1);
      return;
    }
    if (!writeScalarInt(c, keyOp, enumValue)) [[unlikely]] {
      return;
    }
    return;
  }
  if (keyOp.valueKind != ValueKind::Bytes ||
      keyOp.readFn != ReadFn::ParseQuotedString) {
    detail::setError(c, 90);
    return;
  }
  if (!writeScalarBytes(
          c,
          keyOp.writeFn,
          reinterpret_cast<const uint8_t*>(key.data()),
          key.size())) [[unlikely]] {
    return;
  }
}

void writeJsonObjectEnumKey(
    TranscodeCursor* c, const ScalarOp& keyOp, int64_t enumValue) {
  if (!intFits(keyOp.valueKind, enumValue)) [[unlikely]] {
    detail::setError(c, 1);
    return;
  }

  if (keyOp.enumNames != nullptr) {
    if (const auto* name =
            keyOp.enumNames->nameFor(static_cast<int32_t>(enumValue))) {
      thrift_transcode_format_escaped_string(
          c, reinterpret_cast<const uint8_t*>(name->data()), name->size());
      return;
    }
  }

  thrift_transcode_format_quoted_decimal_int(c, enumValue);
}

void writeJsonObjectMapTargetKey(TranscodeCursor* c, const ScalarOp& keyOp) {
  if (keyOp.valueKind == ValueKind::Enum) {
    int64_t enumValue = 0;
    if (!readScalarInt(c, keyOp, 0, &enumValue)) [[unlikely]] {
      return;
    }
    writeJsonObjectEnumKey(c, keyOp, enumValue);
    return;
  }
  if (keyOp.valueKind != ValueKind::Bytes ||
      keyOp.writeFn != WriteFn::WriteQuotedString) {
    detail::setError(c, 90);
    return;
  }
  execScalar(c, keyOp, 0);
}

void execJsonObjectMap(TranscodeCursor* c, const MapOp& op) {
  const auto* keyOp = std::get_if<ScalarOp>(op.key.get());
  if (keyOp == nullptr) {
    detail::setError(c, 90);
    return;
  }

  thrift_transcode_json_skip_whitespace(c);
  if (!thrift_transcode_json_expect_byte(c, '{')) [[unlikely]] {
    return;
  }
  thrift_transcode_json_skip_whitespace(c);
  if (thrift_transcode_json_peek(c) == '}') {
    if (!thrift_transcode_json_expect_byte(c, '}')) [[unlikely]] {
      return;
    }
    writeMapHeader(c, op.writeFraming, 0, op.writeKeyType, op.writeValueType);
    return;
  }

  TranscodePatchPoint writeMark = reserveNonEmptyMapHeader(c, op.writeFraming);
  if (hasError(c)) {
    return;
  }
  uint32_t count = 0;
  bool first = true;
  while (true) {
    thrift_transcode_json_skip_whitespace(c);
    if (thrift_transcode_json_peek(c) == '}') {
      break;
    }
    if (!first) {
      if (!thrift_transcode_json_expect_byte(c, ',')) [[unlikely]] {
        return;
      }
      thrift_transcode_json_skip_whitespace(c);
    }
    first = false;

    std::string key;
    if (!thrift_transcode_read_json_object_key(c, key)) [[unlikely]] {
      return;
    }

    thrift_transcode_json_skip_whitespace(c);
    if (!thrift_transcode_json_expect_byte(c, ':')) [[unlikely]] {
      return;
    }

    writeJsonObjectMapKey(c, *keyOp, key);
    if (hasError(c)) {
      return;
    }
    execCommand(c, *op.value, 0);
    if (hasError(c)) {
      return;
    }
    ++count;
  }
  if (!thrift_transcode_json_expect_byte(c, '}')) [[unlikely]] {
    return;
  }
  patchNonEmptyMapHeader(
      c, writeMark, op.writeFraming, count, op.writeKeyType, op.writeValueType);
}

void execJsonKeyValueArrayEntry(TranscodeCursor* c, const MapOp& op) {
  if (!thrift_transcode_json_expect_byte(c, '{')) [[unlikely]] {
    return;
  }

  const uint8_t* keyStart = nullptr;
  const uint8_t* keyEnd = nullptr;
  const uint8_t* valueStart = nullptr;
  const uint8_t* valueEnd = nullptr;

  bool first = true;
  while (true) {
    thrift_transcode_json_skip_whitespace(c);
    if (thrift_transcode_json_peek(c) == '}') {
      break;
    }
    if (!first) {
      if (!thrift_transcode_json_expect_byte(c, ',')) [[unlikely]] {
        return;
      }
      thrift_transcode_json_skip_whitespace(c);
    }
    first = false;

    std::string name;
    if (!thrift_transcode_read_json_object_key(c, name)) [[unlikely]] {
      return;
    }
    thrift_transcode_json_skip_whitespace(c);
    if (!thrift_transcode_json_expect_byte(c, ':')) [[unlikely]] {
      return;
    }

    const uint8_t* valueBegin = c->readPos;
    if (!thrift_transcode_skip_json_value(c)) [[unlikely]] {
      return;
    }
    const uint8_t* valueFinish = c->readPos;
    if (name == "key") {
      keyStart = valueBegin;
      keyEnd = valueFinish;
    } else if (name == "value") {
      valueStart = valueBegin;
      valueEnd = valueFinish;
    } else {
      detail::setError(c, kMalformedFieldType);
      return;
    }
  }

  if (keyStart == nullptr || valueStart == nullptr) [[unlikely]] {
    detail::setError(c, 1);
    return;
  }

  if (!thrift_transcode_json_expect_byte(c, '}')) [[unlikely]] {
    return;
  }
  const uint8_t* entryEnd = c->readPos;

  c->readPos = keyStart;
  execCommand(c, *op.key, 0);
  if (hasError(c)) {
    return;
  }
  if (c->readPos != keyEnd) {
    detail::setError(c, 1);
    return;
  }

  c->readPos = valueStart;
  execCommand(c, *op.value, 0);
  if (hasError(c)) {
    return;
  }
  if (c->readPos != valueEnd) {
    detail::setError(c, 1);
    return;
  }
  c->readPos = entryEnd;
}

void execJsonKeyValueArrayMap(TranscodeCursor* c, const MapOp& op) {
  thrift_transcode_json_skip_whitespace(c);
  if (!thrift_transcode_json_expect_byte(c, '[')) [[unlikely]] {
    return;
  }
  thrift_transcode_json_skip_whitespace(c);
  if (thrift_transcode_json_peek(c) == ']') {
    if (!thrift_transcode_json_expect_byte(c, ']')) [[unlikely]] {
      return;
    }
    writeMapHeader(c, op.writeFraming, 0, op.writeKeyType, op.writeValueType);
    return;
  }

  TranscodePatchPoint writeMark = reserveNonEmptyMapHeader(c, op.writeFraming);
  if (hasError(c)) {
    return;
  }
  uint32_t count = 0;
  bool first = true;
  while (true) {
    thrift_transcode_json_skip_whitespace(c);
    if (thrift_transcode_json_peek(c) == ']') {
      break;
    }
    if (!first) {
      if (!thrift_transcode_json_expect_byte(c, ',')) [[unlikely]] {
        return;
      }
      thrift_transcode_json_skip_whitespace(c);
    }
    first = false;

    execJsonKeyValueArrayEntry(c, op);
    if (hasError(c)) {
      return;
    }
    ++count;
  }
  if (!thrift_transcode_json_expect_byte(c, ']')) [[unlikely]] {
    return;
  }
  patchNonEmptyMapHeader(
      c, writeMark, op.writeFraming, count, op.writeKeyType, op.writeValueType);
}

void execJsonObjectMapTarget(
    TranscodeCursor* c, const MapOp& op, uint32_t count) {
  const auto* keyOp = std::get_if<ScalarOp>(op.key.get());
  if (keyOp == nullptr) {
    detail::setError(c, 90);
    return;
  }

  thrift_transcode_write_byte_checked(c, '{');
  for (uint32_t i = 0; i < count; ++i) {
    if (hasError(c)) {
      return;
    }
    if (i != 0) {
      thrift_transcode_write_byte_checked(c, ',');
    }
    writeJsonObjectMapTargetKey(c, *keyOp);
    if (hasError(c)) {
      return;
    }
    thrift_transcode_write_byte_checked(c, ':');
    execCommand(c, *op.value, 0);
  }
  thrift_transcode_write_byte_checked(c, '}');
}

void execJsonKeyValueArrayMapTarget(
    TranscodeCursor* c, const MapOp& op, uint32_t count) {
  thrift_transcode_write_byte_checked(c, '[');
  for (uint32_t i = 0; i < count; ++i) {
    if (hasError(c)) {
      return;
    }
    if (i != 0) {
      thrift_transcode_write_byte_checked(c, ',');
    }
    thrift_transcode_write_raw_bytes_checked(
        c, reinterpret_cast<const uint8_t*>("{\"key\":"), 7);
    execCommand(c, *op.key, 0);
    if (hasError(c)) {
      return;
    }
    thrift_transcode_write_raw_bytes_checked(
        c, reinterpret_cast<const uint8_t*>(",\"value\":"), 9);
    execCommand(c, *op.value, 0);
    if (hasError(c)) {
      return;
    }
    thrift_transcode_write_byte_checked(c, '}');
  }
  thrift_transcode_write_byte_checked(c, ']');
}

const FieldEntry* FOLLY_NULLABLE
findFieldByName(const StructOp& op, const TranscodeJsonStringToken& name) {
  for (size_t i = 0; i < op.fields.size(); ++i) {
    const auto& f = op.fields[i];
    const folly::ByteRange fieldName{std::string_view{f.fieldName}};
    if (thrift_transcode_json_string_token_equals(
            &name, fieldName.data(), fieldName.size())) {
      return &f;
    }
  }
  return nullptr;
}

bool readJsonObjectFieldName(
    TranscodeCursor* c, bool& first, TranscodeJsonStringToken& name) {
  thrift_transcode_json_skip_whitespace(c);
  if (thrift_transcode_json_peek(c) == '}') {
    return false;
  }
  if (!first) {
    if (!thrift_transcode_json_expect_byte(c, ',')) [[unlikely]] {
      return false;
    }
    thrift_transcode_json_skip_whitespace(c);
  }
  first = false;

  if (!thrift_transcode_read_json_string_token(c, &name)) [[unlikely]] {
    return false;
  }
  thrift_transcode_json_skip_whitespace(c);
  if (!thrift_transcode_json_expect_byte(c, ':')) [[unlikely]] {
    return false;
  }
  thrift_transcode_json_skip_whitespace(c);
  return true;
}

struct TaggedUnionInput {
  const FieldEntry* member{nullptr};
  const uint8_t* objectBegin{nullptr};
  const uint8_t* objectEnd{nullptr};
  const uint8_t* contentBegin{nullptr};
  const uint8_t* contentEnd{nullptr};
};

TaggedUnionInput scanTaggedUnion(
    TranscodeCursor* c, const StructOp& op, const TaggedUnion& taggedUnion) {
  TaggedUnionInput result{.objectBegin = c->readPos};
  thrift_transcode_json_skip_whitespace(c);
  if (!thrift_transcode_json_expect_byte(c, '{')) [[unlikely]] {
    return {};
  }

  bool first = true;
  bool tagSeen = false;
  bool contentSeen = false;
  const folly::ByteRange tag{std::string_view{taggedUnion.tag}};
  const folly::ByteRange content = taggedUnion.content.has_value()
      ? folly::ByteRange{std::string_view{*taggedUnion.content}}
      : folly::ByteRange{};
  while (!hasError(c)) {
    TranscodeJsonStringToken name{};
    if (!readJsonObjectFieldName(c, first, name)) {
      break;
    }

    if (thrift_transcode_json_string_token_equals(
            &name, tag.data(), tag.size())) {
      if (tagSeen) [[unlikely]] {
        detail::setError(c, kMalformedFieldType);
        return {};
      }
      tagSeen = true;
      TranscodeJsonStringToken value{};
      if (!thrift_transcode_read_json_string_token(c, &value)) [[unlikely]] {
        return {};
      }
      result.member = findFieldByName(op, value);
      if (result.member == nullptr) [[unlikely]] {
        detail::setError(c, kMalformedFieldType);
        return {};
      }
      continue;
    }

    if (taggedUnion.content.has_value() &&
        thrift_transcode_json_string_token_equals(
            &name, content.data(), content.size())) {
      if (contentSeen) [[unlikely]] {
        detail::setError(c, kMalformedFieldType);
        return {};
      }
      contentSeen = true;
      result.contentBegin = c->readPos;
      if (!thrift_transcode_skip_json_value(c)) [[unlikely]] {
        return {};
      }
      result.contentEnd = c->readPos;
      continue;
    }

    // With internal tagging the remaining members are the arm's own fields,
    // which are checked when the arm is read.
    if (taggedUnion.content.has_value() &&
        op.unknownFieldMode == UnknownFieldMode::Reject) [[unlikely]] {
      detail::setError(c, kMalformedFieldType);
      return {};
    }
    if (!thrift_transcode_skip_json_value(c)) [[unlikely]] {
      return {};
    }
  }

  if (!thrift_transcode_json_expect_byte(c, '}') || !tagSeen ||
      (taggedUnion.content.has_value() && !contentSeen)) [[unlikely]] {
    if (!hasError(c)) {
      detail::setError(c, kMalformedFieldType);
    }
    return {};
  }
  result.objectEnd = c->readPos;
  return result;
}

bool writeJsonScalarValue(
    TranscodeCursor* c,
    const FieldEntry& field,
    const ScalarOp& scalar,
    const ScalarOverrideValue& value,
    bool& wroteJsonField) {
  if (wroteJsonField) {
    thrift_transcode_write_byte_checked(c, ',');
  }
  if (hasError(c)) {
    return false;
  }
  wroteJsonField = true;
  thrift_transcode_format_escaped_string(
      c,
      reinterpret_cast<const uint8_t*>(field.fieldName.data()),
      field.fieldName.size());
  thrift_transcode_write_byte_checked(c, ':');
  if (hasError(c)) {
    return false;
  }
  return writeScalarValue(c, scalar, value);
}

void execTaggedUnion(
    TranscodeCursor* c,
    const StructOp& op,
    ScalarFieldOverrides fieldOverrides) {
  if (!fieldOverrides.empty()) [[unlikely]] {
    detail::setError(c, kMalformedFieldType);
    return;
  }
  const FieldProto writeProto = op.writeFieldProto;
  const auto& taggedUnion = *op.readTaggedUnion;

  const auto input = scanTaggedUnion(c, op, taggedUnion);
  if (input.member == nullptr || hasError(c)) {
    return;
  }
  TranscodePatchPoint writeMark{};
  if (op.writeLengthDelimited) {
    writeMark = thrift_transcode_proto_reserve_length(c);
  }

  const Framing wf = framingFor(writeProto);
  int16_t prevWrite = 0;
  if (taggedUnion.content.has_value()) {
    const auto* savedReadEnd = c->readEnd;
    c->readPos = input.contentBegin;
    c->readEnd = input.contentEnd;
    if (const auto* scalar = std::get_if<ScalarOp>(input.member->command.get());
        scalar != nullptr && scalar->writeFn == WriteFn::CompactBoolInType) {
      int64_t value = 0;
      if (readScalarInt(c, *scalar, 0, &value)) {
        thrift_transcode_compact_write_bool_field(
            c, value ? 1 : 0, targetFieldId(*input.member), prevWrite);
      }
    } else {
      wf.writeHeader(
          c,
          input.member->writeTypeInfo,
          targetFieldId(*input.member),
          prevWrite);
      execCommand(c, *input.member->command, 0);
    }
    const bool contentConsumed = !hasError(c) && c->readPos == c->readEnd;
    c->readEnd = savedReadEnd;
    c->readPos = input.objectEnd;
    if (!contentConsumed) {
      detail::setError(c, kMalformedFieldType);
      return;
    }
  } else {
    const auto* arm = std::get_if<StructOp>(input.member->command.get());
    if (arm == nullptr) [[unlikely]] {
      detail::setError(c, kMalformedFieldType);
      return;
    }
    wf.writeHeader(
        c,
        input.member->writeTypeInfo,
        targetFieldId(*input.member),
        prevWrite);
    if (hasError(c)) {
      return;
    }
    c->readPos = input.objectBegin;
    execJsonStruct(
        c, *arm, {}, folly::ByteRange{std::string_view{taggedUnion.tag}});
  }
  if (hasError(c)) {
    return;
  }
  if (!op.writeLengthDelimited) {
    wf.writeStop(c);
  } else {
    thrift_transcode_proto_patch_length(c, writeMark);
  }
}

void execIdStructToJsonFields(
    TranscodeCursor* c,
    const StructOp& op,
    FieldProto readProto,
    const Framing& rf,
    ScalarFieldOverrides fieldOverrides,
    bool& wroteJsonField) {
  const uint8_t* savedReadEnd = nullptr;
  if (!enterIdStructRead(c, op, savedReadEnd)) [[unlikely]] {
    return;
  }

  int16_t prevRead = 0;
  const bool unionStruct = isUnion(op);
  bool unionMemberSeen = false;
  while (!hasError(c)) {
    IdFieldMatch match;
    if (!readNextIdField(
            c, op, readProto, rf, prevRead, match, op.unknownFieldMode)) {
      break;
    }
    if (!noteUnionMember(c, unionStruct, unionMemberSeen)) [[unlikely]] {
      return;
    }
    if (wroteJsonField) {
      thrift_transcode_write_byte_checked(c, ',');
    }
    wroteJsonField = true;
    thrift_transcode_format_escaped_string(
        c,
        reinterpret_cast<const uint8_t*>(match.field->fieldName.data()),
        match.field->fieldName.size());
    thrift_transcode_write_byte_checked(c, ':');
    execCommand(c, *match.field->command, match.typeInfo);
  }

  restoreIdStructReadEnd(c, op, savedReadEnd);
  if (hasError(c)) {
    return;
  }

  for (const auto& override : fieldOverrides) {
    const FieldEntry* field = nullptr;
    const ScalarOp* scalar = nullptr;
    if (!resolveScalarOverrideField(c, op, override, field, scalar))
        [[unlikely]] {
      return;
    }
    if (!noteUnionMember(c, unionStruct, unionMemberSeen)) [[unlikely]] {
      return;
    }
    if (!writeJsonScalarValue(
            c, *field, *scalar, override.value, wroteJsonField)) [[unlikely]] {
      return;
    }
  }

  if (!finishUnion(c, unionStruct, unionMemberSeen)) [[unlikely]] {
    return;
  }
}

void writeJsonFieldPrefix(
    TranscodeCursor* c, std::string_view name, bool& wroteJsonField) {
  if (wroteJsonField) {
    thrift_transcode_write_byte_checked(c, ',');
  }
  wroteJsonField = true;
  thrift_transcode_format_escaped_string(
      c, reinterpret_cast<const uint8_t*>(name.data()), name.size());
  thrift_transcode_write_byte_checked(c, ':');
}

void writeJsonStringField(
    TranscodeCursor* c,
    std::string_view name,
    std::string_view value,
    bool& wroteJsonField) {
  writeJsonFieldPrefix(c, name, wroteJsonField);
  thrift_transcode_format_escaped_string(
      c, reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

void execIdStructToTaggedJson(
    TranscodeCursor* c,
    const StructOp& op,
    FieldProto readProto,
    const Framing& rf,
    ScalarFieldOverrides fieldOverrides) {
  if (!fieldOverrides.empty()) [[unlikely]] {
    detail::setError(c, kMalformedFieldType);
    return;
  }

  const uint8_t* savedReadEnd = nullptr;
  if (!enterIdStructRead(c, op, savedReadEnd)) [[unlikely]] {
    return;
  }

  thrift_transcode_write_byte_checked(c, '{');
  int16_t prevRead = 0;
  bool wroteJsonField = false;
  bool unionMemberSeen = false;
  const auto& taggedUnion = *op.writeTaggedUnion;
  while (!hasError(c)) {
    IdFieldMatch match;
    if (!readNextIdField(
            c, op, readProto, rf, prevRead, match, op.unknownFieldMode)) {
      break;
    }
    if (!noteSingleField(c, unionMemberSeen)) [[unlikely]] {
      return;
    }
    writeJsonStringField(
        c, taggedUnion.tag, match.field->fieldName, wroteJsonField);
    if (hasError(c)) {
      return;
    }
    if (taggedUnion.content.has_value()) {
      writeJsonFieldPrefix(c, *taggedUnion.content, wroteJsonField);
      execCommand(c, *match.field->command, match.typeInfo);
      continue;
    }

    const auto& member = std::get<StructOp>(*match.field->command);
    const auto memberReadProto = member.readFieldProto;
    execIdStructToJsonFields(
        c,
        member,
        memberReadProto,
        framingFor(memberReadProto),
        {},
        wroteJsonField);
  }

  restoreIdStructReadEnd(c, op, savedReadEnd);
  if (hasError(c)) {
    return;
  }
  if (!finishSingleField(c, unionMemberSeen)) [[unlikely]] {
    return;
  }
  thrift_transcode_write_byte_checked(c, '}');
}

} // namespace

void validateInputConsumed(TranscodeCursor* c, const TranscodePlan& plan) {
  if (hasError(c) || plan.sourceProtocol != WireProtocol::Json) {
    return;
  }
  thrift_transcode_json_skip_whitespace(c);
  if (c->readPos != c->readEnd) {
    detail::setError(c, 1);
  }
}

void execJsonBytesScalar(TranscodeCursor* c, const ScalarOp& op) {
  TranscodeJsonStringToken token{};
  if (!thrift_transcode_read_json_string_token(c, &token)) [[unlikely]] {
    return;
  }

  if (op.readFn == ReadFn::ParseQuotedString) {
    switch (op.writeFn) {
      case WriteFn::LengthPrefixedVarint:
        thrift_transcode_write_json_string_token_varint_prefixed(c, &token);
        return;
      case WriteFn::LengthPrefixedI32:
        thrift_transcode_write_json_string_token_i32_prefixed(c, &token);
        return;
      case WriteFn::WriteQuotedString:
        thrift_transcode_write_json_string_token_quoted(c, &token);
        return;
      case WriteFn::WriteBase64String: {
        std::string bytes =
            thrift_transcode_decode_json_string_token_to_string(c, token);
        if (hasError(c)) {
          return;
        }
        thrift_transcode_format_base64_string(
            c, reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
        return;
      }
      case WriteFn::ZigzagVarint:
      case WriteFn::UnsignedVarint:
      case WriteFn::Fixed8:
      case WriteFn::Fixed16BE:
      case WriteFn::Fixed32BE:
      case WriteFn::Fixed64BE:
      case WriteFn::Fixed32LE:
      case WriteFn::Fixed64LE:
      case WriteFn::CompactBoolInType:
      case WriteFn::ByteAsBool:
      case WriteFn::VarintAsBool:
      case WriteFn::IntToDecimalText:
      case WriteFn::EnumNameOrDecimalText:
      case WriteFn::FloatToDecimalText:
      case WriteFn::BoolToKeyword:
      case WriteFn::StoreAtOffset:
      case WriteFn::CallTypeInfoSet:
      case WriteFn::Custom:
        detail::setError(c, 90);
        return;
    }
  }

  switch (op.writeFn) {
    case WriteFn::LengthPrefixedVarint:
      thrift_transcode_write_json_base64_token_varint_prefixed(c, &token);
      return;
    case WriteFn::LengthPrefixedI32:
      thrift_transcode_write_json_base64_token_i32_prefixed(c, &token);
      return;
    case WriteFn::WriteBase64String:
      thrift_transcode_write_json_string_token_quoted(c, &token);
      return;
    case WriteFn::ZigzagVarint:
    case WriteFn::UnsignedVarint:
    case WriteFn::Fixed8:
    case WriteFn::Fixed16BE:
    case WriteFn::Fixed32BE:
    case WriteFn::Fixed64BE:
    case WriteFn::Fixed32LE:
    case WriteFn::Fixed64LE:
    case WriteFn::CompactBoolInType:
    case WriteFn::ByteAsBool:
    case WriteFn::VarintAsBool:
    case WriteFn::IntToDecimalText:
    case WriteFn::EnumNameOrDecimalText:
    case WriteFn::FloatToDecimalText:
    case WriteFn::WriteQuotedString:
    case WriteFn::BoolToKeyword:
    case WriteFn::StoreAtOffset:
    case WriteFn::CallTypeInfoSet:
    case WriteFn::Custom:
      detail::setError(c, 90);
      return;
  }
}

void execJsonSeq(TranscodeCursor* c, const SeqOp& op) {
  // JSON array source: '[' elem (',' elem)* ']'. The element count isn't on
  // the wire, so reuse the ByBytes machinery — reserve the target header,
  // count while looping, then back-patch — with the same reserve sizes and
  // patch calls as execSeq's packed-read arm.
  thrift_transcode_json_skip_whitespace(c);
  thrift_transcode_json_expect_byte(
      c, '['); // untrusted input: validate, unlike the JIT
  if (hasError(c)) {
    return;
  }

  TranscodePatchPoint writeMark = reserveSeqHeader(c, op.writeFraming);

  uint32_t count = 0;
  bool first = true;
  while (true) {
    if (hasError(c)) {
      return;
    }
    thrift_transcode_json_skip_whitespace(c);
    if (thrift_transcode_json_peek(c) == ']') {
      break;
    }
    if (!first) {
      thrift_transcode_json_expect_byte(c, ',');
      if (hasError(c)) {
        return;
      }
    }
    first = false;
    execCommand(c, *op.element, 0);
    ++count;
  }

  thrift_transcode_json_expect_byte(c, ']');
  if (hasError(c)) {
    return;
  }

  patchSeqHeader(c, writeMark, op.writeFraming, count, op.writeElemType);
}

void execJsonSeqTarget(TranscodeCursor* c, const SeqOp& op) {
  uint32_t count = readSeqCount(c, op.readFraming, op.readElemType);
  if (hasError(c)) {
    return;
  }
  thrift_transcode_write_byte_checked(c, '[');
  for (uint32_t i = 0; i < count; ++i) {
    if (hasError(c)) {
      return;
    }
    if (i != 0) {
      thrift_transcode_write_byte_checked(c, ',');
    }
    execCommand(c, *op.element, 0);
  }
  thrift_transcode_write_byte_checked(c, ']');
}

void execJsonMap(TranscodeCursor* c, const MapOp& op) {
  if (jsonMapReadsObjectForm(op)) {
    execJsonObjectMap(c, op);
  } else {
    execJsonKeyValueArrayMap(c, op);
  }
}

void execJsonMapTarget(TranscodeCursor* c, const MapOp& op) {
  if (op.readFraming == ContainerFraming::Json) {
    detail::setError(c, 90);
    return;
  }

  uint32_t count =
      readMapCount(c, op.readFraming, op.readKeyType, op.readValueType);
  if (hasError(c)) {
    return;
  }
  if (jsonMapWritesObjectForm(op)) {
    execJsonObjectMapTarget(c, op, count);
  } else {
    execJsonKeyValueArrayMapTarget(c, op, count);
  }
}

// JSON object source → field-framed target.
// read `{`, loop over `"name": value` pairs writing the matched field through
// the target's numeric field headers, reject unknown keys, read `}`, and
// finish the target framing.
void execJsonStruct(
    TranscodeCursor* c,
    const StructOp& op,
    ScalarFieldOverrides fieldOverrides,
    folly::ByteRange ignoredMember) {
  if (op.readTaggedUnion.has_value()) {
    execTaggedUnion(c, op, fieldOverrides);
    return;
  }
  FieldProto wp = op.writeFieldProto;
  if (wp == FieldProto::Unsupported) {
    detail::setError(c, 90); // interpreter: unsupported protocol
    return;
  }
  Framing wf = framingFor(wp);

  TranscodePatchPoint writeMark{};
  bool patchWrite = false;
  if (op.writeLengthDelimited) {
    writeMark = thrift_transcode_proto_reserve_length(c);
    patchWrite = true;
  }

  thrift_transcode_json_skip_whitespace(c);
  if (!thrift_transcode_json_expect_byte(c, '{')) [[unlikely]] {
    return;
  }

  int16_t prevWrite = 0;
  bool first = true;
  const bool unionStruct = isUnion(op);
  bool unionMemberSeen = false;
  while (true) {
    if (hasError(c)) {
      return;
    }
    TranscodeJsonStringToken name{};
    if (!readJsonObjectFieldName(c, first, name)) {
      if (hasError(c)) {
        return;
      }
      break;
    }

    const FieldEntry* fe = findFieldByName(op, name);
    if (fe == nullptr) {
      const bool ignored = !ignoredMember.empty() &&
          thrift_transcode_json_string_token_equals(
              &name, ignoredMember.data(), ignoredMember.size());
      if (!ignored && op.unknownFieldMode == UnknownFieldMode::Reject)
          [[unlikely]] {
        detail::setError(c, kMalformedFieldType);
        return;
      }
      if (!thrift_transcode_skip_json_value(c)) [[unlikely]] {
        return;
      }
      continue;
    }

    if (thrift_transcode_json_consume_null(c)) {
      if (!fe->optional) {
        detail::setError(c, kMalformedFieldType);
      }
      continue;
    }

    if (!noteUnionMember(c, unionStruct, unionMemberSeen)) [[unlikely]] {
      return;
    }

    // Deferred Compact bool: the value is encoded in the field header type
    // byte, so it must be read before the header is written. Same handling as
    // the field-framed struct path.
    if (const auto* sc = std::get_if<ScalarOp>(fe->command.get());
        sc != nullptr && sc->writeFn == WriteFn::CompactBoolInType) {
      int64_t boolVal = 0;
      if (!readScalarInt(c, *sc, 0, &boolVal)) [[unlikely]] {
        return;
      }
      thrift_transcode_compact_write_bool_field(
          c, boolVal ? 1 : 0, targetFieldId(*fe), prevWrite);
      prevWrite = targetFieldId(*fe);
      continue;
    }

    wf.writeHeader(c, fe->writeTypeInfo, targetFieldId(*fe), prevWrite);
    prevWrite = targetFieldId(*fe);
    execCommand(c, *fe->command, 0);
  }

  for (const auto& override : fieldOverrides) {
    const FieldEntry* field = nullptr;
    const ScalarOp* scalar = nullptr;
    if (!resolveScalarOverrideField(c, op, override, field, scalar))
        [[unlikely]] {
      return;
    }
    if (!noteUnionMember(c, unionStruct, unionMemberSeen)) [[unlikely]] {
      return;
    }
    if (!writeFieldFramedScalarValue(
            c, wf, *field, *scalar, override.value, prevWrite)) [[unlikely]] {
      return;
    }
  }

  if (!thrift_transcode_json_expect_byte(c, '}')) [[unlikely]] {
    return;
  }
  if (!finishUnion(c, unionStruct, unionMemberSeen)) [[unlikely]] {
    return;
  }
  if (!op.writeLengthDelimited) {
    wf.writeStop(c);
  }
  if (c->error != 0) {
    return;
  }

  if (patchWrite) {
    thrift_transcode_proto_patch_length(c, writeMark);
  }
}

void execIdStructToJson(
    TranscodeCursor* c,
    const StructOp& op,
    FieldProto readProto,
    const Framing& rf,
    ScalarFieldOverrides fieldOverrides) {
  if (op.writeTaggedUnion.has_value()) {
    execIdStructToTaggedJson(c, op, readProto, rf, fieldOverrides);
    return;
  }

  thrift_transcode_write_byte_checked(c, '{');
  bool wroteJsonField = false;
  execIdStructToJsonFields(
      c, op, readProto, rf, fieldOverrides, wroteJsonField);
  if (hasError(c)) {
    return;
  }
  thrift_transcode_write_byte_checked(c, '}');
}

void execJsonStructFlattened(TranscodeCursor* c, const StructOp& op) {
  thrift_transcode_json_skip_whitespace(c);
  if (!thrift_transcode_json_expect_byte(c, '{')) [[unlikely]] {
    return;
  }

  bool first = true;
  bool fieldSeen = false;
  while (true) {
    if (hasError(c)) {
      return;
    }
    TranscodeJsonStringToken name{};
    if (!readJsonObjectFieldName(c, first, name)) {
      if (hasError(c)) {
        return;
      }
      break;
    }

    const FieldEntry* field = findFieldByName(op, name);
    if (field == nullptr || field->command == nullptr) {
      detail::setError(c, kMalformedFieldType);
      return;
    }

    if (!execFlattenedField(c, *field, 0, fieldSeen)) [[unlikely]] {
      return;
    }
  }

  if (!thrift_transcode_json_expect_byte(c, '}')) [[unlikely]] {
    return;
  }
  if (!finishSingleField(c, fieldSeen)) [[unlikely]] {
    return;
  }
}

} // namespace apache::thrift::transcode::detail
