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

#include <thrift/lib/cpp2/transcode/TranscodeInterpreter.h>

#include <thrift/lib/cpp2/dynamic/TypeSystem.h>
#include <thrift/lib/cpp2/transcode/InterpreterInternal.h>
#include <thrift/lib/cpp2/transcode/JsonInterpreter.h>
#include <thrift/lib/cpp2/transcode/ProtobufInterpreter.h>
#include <thrift/lib/cpp2/transcode/ReadHelpers.h>
#include <thrift/lib/cpp2/transcode/WireType.h>

#include <folly/CppAttributes.h>
#include <folly/Likely.h>
#include <folly/Range.h>
#include <folly/ScopeGuard.h>
#include <folly/lang/Assume.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <variant>
#include <vector>

namespace apache::thrift::transcode {

namespace {

// Interim mapping from the numeric error codes the intrinsics latch onto the
// cursor to the TranscodeErrc taxonomy. kUnsupportedProtocol is the
// interpreter's own unsupported-protocol code.
// TODO(D4a): canonicalize intrinsic codes so this becomes a direct cast.
TranscodeErrc errcFromCursor(int64_t code) {
  switch (code) {
    case 0:
      return TranscodeErrc::Ok;
    case detail::kUnsupportedProtocol:
      return TranscodeErrc::Unsupported;
    default:
      return TranscodeErrc::Malformed;
  }
}

struct MallocOutput {
  uint8_t* buffer = nullptr;
  size_t capacity = 0;
};

bool checkedAdd(size_t lhs, size_t rhs, size_t& out) {
  if (lhs > std::numeric_limits<size_t>::max() - rhs) {
    return false;
  }
  out = lhs + rhs;
  return true;
}

TranscodeStatus mallocExtend(
    const TranscodeExtendRequest* request,
    TranscodeExtendResult* result,
    void* userData) {
  auto& output = *static_cast<MallocOutput*>(userData);
  const size_t written =
      static_cast<size_t>(request->writePoint - request->segment.begin);
  size_t requested = 0;
  if (!checkedAdd(written, request->minWritable, requested)) {
    return TranscodeStatus::Error;
  }
  const size_t doubled =
      output.capacity > std::numeric_limits<size_t>::max() / 2
      ? requested
      : output.capacity * 2;
  const size_t newCapacity = std::max(doubled, requested);
  auto* next = static_cast<uint8_t*>(realloc(output.buffer, newCapacity));
  if (next == nullptr) {
    return TranscodeStatus::Error;
  }
  result->kind = next == request->segment.begin
      ? TranscodeExtendKind::InPlaceExtension
      : TranscodeExtendKind::RelocatedContiguous;
  result->segment = {next, next + newCapacity};
  output.buffer = next;
  output.capacity = newCapacity;
  return TranscodeStatus::Ok;
}

TranscodeStatus fixedOutputExtend(
    const TranscodeExtendRequest* /*request*/,
    TranscodeExtendResult* /*result*/,
    void* /*userData*/) {
  return TranscodeStatus::Error;
}

} // namespace

namespace detail {
namespace {

namespace wire = apache::thrift::transcode::wire;

uint64_t doubleToBits(double d) {
  uint64_t bits;
  std::memcpy(&bits, &d, sizeof(bits));
  return bits;
}
uint32_t floatToBits(float f) {
  uint32_t bits;
  std::memcpy(&bits, &f, sizeof(bits));
  return bits;
}

bool fieldTypeMatches(
    FieldProto readProto, uint8_t expectedTypeInfo, uint8_t actualTypeInfo) {
  if (readProto == FieldProto::Compact &&
      expectedTypeInfo == wire::kCompactBooleanTrue) {
    return actualTypeInfo == wire::kCompactBooleanTrue ||
        actualTypeInfo == wire::kCompactBooleanFalse;
  }
  return actualTypeInfo == expectedTypeInfo;
}

bool fieldTypeMatches(
    FieldProto readProto, const FieldEntry& field, uint8_t actualTypeInfo) {
  return fieldTypeMatches(readProto, field.readTypeInfo, actualTypeInfo);
}

bool setReadEndFromByteLength(TranscodeCursor& c, uint64_t byteLen) {
  const uint8_t* parentReadEnd = c.readEnd;
  if (c.readPos > parentReadEnd ||
      byteLen > static_cast<uint64_t>(parentReadEnd - c.readPos)) {
    detail::setError(&c, kMalformedContainerHeader);
    return false;
  }
  c.readEnd = c.readPos + static_cast<size_t>(byteLen);
  return true;
}

bool writeScalarFloat(TranscodeCursor* c, WriteFn fn, double v) {
  switch (fn) {
    case WriteFn::Fixed64LE:
      thrift_transcode_write_fixed64_le_checked(c, doubleToBits(v));
      break;
    case WriteFn::Fixed64BE:
      thrift_transcode_write_fixed64_be_checked(c, doubleToBits(v));
      break;
    case WriteFn::Fixed32LE:
      thrift_transcode_write_fixed32_le_checked(
          c, floatToBits(static_cast<float>(v)));
      break;
    case WriteFn::Fixed32BE:
      thrift_transcode_write_fixed32_be_checked(
          c, floatToBits(static_cast<float>(v)));
      break;
    case WriteFn::FloatToDecimalText:
      thrift_transcode_format_decimal_float(c, v);
      break;
    case WriteFn::ZigzagVarint:
    case WriteFn::UnsignedVarint:
    case WriteFn::Fixed8:
    case WriteFn::Fixed16BE:
    case WriteFn::LengthPrefixedVarint:
    case WriteFn::LengthPrefixedI32:
    case WriteFn::CompactBoolInType:
    case WriteFn::ByteAsBool:
    case WriteFn::VarintAsBool:
    case WriteFn::IntToDecimalText:
    case WriteFn::EnumNameOrDecimalText:
    case WriteFn::WriteQuotedString:
    case WriteFn::WriteBase64String:
    case WriteFn::BoolToKeyword:
    case WriteFn::StoreAtOffset:
    case WriteFn::CallTypeInfoSet:
    case WriteFn::Custom:
      folly::assume_unreachable();
  }
  return !hasError(c);
}

void getIntegerOverride(const ScalarOverrideValue& value, int64_t& out) {
  out = value.as<int64_t>();
}

bool getBoolOverride(
    TranscodeCursor* c, const ScalarOverrideValue& value, int64_t& out) {
  getIntegerOverride(value, out);
  if (out != 0 && out != 1) {
    detail::setError(c, kMalformedFieldType);
    return false;
  }
  return true;
}

// ── Container framing (inline, mirrors KernelCodegen) ──

void writeSeqHeader(
    TranscodeCursor* c,
    ContainerFraming framing,
    uint32_t count,
    uint8_t elemType) {
  switch (framing) {
    case ContainerFraming::Compact:
      if (count <= 14) {
        thrift_transcode_write_byte_checked(
            c, static_cast<uint8_t>((count << 4) | elemType));
      } else {
        thrift_transcode_write_byte_checked(
            c, static_cast<uint8_t>(0xF0 | elemType));
        thrift_transcode_write_unsigned_varint(c, count);
      }
      break;
    case ContainerFraming::Binary:
      thrift_transcode_cursor_ensure_write(c, 5);
      if (hasError(c)) {
        return;
      }
      thrift_transcode_write_byte_unchecked(c, elemType);
      thrift_transcode_write_fixed32_be_unchecked(c, count);
      break;
    case ContainerFraming::Json:
    case ContainerFraming::None:
      break;
  }
}

void execSeq(TranscodeCursor* c, const SeqOp& op) {
  if (op.readLoopKind == LoopKind::ByBytes) {
    // ByBytes (e.g. protobuf packed) read: the element count is not on the
    // wire. Read the byte-length, scope the read window, loop until it is
    // consumed, then back-patch the deferred container header once the count is
    // known — mirrors the JIT's emitSeqOp using the same extern "C" cursor
    // intrinsics. The patched Thrift header uses the fixed-width long form
    // (non-canonical but reader-compatible); see the side-effect note in
    // KernelCodegen.cpp's emitSeqOp.
    uint64_t byteLen = thrift_transcode_read_unsigned_varint(c);
    if (hasError(c)) {
      return;
    }
    const uint8_t* savedReadEnd = c->readEnd;
    if (!setReadEndFromByteLength(*c, byteLen)) [[unlikely]] {
      return;
    }

    TranscodePatchPoint writeMark{};
    if (op.writeFraming == ContainerFraming::Json) {
      thrift_transcode_write_byte_checked(c, '[');
    } else if (op.writeLoopKind == LoopKind::ByBytes) {
      writeMark = thrift_transcode_proto_reserve_length(c);
    } else {
      writeMark = reserveSeqHeader(c, op.writeFraming);
    }

    uint32_t count = 0;
    while (c->readPos < c->readEnd && !hasError(c)) {
      if (op.writeFraming == ContainerFraming::Json && count != 0) {
        thrift_transcode_write_byte_checked(c, ',');
      }
      execCommand(c, *op.element, 0);
      ++count;
    }
    c->readEnd = savedReadEnd;
    if (hasError(c)) {
      return;
    }

    if (op.writeFraming == ContainerFraming::Json) {
      thrift_transcode_write_byte_checked(c, ']');
    } else if (op.writeLoopKind == LoopKind::ByBytes) {
      thrift_transcode_proto_patch_length(c, writeMark);
    } else {
      patchSeqHeader(c, writeMark, op.writeFraming, count, op.writeElemType);
    }
    return;
  }

  if (op.readFraming == ContainerFraming::Json) {
    execJsonSeq(c, op);
    return;
  }
  if (op.writeFraming == ContainerFraming::Json) {
    execJsonSeqTarget(c, op);
    return;
  }

  // The interpreter baseline supports ByCount framing (Compact/Binary).
  uint32_t count = readSeqCount(c, op.readFraming, op.readElemType);
  if (hasError(c)) {
    return;
  }

  writeSeqHeader(c, op.writeFraming, count, op.writeElemType);
  if (hasError(c)) {
    return;
  }
  for (uint32_t i = 0; i < count; ++i) {
    if (hasError(c)) {
      break;
    }
    execCommand(c, *op.element, 0);
  }
}

void execMap(TranscodeCursor* c, const MapOp& op) {
  if (op.writeFraming == ContainerFraming::Json) {
    execJsonMapTarget(c, op);
    return;
  }
  if (op.readFraming == ContainerFraming::Json) {
    execJsonMap(c, op);
    return;
  }
  uint32_t count =
      readMapCount(c, op.readFraming, op.readKeyType, op.readValueType);
  if (hasError(c)) {
    return;
  }
  writeMapHeader(c, op.writeFraming, count, op.writeKeyType, op.writeValueType);
  if (hasError(c)) {
    return;
  }
  for (uint32_t i = 0; i < count; ++i) {
    if (hasError(c)) {
      break;
    }
    execCommand(c, *op.key, 0);
    if (hasError(c)) {
      break;
    }
    execCommand(c, *op.value, 0);
  }
}

const FieldEntry* FOLLY_NULLABLE
findFieldById(const StructOp& op, int16_t fieldId) {
  auto it = std::lower_bound(
      op.fields.begin(),
      op.fields.end(),
      fieldId,
      [](const FieldEntry& field, int16_t id) { return field.fieldId < id; });
  if (it == op.fields.end() || it->fieldId != fieldId) {
    return nullptr;
  }
  return &*it;
}

void patchDelimitedStruct(
    TranscodeCursor* c, const StructOp& op, TranscodePatchPoint writeMark) {
  if (!op.writeLengthDelimited || hasError(c)) {
    return;
  }
  thrift_transcode_proto_patch_length(c, writeMark);
}

void execIdStructToFieldFramed(
    TranscodeCursor* c,
    const StructOp& op,
    FieldProto readProto,
    const Framing& rf,
    ScalarFieldOverrides fieldOverrides) {
  FieldProto writeProto = op.writeFieldProto;
  if (writeProto == FieldProto::Unsupported) {
    detail::setError(c, kUnsupportedProtocol);
    return;
  }
  Framing wf = framingFor(writeProto);

  const uint8_t* savedReadEnd = nullptr;
  if (!enterIdStructRead(c, op, savedReadEnd)) [[unlikely]] {
    return;
  }

  TranscodePatchPoint writeMark{};
  if (op.writeLengthDelimited) {
    writeMark = thrift_transcode_proto_reserve_length(c);
  }

  int16_t prevRead = 0;
  int16_t prevWrite = 0;
  const bool unionStruct = isUnion(op);
  bool unionMemberSeen = false;
  std::vector<int16_t> repeatedFieldsRead;
  while (!hasError(c)) {
    IdFieldMatch match;
    if (!readNextIdField(
            c, op, readProto, rf, prevRead, match, op.unknownFieldMode)) {
      break;
    }
    if (!noteUnionMember(c, unionStruct, unionMemberSeen)) [[unlikely]] {
      return;
    }

    if (writesProtobufOccurrences(*match.field->command)) {
      writeProtobufOccurrences(c, *match.field->command, *match.field);
      continue;
    }
    if (readsProtobufOccurrences(*match.field)) {
      wf.writeHeader(
          c,
          match.field->writeTypeInfo,
          targetFieldId(*match.field),
          prevWrite);
      prevWrite = targetFieldId(*match.field);
      readProtobufOccurrences(
          c, *match.field, match.typeInfo, repeatedFieldsRead);
      continue;
    }

    if (const auto* sc = std::get_if<ScalarOp>(match.field->command.get());
        sc != nullptr && sc->writeFn == WriteFn::CompactBoolInType) {
      int64_t boolVal = 0;
      if (!readScalarInt(c, *sc, match.typeInfo, &boolVal)) [[unlikely]] {
        return;
      }
      thrift_transcode_compact_write_bool_field(
          c, boolVal ? 1 : 0, targetFieldId(*match.field), prevWrite);
      prevWrite = targetFieldId(*match.field);
      continue;
    }

    wf.writeHeader(
        c, match.field->writeTypeInfo, targetFieldId(*match.field), prevWrite);
    if (hasError(c)) {
      break;
    }
    prevWrite = targetFieldId(*match.field);
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
    if (!writeFieldFramedScalarValue(
            c, wf, *field, *scalar, override.value, prevWrite)) [[unlikely]] {
      return;
    }
  }

  if (!finishUnion(c, unionStruct, unionMemberSeen)) [[unlikely]] {
    return;
  }
  if (!op.writeLengthDelimited) {
    wf.writeStop(c);
  }
  patchDelimitedStruct(c, op, writeMark);
}

void execIdStructFlattened(
    TranscodeCursor* c,
    const StructOp& op,
    FieldProto readProto,
    const Framing& rf) {
  const uint8_t* savedReadEnd = nullptr;
  if (!enterIdStructRead(c, op, savedReadEnd)) [[unlikely]] {
    return;
  }
  auto restoreReadEnd =
      folly::makeGuard([&] { restoreIdStructReadEnd(c, op, savedReadEnd); });

  int16_t prevRead = 0;
  bool fieldSeen = false;
  while (!hasError(c)) {
    IdFieldMatch match;
    if (!readNextIdField(
            c, op, readProto, rf, prevRead, match, UnknownFieldMode::Reject)) {
      break;
    }
    if (!execFlattenedField(c, *match.field, match.typeInfo, fieldSeen))
        [[unlikely]] {
      return;
    }
  }
  if (!hasError(c)) {
    if (!finishSingleField(c, fieldSeen)) [[unlikely]] {
      return;
    }
  }
}

void execStruct(
    TranscodeCursor* c,
    const StructOp& op,
    ScalarFieldOverrides fieldOverrides = {}) {
  if (op.outputMode == StructOutputMode::Flattened) {
    if (!fieldOverrides.empty()) {
      detail::setError(c, kMalformedFieldType);
      return;
    }
    if (op.fieldIdent == FieldIdent::ByName) {
      execJsonStructFlattened(c, op);
      return;
    }
    FieldProto readProto = op.readFieldProto;
    if (readProto == FieldProto::Unsupported) {
      detail::setError(c, kUnsupportedProtocol);
      return;
    }
    execIdStructFlattened(c, op, readProto, framingFor(readProto));
    return;
  }

  if (op.fieldIdent == FieldIdent::ByName) {
    execJsonStruct(c, op, fieldOverrides);
    return;
  }

  FieldProto readProto = op.readFieldProto;
  if (readProto == FieldProto::Unsupported) {
    detail::setError(c, kUnsupportedProtocol);
    return;
  }
  Framing rf = framingFor(readProto);
  if (op.writeFieldIdent == FieldIdent::ByName) {
    execIdStructToJson(c, op, readProto, rf, fieldOverrides);
    return;
  }
  execIdStructToFieldFramed(c, op, readProto, rf, fieldOverrides);
}

void execRootCommand(
    TranscodeCursor* c,
    const Command& cmd,
    ScalarFieldOverrides topLevelScalarOverrides) {
  if (topLevelScalarOverrides.empty()) {
    execCommand(c, cmd, 0);
    return;
  }
  const auto* st = std::get_if<StructOp>(&cmd);
  if (st == nullptr) {
    detail::setError(c, kMalformedFieldType);
    return;
  }
  execStruct(c, *st, topLevelScalarOverrides);
}

} // namespace

Framing framingFor(FieldProto p) {
  switch (p) {
    case FieldProto::Compact:
      return {
          &thrift_transcode_compact_read_field_header,
          &thrift_transcode_compact_write_field_header,
          &thrift_transcode_compact_write_stop,
          &thrift_transcode_compact_skip_field};
    case FieldProto::Binary:
      return {
          &thrift_transcode_binary_read_field_header,
          &thrift_transcode_binary_write_field_header,
          &thrift_transcode_binary_write_stop,
          &thrift_transcode_binary_skip_field};
    case FieldProto::Protobuf:
      return {
          &thrift_transcode_proto_read_field_header,
          &thrift_transcode_proto_write_field_header,
          &thrift_transcode_proto_write_stop,
          &thrift_transcode_proto_skip_field};
    case FieldProto::Unsupported:
      return {};
  }
  return {};
}

bool intFits(ValueKind kind, int64_t v) {
  switch (kind) {
    case ValueKind::I8:
      return v >= std::numeric_limits<int8_t>::min() &&
          v <= std::numeric_limits<int8_t>::max();
    case ValueKind::I16:
      return v >= std::numeric_limits<int16_t>::min() &&
          v <= std::numeric_limits<int16_t>::max();
    case ValueKind::I32:
    case ValueKind::Enum:
      return v >= std::numeric_limits<int32_t>::min() &&
          v <= std::numeric_limits<int32_t>::max();
    case ValueKind::Bool:
    case ValueKind::I64:
      return true;
    case ValueKind::F32:
    case ValueKind::F64:
    case ValueKind::Bytes:
      folly::assume_unreachable();
  }
  return false;
}

bool writeScalarInt(TranscodeCursor* c, const ScalarOp& op, int64_t v) {
  switch (op.writeFn) {
    case WriteFn::ZigzagVarint:
      thrift_transcode_write_zigzag_varint(c, v);
      break;
    case WriteFn::UnsignedVarint:
      thrift_transcode_write_unsigned_varint(c, static_cast<uint64_t>(v));
      break;
    case WriteFn::Fixed8:
      thrift_transcode_write_byte_checked(c, static_cast<uint8_t>(v));
      break;
    case WriteFn::Fixed16BE:
      thrift_transcode_write_fixed16_be_checked(c, static_cast<uint16_t>(v));
      break;
    case WriteFn::Fixed32BE:
      thrift_transcode_write_fixed32_be_checked(c, static_cast<uint32_t>(v));
      break;
    case WriteFn::Fixed64BE:
      thrift_transcode_write_fixed64_be_checked(c, static_cast<uint64_t>(v));
      break;
    case WriteFn::Fixed32LE:
      thrift_transcode_write_fixed32_le_checked(c, static_cast<uint32_t>(v));
      break;
    case WriteFn::Fixed64LE:
      thrift_transcode_write_fixed64_le_checked(c, static_cast<uint64_t>(v));
      break;
    case WriteFn::ByteAsBool:
      thrift_transcode_write_byte_checked(c, v ? 1 : 0);
      break;
    case WriteFn::VarintAsBool:
      thrift_transcode_write_unsigned_varint(c, v ? 1 : 0);
      break;
    case WriteFn::IntToDecimalText:
      thrift_transcode_format_decimal_int(c, v);
      break;
    case WriteFn::EnumNameOrDecimalText:
      if (op.enumNames != nullptr) {
        if (const auto* name = op.enumNames->nameFor(static_cast<int32_t>(v))) {
          thrift_transcode_format_escaped_string(
              c, reinterpret_cast<const uint8_t*>(name->data()), name->size());
          break;
        }
      }
      thrift_transcode_format_decimal_int(c, v);
      break;
    case WriteFn::BoolToKeyword:
      if (v != 0) {
        thrift_transcode_write_raw_bytes_checked(
            c, reinterpret_cast<const uint8_t*>("true"), 4);
      } else {
        thrift_transcode_write_raw_bytes_checked(
            c, reinterpret_cast<const uint8_t*>("false"), 5);
      }
      break;
    // Not integer writers, so an integer value never routes here:
    // CompactBoolInType is folded into the field header by the StructOp
    // executor; bytes/float and struct-memory writers are dispatched by
    // writeScalarBytes / writeScalarFloat and the struct path.
    case WriteFn::CompactBoolInType:
    case WriteFn::LengthPrefixedVarint:
    case WriteFn::LengthPrefixedI32:
    case WriteFn::WriteQuotedString:
    case WriteFn::WriteBase64String:
    case WriteFn::FloatToDecimalText:
    case WriteFn::StoreAtOffset:
    case WriteFn::CallTypeInfoSet:
    case WriteFn::Custom:
      folly::assume_unreachable();
  }
  return !hasError(c);
}

bool writeScalarBytes(
    TranscodeCursor* c, WriteFn fn, const uint8_t* data, size_t len) {
  switch (fn) {
    case WriteFn::LengthPrefixedVarint:
      thrift_transcode_write_varint_prefixed(c, data, len);
      break;
    case WriteFn::LengthPrefixedI32:
      thrift_transcode_write_i32_prefixed(c, data, len);
      break;
    case WriteFn::WriteQuotedString:
      thrift_transcode_format_escaped_string(c, data, len);
      break;
    case WriteFn::WriteBase64String:
      thrift_transcode_format_base64_string(c, data, len);
      break;
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
      folly::assume_unreachable();
  }
  return !hasError(c);
}

bool writeScalarValue(
    TranscodeCursor* c, const ScalarOp& op, const ScalarOverrideValue& value) {
  switch (op.valueKind) {
    case ValueKind::Bool: {
      int64_t v = 0;
      if (!getBoolOverride(c, value, v)) [[unlikely]] {
        return false;
      }
      return writeScalarInt(c, op, v);
    }
    case ValueKind::I8:
    case ValueKind::I16:
    case ValueKind::I32:
    case ValueKind::I64:
    case ValueKind::Enum: {
      int64_t v = 0;
      getIntegerOverride(value, v);
      if (!intFits(op.valueKind, v)) [[unlikely]] {
        detail::setError(c, kMalformedFieldType);
        return false;
      }
      return writeScalarInt(c, op, v);
    }
    case ValueKind::F32:
    case ValueKind::F64: {
      return writeScalarFloat(c, op.writeFn, value.as<double>());
    }
    case ValueKind::Bytes: {
      const auto bytes = value.as<folly::ByteRange>();
      const uint8_t empty = 0;
      const auto* data = reinterpret_cast<const uint8_t*>(bytes.data());
      return writeScalarBytes(
          c, op.writeFn, data == nullptr ? &empty : data, bytes.size());
    }
  }
  folly::assume_unreachable();
}

void execScalar(TranscodeCursor* c, const ScalarOp& op, uint8_t fieldTypeInfo) {
  if (readFnIsJsonBytes(op.readFn)) {
    execJsonBytesScalar(c, op);
    return;
  }
  if (readFnIsBytes(op.readFn)) {
    const uint8_t* data = nullptr;
    size_t len = 0;
    if (!readScalarBytes(c, op.readFn, &data, &len)) [[unlikely]] {
      return;
    }
    if (data == nullptr && len != 0) {
      detail::setError(c, kMalformedFieldType);
      return;
    }
    const uint8_t empty = 0;
    if (!writeScalarBytes(c, op.writeFn, data == nullptr ? &empty : data, len))
        [[unlikely]] {
      return;
    }
    return;
  }
  if (op.valueKind == ValueKind::F32 || op.valueKind == ValueKind::F64) {
    double v = 0;
    if (!readScalarFloat(c, op.readFn, &v)) [[unlikely]] {
      return;
    }
    if (!writeScalarFloat(c, op.writeFn, v)) [[unlikely]] {
      return;
    }
    return;
  }
  int64_t v = 0;
  if (!readScalarInt(c, op, fieldTypeInfo, &v)) [[unlikely]] {
    return;
  }
  if (!intFits(op.valueKind, v)) [[unlikely]] {
    detail::setError(c, 1);
    return;
  }
  // CoerceOp is a no-op for our int64 register model (WidenI32ToI64 etc. are
  // already represented as int64); float widening is handled in the F32/F64
  // path above.
  if (!writeScalarInt(c, op, v)) [[unlikely]] {
    return;
  }
}

void writeMapHeader(
    TranscodeCursor* c,
    ContainerFraming framing,
    uint32_t count,
    uint8_t keyType,
    uint8_t valueType) {
  switch (framing) {
    case ContainerFraming::Compact:
      if (count == 0) {
        thrift_transcode_write_byte_checked(c, 0);
      } else {
        thrift_transcode_write_unsigned_varint(c, count);
        thrift_transcode_write_byte_checked(
            c, static_cast<uint8_t>((keyType << 4) | (valueType & 0x0F)));
      }
      break;
    case ContainerFraming::Binary:
      thrift_transcode_cursor_ensure_write(c, 6);
      if (hasError(c)) {
        return;
      }
      thrift_transcode_write_byte_unchecked(c, keyType);
      thrift_transcode_write_byte_unchecked(c, valueType);
      thrift_transcode_write_fixed32_be_unchecked(c, count);
      break;
    case ContainerFraming::Json:
    case ContainerFraming::None:
      break;
  }
}

TranscodePatchPoint reserveSeqHeader(
    TranscodeCursor* c, ContainerFraming framing) {
  TranscodePatchPoint writeMark = thrift_transcode_cursor_mark(c);
  switch (framing) {
    case ContainerFraming::Compact:
      thrift_transcode_cursor_skip(c, 6); // escape byte + 5-byte varint count
      break;
    case ContainerFraming::Binary:
      thrift_transcode_cursor_skip(c, 5); // elem-type byte + i32 BE count
      break;
    case ContainerFraming::Json:
    case ContainerFraming::None:
      break;
  }
  return writeMark;
}

void patchSeqHeader(
    TranscodeCursor* c,
    TranscodePatchPoint writeMark,
    ContainerFraming framing,
    uint32_t count,
    uint8_t elemType) {
  switch (framing) {
    case ContainerFraming::Compact:
      thrift_transcode_cursor_patch_byte(
          c, writeMark, static_cast<uint8_t>(0xF0 | elemType));
      thrift_transcode_cursor_patch_varint(
          c,
          thrift_transcode_cursor_offset_patch_point(writeMark, 1),
          count,
          5);
      break;
    case ContainerFraming::Binary:
      thrift_transcode_cursor_patch_byte(c, writeMark, elemType);
      thrift_transcode_cursor_patch_i32_be(
          c,
          thrift_transcode_cursor_offset_patch_point(writeMark, 1),
          static_cast<int32_t>(count));
      break;
    case ContainerFraming::Json:
    case ContainerFraming::None:
      break;
  }
}

TranscodePatchPoint reserveNonEmptyMapHeader(
    TranscodeCursor* c, ContainerFraming framing) {
  TranscodePatchPoint writeMark = thrift_transcode_cursor_mark(c);
  switch (framing) {
    case ContainerFraming::Compact:
    case ContainerFraming::Binary:
      thrift_transcode_cursor_skip(c, 6);
      break;
    case ContainerFraming::Json:
    case ContainerFraming::None:
      break;
  }
  return writeMark;
}

void patchNonEmptyMapHeader(
    TranscodeCursor* c,
    TranscodePatchPoint writeMark,
    ContainerFraming framing,
    uint32_t count,
    uint8_t keyType,
    uint8_t valueType) {
  switch (framing) {
    case ContainerFraming::Compact:
      thrift_transcode_cursor_patch_varint(c, writeMark, count, 5);
      thrift_transcode_cursor_patch_byte(
          c,
          thrift_transcode_cursor_offset_patch_point(writeMark, 5),
          static_cast<uint8_t>((keyType << 4) | (valueType & 0x0F)));
      break;
    case ContainerFraming::Binary:
      thrift_transcode_cursor_patch_byte(c, writeMark, keyType);
      thrift_transcode_cursor_patch_byte(
          c,
          thrift_transcode_cursor_offset_patch_point(writeMark, 1),
          valueType);
      thrift_transcode_cursor_patch_i32_be(
          c,
          thrift_transcode_cursor_offset_patch_point(writeMark, 2),
          static_cast<int32_t>(count));
      break;
    case ContainerFraming::Json:
    case ContainerFraming::None:
      break;
  }
}

int16_t targetFieldId(const FieldEntry& field) {
  return field.writeFieldId.value_or(field.fieldId);
}

bool isUnion(const StructOp& op) {
  return op.schemaType.has_value() && op.schemaType->isUnion();
}

bool noteSingleField(TranscodeCursor* c, bool& fieldSeen) {
  if (fieldSeen) {
    detail::setError(c, kMalformedFieldType);
    return false;
  }
  fieldSeen = true;
  return true;
}

bool finishSingleField(TranscodeCursor* c, bool fieldSeen) {
  if (!fieldSeen) {
    detail::setError(c, kMalformedFieldType);
    return false;
  }
  return true;
}

bool noteUnionMember(
    TranscodeCursor* c, bool unionStruct, bool& unionMemberSeen) {
  if (!unionStruct) {
    return true;
  }
  return noteSingleField(c, unionMemberSeen);
}

bool finishUnion(TranscodeCursor* c, bool unionStruct, bool unionMemberSeen) {
  if (!unionStruct) {
    return true;
  }
  return finishSingleField(c, unionMemberSeen);
}

bool resolveScalarOverrideField(
    TranscodeCursor* c,
    const StructOp& op,
    const ScalarFieldOverride& override,
    const FieldEntry*& field,
    const ScalarOp*& scalar) {
  field = findFieldById(op, override.fieldId);
  if (field == nullptr) {
    detail::setError(c, kMalformedFieldType);
    return false;
  }
  if (field->isRepeated) {
    detail::setError(c, kMalformedFieldType);
    return false;
  }
  scalar = std::get_if<ScalarOp>(field->command.get());
  if (scalar == nullptr) {
    detail::setError(c, kMalformedFieldType);
    return false;
  }
  return true;
}

bool writeFieldFramedScalarValue(
    TranscodeCursor* c,
    const Framing& wf,
    const FieldEntry& field,
    const ScalarOp& scalar,
    const ScalarOverrideValue& value,
    int16_t& prevWrite) {
  if (scalar.writeFn == WriteFn::CompactBoolInType) {
    int64_t boolVal = 0;
    if (!getBoolOverride(c, value, boolVal)) [[unlikely]] {
      return false;
    }
    thrift_transcode_compact_write_bool_field(
        c, boolVal ? 1 : 0, targetFieldId(field), prevWrite);
    prevWrite = targetFieldId(field);
    return !hasError(c);
  }

  wf.writeHeader(c, field.writeTypeInfo, targetFieldId(field), prevWrite);
  if (hasError(c)) {
    return false;
  }
  prevWrite = targetFieldId(field);
  return writeScalarValue(c, scalar, value);
}

bool enterIdStructRead(
    TranscodeCursor* c, const StructOp& op, const uint8_t*& savedReadEnd) {
  savedReadEnd = nullptr;
  if (!op.readLengthDelimited) {
    return true;
  }
  uint64_t len = thrift_transcode_read_unsigned_varint(c);
  if (hasError(c)) {
    return false;
  }
  savedReadEnd = c->readEnd;
  return setReadEndFromByteLength(*c, len);
}

void restoreIdStructReadEnd(
    TranscodeCursor* c,
    const StructOp& op,
    const uint8_t* FOLLY_NULLABLE savedReadEnd) {
  if (op.readLengthDelimited && savedReadEnd != nullptr) {
    c->readEnd = savedReadEnd;
  }
}

bool readNextIdField(
    TranscodeCursor* c,
    const StructOp& op,
    FieldProto readProto,
    const Framing& rf,
    int16_t& prevRead,
    IdFieldMatch& match,
    UnknownFieldMode unknownFieldMode) {
  while (true) {
    int16_t fieldId = 0;
    uint8_t typeInfo = rf.readHeader(c, &fieldId, prevRead);
    if (typeInfo == 0 || hasError(c)) {
      return false;
    }
    prevRead = fieldId;

    const FieldEntry* field = findFieldById(op, fieldId);
    if (field == nullptr) {
      if (unknownFieldMode == UnknownFieldMode::Reject) {
        detail::setError(c, kMalformedFieldType);
        return false;
      }
      rf.skip(c, typeInfo);
      if (hasError(c)) {
        return false;
      }
      continue;
    }
    if (field->command == nullptr ||
        !fieldTypeMatches(readProto, *field, typeInfo)) [[unlikely]] {
      detail::setError(c, kMalformedFieldType);
      return false;
    }
    match = IdFieldMatch{fieldId, typeInfo, field};
    return true;
  }
}

bool execFlattenedField(
    TranscodeCursor* c,
    const FieldEntry& field,
    uint8_t typeInfo,
    bool& fieldSeen) {
  if (!noteSingleField(c, fieldSeen)) [[unlikely]] {
    return false;
  }
  execCommand(c, *field.command, typeInfo);
  return !hasError(c);
}

void execCommand(
    TranscodeCursor* c, const Command& cmd, uint8_t fieldTypeInfo) {
  if (hasError(c)) {
    return;
  }
  std::visit(
      [c, fieldTypeInfo](const auto& op) {
        using T = std::decay_t<decltype(op)>;
        if constexpr (std::is_same_v<T, ScalarOp>) {
          execScalar(c, op, fieldTypeInfo);
        } else if constexpr (std::is_same_v<T, SeqOp>) {
          execSeq(c, op);
        } else if constexpr (std::is_same_v<T, MapOp>) {
          execMap(c, op);
        } else if constexpr (std::is_same_v<T, StructOp>) {
          execStruct(c, op);
        } else {
          detail::setError(c, kUnsupportedProtocol);
        }
      },
      cmd);
}

} // namespace detail

TranscodeInterpreter::TranscodeInterpreter(TranscodePlan plan)
    : plan_(std::move(plan)) {}

folly::Expected<std::unique_ptr<folly::IOBuf>, TranscodeError>
TranscodeInterpreter::transcode(
    const folly::IOBuf& input,
    ScalarFieldOverrides topLevelScalarOverrides) const {
  // TODO @sadroeck - Support chained input buffers without coalescing
  // Note: this is part of the "streaming" input support.
  auto coalesced = input.isChained()
      ? input.cloneCoalescedAsValue()
      : folly::IOBuf::wrapBufferAsValue(input.data(), input.length());

  size_t capacity = 0;
  if (!checkedAdd(coalesced.length(), coalesced.length(), capacity) ||
      !checkedAdd(capacity, 64, capacity)) {
    return folly::makeUnexpected(
        TranscodeError{TranscodeErrc::Oom, "interpreter: output too large"});
  }
  MallocOutput output;
  output.capacity = capacity;
  output.buffer = static_cast<uint8_t*>(malloc(capacity));
  if (output.buffer == nullptr) {
    return folly::makeUnexpected(
        TranscodeError{TranscodeErrc::Oom, "interpreter: oom"});
  }
  auto bufferGuard = folly::makeGuard([&] { free(output.buffer); });

  TranscodeCursor cursor{};
  TranscodeByteRange inputRange{
      coalesced.data(), coalesced.data() + coalesced.length()};
  thrift_transcode_cursor_init(
      &cursor,
      inputRange,
      {output.buffer, output.buffer + output.capacity},
      mallocExtend,
      nullptr,
      &output);

  detail::execRootCommand(&cursor, plan_.root, topLevelScalarOverrides);
  detail::validateInputConsumed(&cursor, plan_);

  if (cursor.error != 0) {
    return folly::makeUnexpected(
        TranscodeError{
            errcFromCursor(cursor.error), "interpreter returned error"});
  }

  size_t len = thrift_transcode_cursor_bytes_written(&cursor);
  bufferGuard.dismiss();
  return folly::IOBuf::takeOwnership(
      output.buffer, len, [](void* p, void*) { free(p); });
}

folly::Expected<size_t, TranscodeError> TranscodeInterpreter::transcodeInto(
    const folly::IOBuf& input,
    uint8_t* output,
    size_t outputCapacity,
    ScalarFieldOverrides topLevelScalarOverrides) const {
  auto coalesced = input.isChained()
      ? input.cloneCoalescedAsValue()
      : folly::IOBuf::wrapBufferAsValue(input.data(), input.length());

  TranscodeCursor cursor{};
  TranscodeByteRange inputRange{
      coalesced.data(), coalesced.data() + coalesced.length()};
  thrift_transcode_cursor_init(
      &cursor,
      inputRange,
      {output, output + outputCapacity},
      fixedOutputExtend,
      nullptr,
      nullptr);

  detail::execRootCommand(&cursor, plan_.root, topLevelScalarOverrides);
  detail::validateInputConsumed(&cursor, plan_);

  if (cursor.error != 0) {
    return folly::makeUnexpected(
        TranscodeError{
            errcFromCursor(cursor.error), "interpreter returned error"});
  }
  return thrift_transcode_cursor_bytes_written(&cursor);
}

} // namespace apache::thrift::transcode
