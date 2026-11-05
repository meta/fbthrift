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

// Each benchmark runs at doubling sizes so the output reads as a scaling
// curve: a row ~2x the previous one is linear, ~4x is quadratic.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <thrift/lib/cpp2/op/Patch.h>
#include <thrift/lib/cpp2/patch/DynamicPatch.h>
#include <thrift/lib/cpp2/protocol/Object.h>

namespace apache::thrift::protocol {
namespace {

Value makeI32List(size_t size, size_t zeros) {
  Value v;
  auto& list = v.ensure_list();
  for (size_t i = 0; i < size; ++i) {
    list.emplace_back().emplace_i32(i < zeros ? 0 : 1);
  }
  return v;
}

// `src` = [0]*(n/2) + [1] is not a prefix of `dst` = [0]*n, but n/2 elements
// match at every offset of `dst`.
void diffListNotPrefix(unsigned iters, size_t n) {
  Value src, dst;
  BENCHMARK_SUSPEND {
    src = makeI32List(n / 2 + 1, n / 2);
    dst = makeI32List(n, n);
  }
  while (iters--) {
    folly::doNotOptimizeAway(DiffVisitorBase{}.diff(src, dst));
  }
}

const std::string kPiece = "0123456789abcdef";

// Merges `patches` one at a time into the first, as a consumer folding many
// patches of one value does.
void fold(std::vector<DynamicPatch> patches) {
  DynamicPatch merged = std::move(patches.front());
  for (size_t i = 1; i < patches.size(); ++i) {
    merged.merge(std::move(patches[i]));
  }
  folly::doNotOptimizeAway(merged);
}

template <class MakePatch>
void foldPatches(unsigned iters, size_t n, MakePatch makePatch) {
  while (iters--) {
    std::vector<DynamicPatch> patches;
    BENCHMARK_SUSPEND {
      patches.reserve(n);
      for (size_t i = 0; i < n; ++i) {
        patches.push_back(makePatch(i));
      }
    }
    fold(std::move(patches));
  }
}

void foldBinaryAppend(unsigned iters, size_t n) {
  foldPatches(iters, n, [](size_t) {
    op::BinaryPatch p;
    p.append(*folly::IOBuf::copyBuffer(kPiece));
    return DynamicPatch::fromStaticPatch(p);
  });
}

void foldBinaryPrepend(unsigned iters, size_t n) {
  foldPatches(iters, n, [](size_t) {
    op::BinaryPatch p;
    p.prepend(*folly::IOBuf::copyBuffer(kPiece));
    return DynamicPatch::fromStaticPatch(p);
  });
}

// Converts a BinaryPatch that prepends and appends `len` bytes each into a
// StringPatch, as `getStoredPatchByTag<type::string_t>()` does.
void binaryToStringPatch(unsigned iters, size_t len) {
  while (iters--) {
    DynamicPatch patch;
    BENCHMARK_SUSPEND {
      op::BinaryPatch binary;
      binary.prepend(std::string(len, 'p'));
      binary.append(std::string(len, 'a'));
      patch = DynamicPatch::fromStaticPatch(binary);
    }
    folly::doNotOptimizeAway(patch.getStoredPatchByTag<type::string_t>());
  }
}

// A chain of `depth` map patches, each patching one key of the next.
DynamicPatch makeNestedMapPatch(size_t depth) {
  DynamicMapPatch leaf;
  leaf.put(
      asValueStruct<type::string_t>("key"),
      asValueStruct<type::string_t>("value"));
  DynamicPatch patch{std::move(leaf)};
  for (size_t i = 1; i < depth; ++i) {
    DynamicMapPatch parent;
    parent.patchByKey(
        asValueStruct<type::string_t>("level_" + std::to_string(i)),
        std::move(patch));
    patch = DynamicPatch{std::move(parent)};
  }
  return patch;
}

void nestedMapPatchToObject(unsigned iters, size_t depth) {
  DynamicPatch patch;
  BENCHMARK_SUSPEND {
    patch = makeNestedMapPatch(depth);
  }
  while (iters--) {
    folly::doNotOptimizeAway(patch.toObject());
  }
}

} // namespace

BENCHMARK_PARAM(diffListNotPrefix, 100)
BENCHMARK_PARAM(diffListNotPrefix, 200)
BENCHMARK_PARAM(diffListNotPrefix, 400)
BENCHMARK_PARAM(diffListNotPrefix, 800)
BENCHMARK_PARAM(diffListNotPrefix, 1600)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM(foldBinaryAppend, 100)
BENCHMARK_PARAM(foldBinaryAppend, 200)
BENCHMARK_PARAM(foldBinaryAppend, 400)
BENCHMARK_PARAM(foldBinaryAppend, 800)
BENCHMARK_PARAM(foldBinaryAppend, 1600)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM(foldBinaryPrepend, 100)
BENCHMARK_PARAM(foldBinaryPrepend, 200)
BENCHMARK_PARAM(foldBinaryPrepend, 400)
BENCHMARK_PARAM(foldBinaryPrepend, 800)
BENCHMARK_PARAM(foldBinaryPrepend, 1600)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM(binaryToStringPatch, 16)
BENCHMARK_PARAM(binaryToStringPatch, 256)
BENCHMARK_PARAM(binaryToStringPatch, 4096)
BENCHMARK_PARAM(binaryToStringPatch, 65536)
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM(nestedMapPatchToObject, 25)
BENCHMARK_PARAM(nestedMapPatchToObject, 50)
BENCHMARK_PARAM(nestedMapPatchToObject, 100)
BENCHMARK_PARAM(nestedMapPatchToObject, 200)
BENCHMARK_DRAW_LINE();

} // namespace apache::thrift::protocol

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
