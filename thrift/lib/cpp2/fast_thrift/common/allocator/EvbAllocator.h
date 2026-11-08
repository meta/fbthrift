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

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <utility>

#include <glog/logging.h>

#include <folly/Executor.h>
#include <folly/Likely.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseLocal.h>
#include <folly/lang/Align.h>
#include <folly/lang/Bits.h>
#include <folly/portability/SysMman.h>
#include <folly/portability/Unistd.h>
#include <folly/system/ThreadId.h>

#include <thrift/lib/cpp2/fast_thrift/common/allocator/EvbLocalPtr.h>
#include <thrift/lib/cpp2/fast_thrift/common/allocator/EvbSharedPtr.h>

namespace apache::thrift::fast_thrift::mem {

// ============================================================
// Page — a bump-allocated memory region.
//
// Pages are aligned to kPageSize, so the owning Page can be recovered from any
// object pointer by masking the address down to the page base.
// ============================================================

struct alignas(folly::max_align_v) Page {
  static constexpr size_t kMaxObjectSize = 64 * 1024;
  static constexpr size_t kMaxObjectAlignment = 1024 * 1024;
  static constexpr size_t kMinPageSize = 1024 * 1024;
  static constexpr size_t kPageSize = 2 * 1024 * 1024;

  static_assert(folly::isPowTwo(kPageSize));
  static_assert(folly::isPowTwo(kMaxObjectAlignment));
  static_assert(kPageSize >= kMinPageSize);

  Page* next{nullptr}; // for retired list
  Page* nextFree{nullptr}; // for free list
  folly::EventBase* evb{nullptr};
  std::byte* ptr{nullptr};
  std::byte* end{nullptr};
  uint32_t outstanding{0};
  pid_t creatorTid{0};

  std::byte* bumpStart() {
    return reinterpret_cast<std::byte*>(this) + sizeof(Page);
  }

  /// Reset page for reuse (called when popping from free list)
  void reset() {
    ptr = bumpStart();
    outstanding = 0;
    next = nullptr;
    nextFree = nullptr;
  }

  /// Raw bump allocation.
  FOLLY_ALWAYS_INLINE void* allocate(size_t bytes, size_t alignment) {
    DCHECK(folly::isPowTwo(alignment));
    auto* aligned = folly::align_ceil(ptr, alignment);
    if (aligned + bytes <= end) [[likely]] {
      ptr = aligned + bytes;
      return aligned;
    }
    return nullptr;
  }

  /// Recover the owning Page from any object pointer allocated in the page.
  static FOLLY_ALWAYS_INLINE Page* fromObject(void* obj) {
    auto* page = reinterpret_cast<Page*>(folly::align_floor(obj, kPageSize));
    DCHECK_EQ(page, folly::align_floor(page, kPageSize));
    DCHECK_GE(reinterpret_cast<std::byte*>(obj), page->bumpStart());
    DCHECK_LT(reinterpret_cast<std::byte*>(obj), page->end);
    return page;
  }

  static Page* create(folly::EventBase* evb) {
    constexpr size_t kMappingSize = kPageSize * 2;
    void* raw = mmap(
        nullptr,
        kMappingSize,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS,
        -1,
        0);
    if (raw == MAP_FAILED) {
      throw std::bad_alloc();
    }

    auto* rawBytes = static_cast<std::byte*>(raw);
    auto* alignedBytes = folly::align_ceil(rawBytes, kPageSize);
    auto prefixSize = static_cast<size_t>(alignedBytes - rawBytes);
    auto* suffix = alignedBytes + kPageSize;
    auto suffixSize = static_cast<size_t>((rawBytes + kMappingSize) - suffix);

    if (prefixSize != 0) {
      PCHECK(munmap(raw, prefixSize) == 0);
    }
    if (suffixSize != 0) {
      PCHECK(munmap(suffix, suffixSize) == 0);
    }

#if defined(MADV_HUGEPAGE)
    (void)madvise(alignedBytes, kPageSize, MADV_HUGEPAGE);
#endif
    prefault(alignedBytes, kPageSize);

    auto* page = ::new (alignedBytes) Page();
    page->evb = evb;
    page->ptr = page->bumpStart();
    page->end = reinterpret_cast<std::byte*>(page) + kPageSize;
    page->creatorTid = static_cast<pid_t>(folly::getOSThreadID());
    return page;
  }

  static void prefault(void* base, size_t size) {
    const long pageSize = sysconf(_SC_PAGESIZE);
    CHECK_GT(pageSize, 0);
    const auto osPageSize = static_cast<size_t>(pageSize);
    auto* begin = static_cast<volatile std::byte*>(base);
    for (auto* p = begin; p < begin + size; p += osPageSize) {
      *p = std::byte{0};
    }
  }

  static void destroy(Page* page) {
    page->~Page();
    munmap(page, kPageSize);
  }
};

static_assert(Page::kPageSize > Page::kMaxObjectSize + sizeof(Page));
static_assert(
    Page::kPageSize >=
    sizeof(Page) + Page::kMaxObjectAlignment - 1 + Page::kMaxObjectSize);

// Forward declarations
template <typename T>
class evb_local_ptr;
template <typename T>
class evb_shared_ptr;

// ============================================================
// EvbAllocator
// ============================================================

class EvbAllocator {
 public:
  struct Stats {
    size_t bumpBytes{0};
    size_t mappedBytes{0};
    size_t pageCount{0};
    size_t retiredPageCount{0};
    size_t freePageCount{0};
    size_t outstandingAllocations{0};
  };
  static constexpr size_t kMaxObjectSize = Page::kMaxObjectSize;
  static constexpr size_t kMaxObjectAlignment = Page::kMaxObjectAlignment;
  static constexpr size_t kMinPageSize = Page::kMinPageSize;
  static constexpr size_t kPageSize = Page::kPageSize;
  static constexpr size_t kMaxFreePages = 2;
  static constexpr size_t kPrewarmedFreePages = kMaxFreePages;

  explicit EvbAllocator(folly::EventBase* evb);
  ~EvbAllocator();

  EvbAllocator(const EvbAllocator&) = delete;
  EvbAllocator& operator=(const EvbAllocator&) = delete;
  EvbAllocator(EvbAllocator&&) = delete;
  EvbAllocator& operator=(EvbAllocator&&) = delete;

  /// Make an evb_local_ptr - fast, assumes EventBase thread affinity.
  /// The pointer will DCHECK if destroyed off the EventBase thread.
  template <typename T, typename... Args>
  FOLLY_ALWAYS_INLINE evb_local_ptr<T> make_local(Args&&... args);

  /// Make an evb_shared_ptr - safe to pass across threads.
  /// Holds a KeepAlive token to the EventBase.
  template <typename T, typename... Args>
  FOLLY_ALWAYS_INLINE evb_shared_ptr<T> make_shared(Args&&... args);

  /// Raw bump allocation.
  FOLLY_ALWAYS_INLINE void* allocate(size_t bytes, size_t alignment);

  [[nodiscard]] size_t bytesUsed() const;

  [[nodiscard]] Stats snapshotStats() const;
  [[nodiscard]] size_t pageCount() const;
  [[nodiscard]] size_t pageSize() const { return kPageSize; }
  [[nodiscard]] Page* activePage() const { return activePage_; }

  [[nodiscard]] folly::EventBase* evb() const { return evb_; }

  static EvbAllocator* tryGet(folly::EventBase& evb);
  static EvbAllocator& getOrCreate(folly::EventBase& evb);

 private:
  void allocateNewPage();
  void prewarmFreePages();
  FOLLY_NOINLINE void* allocateSlow(size_t bytes, size_t alignment);

  /// PageRecycler runs before each EventBase loop iteration to detect idle
  /// periods and reclaim empty pages. Uses a two-state machine:
  /// Active -> Idle -> Active. Reclaims on the second consecutive idle.
  class PageRecycler : public folly::EventBase::LoopCallback {
    enum State : uint8_t {
      Active, // work happened, no reclaim
      Idle, // no work last iteration, will reclaim next time if still idle
    };

   public:
    explicit PageRecycler(EvbAllocator* allocator)
        : allocator_(allocator), state_(Active) {}

    void runLoopCallback() noexcept override;

   private:
    EvbAllocator* allocator_;
    State state_;
  };

  folly::EventBase* evb_;
  Page* activePage_;
  Page* retiredHead_{nullptr};
  Page* freeHead_{nullptr};
  size_t retiredPageCount_{0};
  PageRecycler pageRecycler_{this};

  class RecyclerCleanup : public folly::EventBase::OnDestructionCallback {
   public:
    explicit RecyclerCleanup(PageRecycler& recycler) : recycler_(recycler) {}
    void onEventBaseDestruction() noexcept override {
      recycler_.cancelLoopCallback();
    }

   private:
    PageRecycler& recycler_;
  };
  RecyclerCleanup recyclerCleanup_{pageRecycler_};

 public:
  /// Reclaim pages with zero outstanding allocations.
  /// Called by PageRecycler during idle periods.
  void reclaimEmptyPages();

 private:
};

template <typename T, typename... Args>
evb_local_ptr<T> evb_make_local(folly::EventBase& evb, Args&&... args) {
  return EvbAllocator::getOrCreate(evb).make_local<T>(
      std::forward<Args>(args)...);
}

template <typename T, typename... Args>
evb_shared_ptr<T> evb_make_shared(folly::EventBase& evb, Args&&... args) {
  return EvbAllocator::getOrCreate(evb).make_shared<T>(
      std::forward<Args>(args)...);
}

// ============================================================
// Out-of-line implementations for EvbAllocator templates
// ============================================================

template <typename T, typename... Args>
FOLLY_ALWAYS_INLINE evb_local_ptr<T> EvbAllocator::make_local(Args&&... args) {
  static_assert(sizeof(T) <= kMaxObjectSize);
  static_assert(alignof(T) <= kMaxObjectAlignment);
  void* mem = allocate(sizeof(T), alignof(T));
  T* obj = ::new (mem) T(std::forward<Args>(args)...);
  auto* page = Page::fromObject(obj);
  ++page->outstanding;
  return evb_local_ptr<T>(obj);
}

template <typename T, typename... Args>
FOLLY_ALWAYS_INLINE evb_shared_ptr<T> EvbAllocator::make_shared(
    Args&&... args) {
  static_assert(sizeof(T) <= kMaxObjectSize);
  static_assert(alignof(T) <= kMaxObjectAlignment);
  void* mem = allocate(sizeof(T), alignof(T));
  T* obj = ::new (mem) T(std::forward<Args>(args)...);
  auto* page = Page::fromObject(obj);
  ++page->outstanding;
  return evb_shared_ptr<T>(obj, folly::getKeepAliveToken(evb_));
}

FOLLY_ALWAYS_INLINE void* EvbAllocator::allocate(
    size_t bytes, size_t alignment) {
  DCHECK_LE(bytes, kMaxObjectSize);
  DCHECK(folly::isPowTwo(alignment));
  DCHECK_LE(alignment, kMaxObjectAlignment);
  void* result = activePage_->allocate(bytes, alignment);
  if (result != nullptr) [[likely]] {
    return result;
  }
  return allocateSlow(bytes, alignment);
}

inline size_t EvbAllocator::bytesUsed() const {
  size_t total =
      static_cast<size_t>(activePage_->ptr - activePage_->bumpStart());
  auto* page = retiredHead_;
  while (page) {
    total += static_cast<size_t>(page->ptr - page->bumpStart());
    page = page->next;
  }
  return total;
}

inline size_t EvbAllocator::pageCount() const {
  size_t count = 1 + retiredPageCount_;
  auto* page = freeHead_;
  while (page) {
    ++count;
    page = page->nextFree;
  }
  return count;
}

// ============================================================
// Out-of-line implementations for evb_local_ptr and evb_shared_ptr
// These need Page to be fully defined
// ============================================================

template <typename T>
void evb_local_ptr<T>::reset() noexcept {
  if (ptr_) {
    auto* page = Page::fromObject(ptr_);
    // DCHECK that we're on the EventBase thread
    DCHECK(page->evb->isInEventBaseThread())
        << "evb_local_ptr destroyed off EventBase thread";
    ptr_->~T();
    --page->outstanding;
    ptr_ = nullptr;
  }
}

template <typename T>
evb_shared_ptr<T> evb_local_ptr<T>::upgrade() && {
  if (!ptr_) {
    return evb_shared_ptr<T>();
  }
  auto* page = Page::fromObject(ptr_);
  auto keepAlive = folly::getKeepAliveToken(page->evb);
  T* raw = ptr_;
  ptr_ = nullptr; // Prevent double-free
  return evb_shared_ptr<T>(raw, std::move(keepAlive));
}

template <typename T>
void evb_shared_ptr<T>::reset() noexcept {
  if (!ptr_) {
    return;
  }
  auto* page = Page::fromObject(ptr_);
  auto* evb = keepAlive_.get();

  if (evb->isInEventBaseThread()) [[likely]] {
    // Fast path: destroy inline
    ptr_->~T();
    --page->outstanding;
  } else {
    // Slow path: schedule back to EventBase
    // Move KeepAlive into lambda to keep EventBase alive
    evb->runInEventBaseThread(
        [ptr = ptr_, ka = std::move(keepAlive_)]() mutable {
          auto* page = Page::fromObject(ptr);
          ptr->~T();
          --page->outstanding;
        });
  }
  ptr_ = nullptr;
}

template <typename T>
evb_local_ptr<T> evb_shared_ptr<T>::downgrade() && {
  DCHECK(!ptr_ || keepAlive_.get()->isInEventBaseThread())
      << "downgrade() must be called on EventBase thread";
  T* raw = ptr_;
  ptr_ = nullptr;
  keepAlive_.reset(); // Release KeepAlive - local_ptr doesn't need it
  return evb_local_ptr<T>(raw);
}

} // namespace apache::thrift::fast_thrift::mem
