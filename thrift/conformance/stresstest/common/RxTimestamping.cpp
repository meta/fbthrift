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

#include <thrift/conformance/stresstest/common/RxTimestamping.h>

#include <array>
#include <ctime>

#include <linux/errqueue.h>
#include <linux/net_tstamp.h>

#include <glog/logging.h>
#include <folly/io/SocketOptionMap.h>
#include <folly/io/async/AsyncSocket.h>

namespace apache::thrift::stress {
namespace {

constexpr int kRxTimestampingFlags =
    SOF_TIMESTAMPING_SOFTWARE | SOF_TIMESTAMPING_RX_SOFTWARE;
constexpr auto kMaxCollect = std::chrono::seconds{10};
constexpr size_t kTimestampsSize = sizeof(struct scm_timestamping);
// io_uring multishot recvmsg carves the control area out of every provided
// buffer, so anything beyond the SO_TIMESTAMPING cmsg shrinks the payload.
constexpr size_t kControlBufferSize = CMSG_SPACE(kTimestampsSize);

std::chrono::nanoseconds toDuration(const struct timespec& ts) noexcept {
  return std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec};
}

std::chrono::nanoseconds realtimeNow() noexcept {
  struct timespec now{};
  if (::clock_gettime(CLOCK_REALTIME, &now) != 0) {
    PLOG_EVERY_N(WARNING, 100)
        << "clock_gettime(CLOCK_REALTIME) failed; skipping RX timestamp collect";
    return std::chrono::nanoseconds{0};
  }
  return toDuration(now);
}

class RxTimestampingCallback final
    : public RxTimestampingRegistration,
      public folly::AsyncSocket::ReadAncillaryDataCallback {
 public:
  RxTimestampingCallback(folly::AsyncSocket& socket, RxTimestampSink sink)
      : socketGuard_(&socket), socket_(socket), sink_(std::move(sink)) {}

  ~RxTimestampingCallback() override {
    DCHECK(
        socket_.getEventBase() == nullptr ||
        socket_.getEventBase()->isInEventBaseThread());
    if (socket_.getReadAncillaryDataCallback() == this) {
      socket_.setReadAncillaryDataCB(nullptr);
    }
  }

  folly::Expected<folly::Unit, folly::AsyncSocketException> ancillaryData(
      struct ::msghdr& msg) noexcept override {
    if (msg.msg_flags & MSG_CTRUNC) {
      // Losing optional timestamps must not fail the associated payload read.
      msg.msg_flags &= ~MSG_CTRUNC;
      return folly::unit;
    }
    if (!sink_) {
      return folly::unit;
    }
    if (auto collect = detail::getRxTimestampCollect(msg, realtimeNow())) {
      sink_(*collect);
    }
    return folly::unit;
  }

  folly::MutableByteRange getAncillaryDataCtrlBuffer() override {
    return folly::MutableByteRange{controlBuffer_};
  }

 private:
  // Released last so cleanup remains safe after logical socket destruction.
  folly::AsyncSocket::DestructorGuard socketGuard_;
  folly::AsyncSocket& socket_;
  RxTimestampSink sink_;
  alignas(cmsghdr) std::array<uint8_t, kControlBufferSize> controlBuffer_{};
};

} // namespace

namespace detail {

std::optional<std::chrono::microseconds> getRxTimestampCollect(
    struct ::msghdr& msg, std::chrono::nanoseconds now) noexcept {
  if (now == std::chrono::nanoseconds{0} || (msg.msg_flags & MSG_CTRUNC)) {
    return std::nullopt;
  }

  for (auto* cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr;
       cmsg = CMSG_NXTHDR(&msg, cmsg)) {
    if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SO_TIMESTAMPING ||
        cmsg->cmsg_len < CMSG_LEN(kTimestampsSize)) {
      continue;
    }

    const auto* timestamps =
        reinterpret_cast<const struct timespec*>(CMSG_DATA(cmsg));
    const auto kernelRx = toDuration(timestamps[0]);
    const auto collect = now - kernelRx;
    if (kernelRx != std::chrono::nanoseconds{0} &&
        collect > std::chrono::nanoseconds{0} && collect < kMaxCollect) {
      return std::chrono::duration_cast<std::chrono::microseconds>(collect);
    }
  }
  return std::nullopt;
}

} // namespace detail

RxTimestampingRegistration::~RxTimestampingRegistration() = default;

std::unique_ptr<RxTimestampingRegistration> installRxTimestamping(
    folly::AsyncSocket& socket, RxTimestampSink sink) noexcept {
  DCHECK(
      socket.getEventBase() == nullptr ||
      socket.getEventBase()->isInEventBaseThread());
  if (socket.getReadAncillaryDataCallback() != nullptr) {
    LOG(WARNING) << "Cannot enable RX timestamping: the socket already has an "
                    "ancillary-data callback";
    return nullptr;
  }

  int timestampingFlags = 0;
  socklen_t timestampingFlagsSize = sizeof(timestampingFlags);
  if (socket.getSockOpt(
          SOL_SOCKET,
          SO_TIMESTAMPING,
          &timestampingFlags,
          &timestampingFlagsSize) != 0) {
    PLOG(WARNING) << "Cannot read existing SO_TIMESTAMPING flags";
    return nullptr;
  }

  folly::SocketOptionKey key{SOL_SOCKET, SO_TIMESTAMPING};
  if ((timestampingFlags & kRxTimestampingFlags) != kRxTimestampingFlags &&
      key.apply(
          socket.getNetworkSocket(),
          timestampingFlags | kRxTimestampingFlags) != 0) {
    PLOG(WARNING) << "Cannot enable RX SO_TIMESTAMPING";
    return nullptr;
  }

  auto registration =
      std::make_unique<RxTimestampingCallback>(socket, std::move(sink));
  socket.setReadAncillaryDataCB(registration.get());
  return registration;
}

} // namespace apache::thrift::stress
