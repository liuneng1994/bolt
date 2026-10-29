/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <fmt/core.h>
#include <cstdint>
#include <limits>
#include <string>
namespace bytedance::bolt::dwio::common {

constexpr uint32_t MAX_UINT32 = std::numeric_limits<uint32_t>::max();

/// Describes how a stream participates in read planning.
///
/// kLegacy preserves the existing access-frequency-based planning. kFilter and
/// kOutput let readers keep low-frequency output-only streams out of loads
/// needed to evaluate filters. An output stream may still be promoted by the
/// BufferedInput implementation when its observed access frequency is high.
enum class IoReadClass : uint8_t {
  kLegacy,
  kFilter,
  kOutput,
};

class StreamIdentifier {
 public:
  StreamIdentifier() : id_(MAX_UINT32) {}

  explicit StreamIdentifier(
      int32_t id,
      IoReadClass ioReadClass = IoReadClass::kLegacy)
      : id_(id), ioReadClass_(ioReadClass) {}

  virtual ~StreamIdentifier() = default;

  virtual int32_t getId() const {
    return id_;
  }

  IoReadClass ioReadClass() const {
    return ioReadClass_;
  }

  virtual bool operator==(const StreamIdentifier& other) const {
    return id_ == other.id_;
  }

  virtual std::size_t hash() const {
    return std::hash<uint32_t>()(id_);
  }

  virtual std::string toString() const {
    return fmt::format("[id={}]", id_);
  }

  /// Returns a special value indicating a stream to be read load quantum by
  /// load quantum.
  static StreamIdentifier sequentialFile() {
    constexpr int32_t kSequentialFile = std::numeric_limits<int32_t>::max() - 1;
    return StreamIdentifier(kSequentialFile);
  }

  int32_t id_;

 private:
  IoReadClass ioReadClass_{IoReadClass::kLegacy};
};

struct StreamIdentifierHash {
  std::size_t operator()(const StreamIdentifier& si) const {
    return si.hash();
  }
};

} // namespace bytedance::bolt::dwio::common
