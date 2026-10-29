/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <algorithm>
#include <cstdint>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::parquet {

/// Driver-owned state for one Parquet row group. State advances only after the
/// corresponding operation succeeds. Errors therefore leave the state at the
/// failed operation and never expose a partially completed transition.
///
/// Legal transitions:
///   kPlanInitial -> kInitialLoaded -> kAwaitingSelection
///   kAwaitingSelection -> kLoadingOutput -> kOutputReady
///   kAwaitingSelection -> kFinished (no output is needed)
///   kOutputReady -> kFinished
class ParquetLateMaterializationState {
 public:
  enum class Phase : uint8_t {
    kPlanInitial,
    kInitialLoaded,
    kAwaitingSelection,
    kLoadingOutput,
    kOutputReady,
    kFinished,
  };

  enum class Strategy : uint8_t {
    kEagerCoalesced,
    kTwoPhase,
  };

  explicit ParquetLateMaterializationState(Strategy strategy)
      : strategy_(strategy) {}

  Phase phase() const {
    return phase_;
  }

  Strategy strategy() const {
    return strategy_;
  }

  void initialLoaded() {
    transition(Phase::kPlanInitial, Phase::kInitialLoaded);
  }

  void beginBatch() {
    if (phase_ == Phase::kAwaitingSelection) {
      // No output column from the previous batch was accessed.
      return;
    }
    BOLT_CHECK(
        phase_ == Phase::kInitialLoaded || phase_ == Phase::kOutputReady,
        "Cannot begin Parquet batch in phase {}",
        static_cast<int>(phase_));
    phase_ = Phase::kAwaitingSelection;
  }

  /// Returns true exactly once when the deferred output input must be loaded.
  bool beginOutput() {
    if (phase_ == Phase::kOutputReady) {
      return false;
    }
    if (strategy_ == Strategy::kEagerCoalesced) {
      BOLT_CHECK(
          phase_ == Phase::kAwaitingSelection,
          "Cannot make eager output ready in phase {}",
          static_cast<int>(phase_));
      phase_ = Phase::kOutputReady;
      return false;
    }
    if (outputLoaded_ && phase_ == Phase::kAwaitingSelection) {
      phase_ = Phase::kOutputReady;
      return false;
    }
    transition(Phase::kAwaitingSelection, Phase::kLoadingOutput);
    return true;
  }

  void outputLoaded() {
    transition(Phase::kLoadingOutput, Phase::kOutputReady);
    outputLoaded_ = true;
  }

  void finish() {
    BOLT_CHECK(
        phase_ == Phase::kInitialLoaded ||
            phase_ == Phase::kAwaitingSelection ||
            phase_ == Phase::kOutputReady,
        "Cannot finish Parquet row group in phase {}",
        static_cast<int>(phase_));
    phase_ = Phase::kFinished;
  }

 private:
  void transition(Phase expected, Phase next) {
    BOLT_CHECK(
        phase_ == expected,
        "Invalid Parquet late-materialization transition {} -> {}",
        static_cast<int>(phase_),
        static_cast<int>(next));
    phase_ = next;
  }

  const Strategy strategy_;
  Phase phase_{Phase::kPlanInitial};
  bool outputLoaded_{false};
};

/// Scan-local adaptive policy. Strategy changes are applied only when a new
/// row group is planned. The two thresholds provide hysteresis so selectivity
/// close to the boundary doesn't flip the I/O layout for every row group.
class ParquetLateMaterializationPolicy {
 public:
  // Row selectivity is only a broad candidate gate. The I/O cost model makes
  // the final decision from output bytes, request waves and configured RTT.
  static constexpr uint32_t kEnterTwoPhasePct = 80;
  static constexpr uint32_t kExitTwoPhasePct = 90;

  ParquetLateMaterializationState::Strategy strategy() const {
    return strategy_;
  }

  bool hasObservation() const {
    return hasObservation_;
  }

  uint32_t selectedPct() const {
    return selectedPct_;
  }

  uint32_t selectedSpanPct() const {
    return selectedSpanPct_;
  }

  uint32_t outputCoveragePct() const {
    return hasOffsetIndex_ ? selectedPct_
                           : std::max(selectedPct_, selectedSpanPct_);
  }

  void observeSelection(
      uint64_t selectedRows,
      uint64_t inputRows,
      uint64_t selectedSpanRows = 0,
      bool hasOffsetIndex = false) {
    if (inputRows == 0) {
      return;
    }
    const auto selectedPct = selectedRows * 100 / inputRows;
    if (selectedRows > 0 && selectedSpanRows == 0) {
      selectedSpanRows = selectedRows;
    }
    const auto selectedSpanPct = selectedSpanRows * 100 / inputRows;
    hasObservation_ = true;
    selectedPct_ = selectedPct;
    selectedSpanPct_ = selectedSpanPct;
    hasOffsetIndex_ = hasOffsetIndex;
    const auto coveragePct = outputCoveragePct();
    if (coveragePct < kEnterTwoPhasePct) {
      strategy_ = ParquetLateMaterializationState::Strategy::kTwoPhase;
    } else if (coveragePct >= kExitTwoPhasePct) {
      strategy_ = ParquetLateMaterializationState::Strategy::kEagerCoalesced;
    }
  }

 private:
  ParquetLateMaterializationState::Strategy strategy_{
      ParquetLateMaterializationState::Strategy::kEagerCoalesced};
  bool hasObservation_{false};
  uint32_t selectedPct_{100};
  uint32_t selectedSpanPct_{100};
  bool hasOffsetIndex_{false};
};

} // namespace bytedance::bolt::parquet
