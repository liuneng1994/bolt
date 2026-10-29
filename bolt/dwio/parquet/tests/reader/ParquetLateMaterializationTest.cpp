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

#include <gtest/gtest.h>

#include "bolt/dwio/parquet/reader/ParquetLateMaterialization.h"

namespace bytedance::bolt::parquet {
namespace {

using Phase = ParquetLateMaterializationState::Phase;
using Strategy = ParquetLateMaterializationState::Strategy;

TEST(ParquetLateMaterializationTest, eagerStateTransitions) {
  ParquetLateMaterializationState state(Strategy::kEagerCoalesced);
  EXPECT_EQ(state.phase(), Phase::kPlanInitial);

  state.initialLoaded();
  state.beginBatch();
  EXPECT_EQ(state.phase(), Phase::kAwaitingSelection);
  EXPECT_FALSE(state.beginOutput());
  EXPECT_EQ(state.phase(), Phase::kOutputReady);
  EXPECT_FALSE(state.beginOutput());

  state.beginBatch();
  EXPECT_FALSE(state.beginOutput());
  state.finish();
  EXPECT_EQ(state.phase(), Phase::kFinished);
}

TEST(ParquetLateMaterializationTest, twoPhaseLoadsOutputOnce) {
  ParquetLateMaterializationState state(Strategy::kTwoPhase);
  state.initialLoaded();
  state.beginBatch();
  EXPECT_TRUE(state.beginOutput());
  EXPECT_EQ(state.phase(), Phase::kLoadingOutput);
  state.outputLoaded();
  EXPECT_EQ(state.phase(), Phase::kOutputReady);
  EXPECT_FALSE(state.beginOutput());

  state.beginBatch();
  EXPECT_FALSE(state.beginOutput());
  EXPECT_EQ(state.phase(), Phase::kOutputReady);
  state.finish();
}

TEST(ParquetLateMaterializationTest, unusedOutputDoesNotBlockNextBatch) {
  ParquetLateMaterializationState state(Strategy::kTwoPhase);
  state.initialLoaded();
  state.beginBatch();
  EXPECT_EQ(state.phase(), Phase::kAwaitingSelection);
  state.beginBatch();
  EXPECT_EQ(state.phase(), Phase::kAwaitingSelection);
  state.finish();
}

TEST(ParquetLateMaterializationTest, adaptivePolicyUsesHysteresis) {
  ParquetLateMaterializationPolicy policy;
  EXPECT_EQ(policy.strategy(), Strategy::kEagerCoalesced);

  policy.observeSelection(79, 100);
  EXPECT_EQ(policy.strategy(), Strategy::kTwoPhase);

  policy.observeSelection(85, 100);
  EXPECT_EQ(policy.strategy(), Strategy::kTwoPhase);

  policy.observeSelection(90, 100);
  EXPECT_EQ(policy.strategy(), Strategy::kEagerCoalesced);
}

TEST(ParquetLateMaterializationTest, scatteredRowsStayEager) {
  ParquetLateMaterializationPolicy policy;
  policy.observeSelection(1, 100, 100);
  EXPECT_EQ(policy.selectedPct(), 1);
  EXPECT_EQ(policy.selectedSpanPct(), 100);
  EXPECT_EQ(policy.strategy(), Strategy::kEagerCoalesced);
}

TEST(ParquetLateMaterializationTest, offsetIndexAllowsScatteredRows) {
  ParquetLateMaterializationPolicy policy;
  policy.observeSelection(1, 100, 100, true);
  EXPECT_EQ(policy.selectedPct(), 1);
  EXPECT_EQ(policy.selectedSpanPct(), 100);
  EXPECT_EQ(policy.outputCoveragePct(), 1);
  EXPECT_EQ(policy.strategy(), Strategy::kTwoPhase);
}

} // namespace
} // namespace bytedance::bolt::parquet
