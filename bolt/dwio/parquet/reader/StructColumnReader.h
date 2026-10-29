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

#include <limits>
#include <unordered_map>

#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/SelectiveStructColumnReader.h"
#include "bolt/dwio/parquet/arrow/LevelConversion.h"
#include "bolt/dwio/parquet/reader/ParquetLateMaterialization.h"
namespace bytedance::bolt::dwio::common {
class BufferedInput;
}
namespace bytedance::bolt::parquet {

enum class LevelMode;
class PageReader;
class ParquetParams;

class StructColumnReader : public dwio::common::SelectiveStructColumnReader {
 public:
  StructColumnReader(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec,
      memory::MemoryPool& pool);

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override;

  void seekToRowGroup(int64_t index) override;

  void prepareOutputRead(const RowSet& rows) override;

  void releaseRowGroupReader();

  /// Creates the streams for 'rowGroup'. Checks whether row 'rowGroup'
  /// has been buffered in 'input'. If true, return the input. Or else creates
  /// the streams in a new input and loads.
  std::shared_ptr<dwio::common::BufferedInput> loadRowGroup(
      uint32_t index,
      const std::shared_ptr<dwio::common::BufferedInput>& input);

  // No-op in Parquet. All readers switch row groups at the same time, there is
  // no on-demand skipping to a new row group.
  void advanceFieldReader(
      dwio::common::SelectiveColumnReader* /*reader*/,
      int64_t /*offset*/) override {}

  void setNullsFromRepDefs(PageReader& pageReader);
  const arrow::LevelInfo& levelInfo() const {
    return levelInfo_;
  }
  arrow::ValidityBitmapInputOutput prepareRepDefNulls(int32_t maxItems);
  void setNullsFromRepDefOutput(const arrow::ValidityBitmapInputOutput& bits);

  bool shouldReadChildrenEagerly() const override {
    return fileType_->isDCMap();
  }

  dwio::common::SelectiveColumnReader* FOLLY_NULLABLE childForRepDefs() const {
    return childForRepDefs_;
  }

  /// Nested struct readers all get null flags and lengths for
  /// contained repeated readers for each range of top level rows. At
  /// the end of a read() with filters in different members, some of
  /// which are structs themselves, different inner structs may be left
  /// on different rows. Before receiving the next set of
  /// nulls/lengths, the contained complex readers need to be
  /// positioned at the end of the last set of nulls/lengths.
  void seekToEndOfPresetNulls();

  void filterRowGroups(
      uint64_t rowGroupSize,
      const dwio::common::StatsContext&,
      dwio::common::FormatData::FilterRowGroupsResult&,
      dwio::common::BufferedInput& input) const override;

 private:
  static constexpr int32_t kSparseOutputLoadQuantum = 256 << 10;

  struct RowGroupReadContext {
    RowGroupReadContext(
        ParquetLateMaterializationState::Strategy strategy,
        std::shared_ptr<dwio::common::BufferedInput> outputInput)
        : state(strategy), outputInput(std::move(outputInput)) {}

    ParquetLateMaterializationState state;
    std::shared_ptr<dwio::common::BufferedInput> outputInput;
    uint64_t lastObservedRead{std::numeric_limits<uint64_t>::max()};
    uint64_t lastPrefetchedRead{std::numeric_limits<uint64_t>::max()};
  };

  struct OutputIoEstimate {
    uint64_t bytes{0};
    uint32_t columns{0};
    uint64_t indexedBytes{0};
    uint32_t indexedColumns{0};
    uint64_t indexedPagesEstimate{0};
  };

  dwio::common::SelectiveColumnReader* findBestLeaf();

  void enqueueRowGroup(uint32_t index, dwio::common::BufferedInput& input);

  void enqueueRowGroup(
      uint32_t index,
      dwio::common::BufferedInput& initialInput,
      dwio::common::BufferedInput* FOLLY_NULLABLE outputInput);

  bool hasLateMaterializationRoles() const;

  bool hasOutputOffsetIndex(uint32_t index) const;

  bool prefetchOutputPages(const RowSet& rows);

  OutputIoEstimate estimateOutputIo(uint32_t index) const;

  bool shouldUseTwoPhase(
      uint32_t index,
      const dwio::common::BufferedInput& input) const;

  void observeSelection(
      RowGroupReadContext& context,
      uint64_t selectedRows,
      uint64_t selectedSpanRows);

  bool isRowGroupBuffered(uint32_t index, dwio::common::BufferedInput& input);

  // Leaf column reader used for getting nullability information for
  // 'this'. This is nullptr for the root of a table.
  dwio::common::SelectiveColumnReader* FOLLY_NULLABLE childForRepDefs_{nullptr};

  // Mode for getting nulls from repdefs. kStructOverLists if 'this'
  // only has list children.
  LevelMode levelMode_;

  // The level information for extracting nulls for 'this' from the
  // repdefs in a leaf PageReader.
  arrow::LevelInfo levelInfo_;

  ParquetLateMaterializationPolicy lateMaterializationPolicy_;
  std::unordered_map<uint32_t, RowGroupReadContext> rowGroupReadContexts_;
  int64_t currentRowGroup_{-1};
};

} // namespace bytedance::bolt::parquet
