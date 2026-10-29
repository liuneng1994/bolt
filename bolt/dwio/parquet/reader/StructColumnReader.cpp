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

#include "bolt/dwio/parquet/reader/StructColumnReader.h"

#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/parquet/reader/ParquetColumnReader.h"
#include "bolt/dwio/parquet/reader/RepeatedColumnReader.h"
namespace bytedance::bolt::common {
class ScanSpec;
}
namespace bytedance::bolt::parquet {

StructColumnReader::StructColumnReader(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec,
    memory::MemoryPool& pool)
    : SelectiveStructColumnReader(
          columnReaderOptions,
          requestedType,
          fileType,
          params,
          scanSpec) {
  if (fileType->isDCMap() || fileType->type()->isVariant()) {
    // scanSpec for DCMap is a map, but we read it as a Struct.
    // scanSpec for Variant is a primitive, but we read it as a Struct.
    // manually add Struct type to scanSpec.
    // remove children from previous fileType or original Map type
    // which may not be applicable to current fileType
    scanSpec_->removeAllChildFields();
    scanSpec_->addAllChildFields(*(fileType->type()));
  }
  const bool useColumnNames =
      columnReaderOptions.useColumnNamesForColumnMapping_;
  const auto& childSpecs = scanSpec_->stableChildren();
  std::vector<column_index_t> missingFields;
  for (int i = childSpecs.size() - 1; i >= 0; --i) {
    auto childSpec = childSpecs[i];
    if (childSpec->isConstant() &&
        (!useColumnNames && isChildMissing(*childSpec))) {
      childSpec->setSubscript(kConstantChildSpecSubscript);
      continue;
    }
    if (!childSpec->readFromFile()) {
      continue;
    }
    if (useColumnNames && isChildMissing(*childSpec)) {
      if (!fileType_->isDCMap()) {
        missingFields.emplace_back(i);
      } else {
        scanSpec_->removeChild(childSpec);
      }
      continue;
    }
    const auto& fieldName = childSpec->fieldName();
    if (!fileType_->containsChild(fieldName)) {
      missingFields.emplace_back(i);
      continue;
    }
    auto childFileType = fileType_->childByName(fieldName);
    auto childRequestedType = childFileType->isDCMap()
        ? fileType_->childByName(fieldName)
        : requestedType_->childByName(fieldName);
    addChild(ParquetColumnReader::build(
        columnReaderOptions,
        childRequestedType,
        childFileType,
        params,
        *childSpec,
        pool));
    childSpecs[i]->setSubscript(children_.size() - 1);
  }

  if (missingFields.size() > 0) {
    // Set the struct as null if all the children fields in the output type are
    // missing and the number of child fields is more than one.
    if (childSpecs.size() > 1 && missingFields.size() == childSpecs.size()) {
      scanSpec_->setConstantValue(
          BaseVector::createNullConstant(requestedType_->type(), 1, &pool));
    } else {
      // Set null constant for the missing child field of output type.
      for (int channel : missingFields) {
        childSpecs[channel]->setConstantValue(BaseVector::createNullConstant(
            requestedType_->childByName(childSpecs[channel]->fieldName())
                ->type(),
            1,
            &pool));
      }
    }
  }

  auto type = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get());
  if (type->parent()) {
    levelMode_ = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get())
                     ->makeLevelInfo(levelInfo_);
    childForRepDefs_ = findBestLeaf();
    // Set mode to struct over lists if the child for repdefs has a list between
    // this and the child.
    auto child = childForRepDefs_;
    for (;;) {
      if (child == nullptr) {
        levelMode_ = LevelMode::kNulls;
        break;
      }
      if (child->fileType().type()->kind() == TypeKind::ARRAY ||
          child->fileType().type()->kind() == TypeKind::MAP) {
        levelMode_ = LevelMode::kStructOverLists;
        break;
      }
      if (child->fileType().type()->kind() == TypeKind::ROW) {
        child = reinterpret_cast<StructColumnReader*>(child)->childForRepDefs();
        continue;
      }
      levelMode_ = LevelMode::kNulls;
      break;
    }
  }
}

dwio::common::SelectiveColumnReader* FOLLY_NONNULL
StructColumnReader::findBestLeaf() {
  SelectiveColumnReader* best = nullptr;
  for (auto i = 0; i < children_.size(); ++i) {
    auto child = children_[i];
    auto kind = child->fileType().type()->kind();
    // Complex type child repdefs must be read in any case.
    if (kind == TypeKind::ROW || kind == TypeKind::ARRAY) {
      return child;
    }
    if (!best) {
      best = child;
    } else if (best->scanSpec()->filter() && !child->scanSpec()->filter()) {
      continue;
    } else if (!best->scanSpec()->filter() && child->scanSpec()->filter()) {
      best = child;
      continue;
    } else if (kind < best->fileType().type()->kind()) {
      best = child;
    }
  }
  assert(best);
  return best;
}

namespace {
void releaseRowGroupReaderRecursive(
    dwio::common::SelectiveColumnReader* reader) {
  reader->scanState().clear();
  reader->formatData().as<ParquetData>().releaseRowGroupReader();
  for (auto* child : reader->children()) {
    if (child != nullptr) {
      releaseRowGroupReaderRecursive(child);
    }
  }
}
} // namespace

void StructColumnReader::releaseRowGroupReader() {
  releaseRowGroupReaderRecursive(this);
  if (currentRowGroup_ >= 0) {
    auto it = rowGroupReadContexts_.find(currentRowGroup_);
    if (it != rowGroupReadContexts_.end()) {
      it->second.state.finish();
      rowGroupReadContexts_.erase(it);
    }
    currentRowGroup_ = -1;
  }
}

void StructColumnReader::read(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* /*incomingNulls*/) {
  if (auto it = rowGroupReadContexts_.find(currentRowGroup_);
      it != rowGroupReadContexts_.end()) {
    it->second.state.beginBatch();
  }
  ensureRepDefs(*this, offset + rows.back() + 1 - readOffset_);
  SelectiveStructColumnReader::read(offset, rows, nullptr);
  if (outputRows().empty()) {
    if (auto it = rowGroupReadContexts_.find(currentRowGroup_);
        it != rowGroupReadContexts_.end()) {
      observeSelection(it->second, 0, 0);
    }
  }
}

std::shared_ptr<dwio::common::BufferedInput> StructColumnReader::loadRowGroup(
    uint32_t index,
    const std::shared_ptr<dwio::common::BufferedInput>& input) {
  if (isRowGroupBuffered(index, *input)) {
    enqueueRowGroup(index, *input);
    auto [it, inserted] = rowGroupReadContexts_.try_emplace(
        index,
        ParquetLateMaterializationState::Strategy::kEagerCoalesced,
        nullptr);
    BOLT_CHECK(inserted);
    it->second.state.initialLoaded();
    return input;
  }

  auto strategy = hasLateMaterializationRoles()
      ? lateMaterializationPolicy_.strategy()
      : ParquetLateMaterializationState::Strategy::kEagerCoalesced;
  if (strategy == ParquetLateMaterializationState::Strategy::kTwoPhase &&
      !shouldUseTwoPhase(index, *input)) {
    strategy = ParquetLateMaterializationState::Strategy::kEagerCoalesced;
  }
  auto initialInput =
      std::shared_ptr<dwio::common::BufferedInput>(std::move(input->clone()));
  std::shared_ptr<dwio::common::BufferedInput> outputInput;
  if (strategy == ParquetLateMaterializationState::Strategy::kTwoPhase) {
    outputInput =
        std::shared_ptr<dwio::common::BufferedInput>(std::move(input->clone()));
    enqueueRowGroup(index, *initialInput, outputInput.get());
  } else {
    enqueueRowGroup(index, *initialInput);
  }
  initialInput->load(dwio::common::LogType::STRIPE);

  auto [it, inserted] =
      rowGroupReadContexts_.try_emplace(index, strategy, outputInput);
  BOLT_CHECK(inserted);
  it->second.state.initialLoaded();
  return initialInput;
}

void StructColumnReader::prepareOutputRead(const RowSet& rows) {
  auto it = rowGroupReadContexts_.find(currentRowGroup_);
  if (it == rowGroupReadContexts_.end()) {
    return;
  }
  auto& context = it->second;
  const auto selectedSpanRows =
      rows.empty() ? 0 : static_cast<uint64_t>(rows.back() - rows.front() + 1);
  observeSelection(context, rows.size(), selectedSpanRows);
  const bool shouldPrefetch = context.lastPrefetchedRead != numReads();
  if (shouldPrefetch) {
    context.lastPrefetchedRead = numReads();
  }
  if (!context.state.beginOutput()) {
    if (context.state.strategy() ==
            ParquetLateMaterializationState::Strategy::kTwoPhase &&
        shouldPrefetch) {
      prefetchOutputPages(rows);
    }
    return;
  }

  BOLT_CHECK_NOT_NULL(context.outputInput);
  const auto inputRows = rows_.size();
  if (inputRows > 0 &&
      rows.size() * 100 <
          inputRows * ParquetLateMaterializationPolicy::kEnterTwoPhasePct) {
    context.outputInput->setLoadQuantum(kSparseOutputLoadQuantum);
  }
  const bool indexedOutput = hasOutputOffsetIndex(currentRowGroup_);
  if (!indexedOutput && context.outputInput->estimatedReadLatencyUs() > 0 &&
      context.outputInput->executor()) {
    context.outputInput->loadDeferredOutput(dwio::common::LogType::STRIPE);
  } else {
    context.outputInput->load(dwio::common::LogType::STRIPE);
  }
  if (shouldPrefetch) {
    prefetchOutputPages(rows);
  }
  context.state.outputLoaded();
}

void StructColumnReader::observeSelection(
    RowGroupReadContext& context,
    uint64_t selectedRows,
    uint64_t selectedSpanRows) {
  if (context.lastObservedRead == numReads()) {
    return;
  }
  context.lastObservedRead = numReads();
  lateMaterializationPolicy_.observeSelection(
      selectedRows,
      rows_.size(),
      selectedSpanRows,
      hasOutputOffsetIndex(currentRowGroup_));
}

bool StructColumnReader::isRowGroupBuffered(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto [offset, length] =
      formatData().as<ParquetData>().getRowGroupRegion(index);
  return input.isBuffered(offset, length);
}

void StructColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueRowGroup(index, input, nullptr);
}

void StructColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& initialInput,
    dwio::common::BufferedInput* outputInput) {
  for (auto& child : children_) {
    if (auto structChild = dynamic_cast<StructColumnReader*>(child)) {
      structChild->enqueueRowGroup(index, initialInput, outputInput);
    } else if (auto listChild = dynamic_cast<ListColumnReader*>(child)) {
      auto& input = outputInput &&
              child->scanSpec()->ioReadClass() ==
                  dwio::common::IoReadClass::kOutput
          ? *outputInput
          : initialInput;
      listChild->enqueueRowGroup(
          index, input, outputInput ? &initialInput : nullptr);
    } else if (auto mapChild = dynamic_cast<MapColumnReader*>(child)) {
      auto& input = outputInput &&
              child->scanSpec()->ioReadClass() ==
                  dwio::common::IoReadClass::kOutput
          ? *outputInput
          : initialInput;
      mapChild->enqueueRowGroup(
          index, input, outputInput ? &initialInput : nullptr);
    } else {
      const auto readClass = child->scanSpec()->ioReadClass();
      auto& input =
          outputInput && readClass == dwio::common::IoReadClass::kOutput
          ? *outputInput
          : initialInput;
      child->formatData().as<ParquetData>().enqueueRowGroup(
          index,
          input,
          outputInput ? readClass : dwio::common::IoReadClass::kLegacy,
          outputInput && readClass == dwio::common::IoReadClass::kOutput,
          outputInput ? &initialInput : nullptr);
    }
  }
}

bool StructColumnReader::hasLateMaterializationRoles() const {
  bool hasFilter = false;
  bool hasOutput = false;
  for (const auto* child : children_) {
    hasFilter |=
        child->scanSpec()->ioReadClass() == dwio::common::IoReadClass::kFilter;
    hasOutput |=
        child->scanSpec()->ioReadClass() == dwio::common::IoReadClass::kOutput;
  }
  return hasFilter && hasOutput;
}

bool StructColumnReader::hasOutputOffsetIndex(uint32_t index) const {
  bool found = false;
  auto visit = [&](const dwio::common::SelectiveColumnReader* reader,
                   bool output,
                   auto& self) -> void {
    output |=
        reader->scanSpec()->ioReadClass() == dwio::common::IoReadClass::kOutput;
    if (reader->children().empty()) {
      found |= output &&
          reader->formatData().as<ParquetData>().hasOffsetIndex(index);
      return;
    }
    for (const auto* child : reader->children()) {
      if (child) {
        self(child, output, self);
      }
    }
  };
  for (const auto* child : children_) {
    visit(child, false, visit);
  }
  return found;
}

bool StructColumnReader::prefetchOutputPages(const RowSet& rows) {
  raw_vector<vector_size_t> topLevelRows;
  topLevelRows.resize(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    topLevelRows[i] = lazyVectorReadOffset_ + rows[i];
  }
  const RowSet selectedRows(topLevelRows);
  bool prefetched = false;
  auto visit = [&](dwio::common::SelectiveColumnReader* reader,
                   bool output,
                   auto& self) -> void {
    output |=
        reader->scanSpec()->ioReadClass() == dwio::common::IoReadClass::kOutput;
    if (reader->children().empty()) {
      if (output) {
        prefetched |=
            reader->formatData().as<ParquetData>().prefetchSelectedPages(
                selectedRows);
      }
      return;
    }
    for (auto* child : reader->children()) {
      if (child) {
        self(child, output, self);
      }
    }
  };
  for (auto* child : children_) {
    visit(child, false, visit);
  }
  return prefetched;
}

StructColumnReader::OutputIoEstimate StructColumnReader::estimateOutputIo(
    uint32_t index) const {
  OutputIoEstimate estimate;
  auto visit = [&](const dwio::common::SelectiveColumnReader* reader,
                   bool output,
                   auto& self) -> void {
    output |=
        reader->scanSpec()->ioReadClass() == dwio::common::IoReadClass::kOutput;
    if (reader->children().empty()) {
      if (output) {
        const auto& parquetData = reader->formatData().as<ParquetData>();
        const auto bytes = parquetData.compressedSize(index);
        estimate.bytes += bytes;
        ++estimate.columns;
        if (parquetData.hasOffsetIndex(index)) {
          estimate.indexedBytes += bytes;
          ++estimate.indexedColumns;
          // A compact-protocol PageLocation is about 20-30 bytes for common
          // file offsets and page sizes. Use 32 bytes as a coarse page-count
          // estimate without reading the OffsetIndex during planning.
          constexpr uint32_t kEstimatedPageLocationBytes = 32;
          estimate.indexedPagesEstimate += std::max<uint32_t>(
              1,
              (parquetData.offsetIndexLength(index) +
               kEstimatedPageLocationBytes - 1) /
                  kEstimatedPageLocationBytes);
        }
      }
      return;
    }
    for (const auto* child : reader->children()) {
      if (child) {
        self(child, output, self);
      }
    }
  };
  for (const auto* child : children_) {
    visit(child, false, visit);
  }
  return estimate;
}

bool StructColumnReader::shouldUseTwoPhase(
    uint32_t index,
    const dwio::common::BufferedInput& input) const {
  if (!lateMaterializationPolicy_.hasObservation()) {
    return false;
  }
  const auto estimate = estimateOutputIo(index);
  if (estimate.bytes == 0 || estimate.columns == 0) {
    return false;
  }

  const auto selectedPct = lateMaterializationPolicy_.selectedPct();
  const auto selectedSpanPct = lateMaterializationPolicy_.selectedSpanPct();
  const auto estimatePlannedBytes =
      [](uint64_t bytes, uint32_t columns, uint32_t coveragePct) {
        if (bytes == 0 || columns == 0) {
          return uint64_t{0};
        }
        const auto averageColumnBytes = bytes / columns;
        const auto selectedColumnBytes = std::max<uint64_t>(
            kSparseOutputLoadQuantum,
            (averageColumnBytes * coveragePct + 99) / 100);
        return std::min<uint64_t>(bytes, selectedColumnBytes * columns);
      };
  const auto unindexedBytes = estimate.bytes - estimate.indexedBytes;
  const auto unindexedColumns = estimate.columns - estimate.indexedColumns;
  const auto sequentialCoveragePct = std::max(selectedPct, selectedSpanPct);
  const auto plannedIndexedBytes = estimatePlannedBytes(
      estimate.indexedBytes, estimate.indexedColumns, selectedPct);
  const auto plannedUnindexedBytes = estimatePlannedBytes(
      unindexedBytes, unindexedColumns, sequentialCoveragePct);
  const auto plannedOutputBytes = plannedIndexedBytes + plannedUnindexedBytes;
  if (plannedOutputBytes >= estimate.bytes) {
    return false;
  }

  const auto requestLatencyMicros = input.estimatedReadLatencyUs();
  if (requestLatencyMicros == 0) {
    return true;
  }
  if (!input.executor()) {
    return false;
  }
  const auto* ioStats = input.ioStatistics();
  if (!ioStats || ioStats->rawBytesRead() == 0 ||
      ioStats->totalScanTime() == 0) {
    return false;
  }

  const auto indexedRequests = estimate.indexedColumns == 0
      ? uint64_t{0}
      : std::max<uint64_t>(
            estimate.indexedColumns,
            (estimate.indexedPagesEstimate * selectedPct + 99) / 100);
  const auto unindexedRequests = plannedUnindexedBytes == 0
      ? uint64_t{0}
      : (plannedUnindexedBytes + kSparseOutputLoadQuantum - 1) /
          kSparseOutputLoadQuantum;
  const auto sparseRequests = indexedRequests + unindexedRequests;
  const auto eagerQuantum = std::max<int32_t>(1, input.loadQuantum());
  const auto eagerRequests =
      std::max<uint64_t>(1, (estimate.bytes + eagerQuantum - 1) / eagerQuantum);
  // Splitting initial and output reads costs at least one extra request even
  // when both layouts use the same number of output ranges.
  const auto extraRequests = std::max<uint64_t>(
      1,
      sparseRequests > eagerRequests ? sparseRequests - eagerRequests + 1 : 1);

  const auto ioMicros = std::max<uint64_t>(1, ioStats->totalScanTime() / 1'000);
  const auto bytesPerMicro =
      static_cast<double>(ioStats->rawBytesRead()) / ioMicros;
  const auto savedBytes = estimate.bytes - plannedOutputBytes;
  const auto savedTransferMicros = savedBytes / bytesPerMicro;
  // Indexed page ranges are all dispatched to the I/O executor. Unindexed
  // columns issue the first quantum together and pull later quanta on demand.
  constexpr uint64_t kIoExecutorParallelism = 32;
  const auto additionalIndexedWaves = indexedRequests == 0
      ? uint64_t{0}
      : (indexedRequests - 1) / kIoExecutorParallelism;
  const auto serialUnindexedWaves = unindexedRequests > unindexedColumns
      ? unindexedRequests - unindexedColumns
      : 0;
  const auto additionalRequestWaves =
      additionalIndexedWaves + serialUnindexedWaves;
  // A staged HDFS row group has fixed synchronization costs before transfer:
  // finish the filter read, fetch OffsetIndex metadata when present, dispatch
  // output ranges, and join the first output wave. Independent output columns
  // overlap these costs on the I/O executor. Model this as four waves for a
  // narrow scan, decreasing linearly to two waves at 16 output columns.
  constexpr double kNarrowFixedRequestWaves = 4.0;
  constexpr double kWideFixedRequestWaves = 2.0;
  constexpr uint32_t kFullIoParallelismColumns = 16;
  const auto parallelColumns =
      std::min(estimate.columns, kFullIoParallelismColumns);
  const auto fixedRequestWaves = kNarrowFixedRequestWaves -
      (kNarrowFixedRequestWaves - kWideFixedRequestWaves) * parallelColumns /
          kFullIoParallelismColumns;
  const auto requestWaves = fixedRequestWaves + additionalRequestWaves;
  const auto extraRequestMicros = requestWaves * requestLatencyMicros;
  constexpr double kMinimumBenefitRatio = 1.3;
  const bool useTwoPhase =
      savedTransferMicros > extraRequestMicros * kMinimumBenefitRatio;
  VLOG(1) << "Parquet late materialization cost: rowGroup=" << index
          << ", selectedPct=" << selectedPct
          << ", selectedSpanPct=" << selectedSpanPct
          << ", outputBytes=" << estimate.bytes
          << ", indexedBytes=" << estimate.indexedBytes
          << ", indexedColumns=" << estimate.indexedColumns
          << ", indexedPagesEstimate=" << estimate.indexedPagesEstimate
          << ", plannedOutputBytes=" << plannedOutputBytes
          << ", extraRequests=" << extraRequests
          << ", indexedRequests=" << indexedRequests
          << ", unindexedRequests=" << unindexedRequests
          << ", requestWaves=" << requestWaves
          << ", requestLatencyMicros=" << requestLatencyMicros
          << ", savedTransferMicros=" << savedTransferMicros
          << ", strategy=" << (useTwoPhase ? "two-phase" : "eager");
  return useTwoPhase;
}

void StructColumnReader::seekToRowGroup(int64_t index) {
  currentRowGroup_ = index;
  SelectiveStructColumnReader::seekToRowGroup(index);
  BufferPtr noBuffer;
  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  readOffset_ = 0;
  rowGroupOffset_ = formatData_->as<ParquetData>().rowGroupOffset(index);
  for (auto& child : children_) {
    child->seekToRowGroup(index);
  }
}

void StructColumnReader::seekToEndOfPresetNulls() {
  auto numUnread = formatData_->as<ParquetData>().presetNullsLeft();
  for (auto i = 0; i < children_.size(); ++i) {
    auto child = children_[i];
    if (!child) {
      continue;
    }

    if (child->fileType().type()->kind() != TypeKind::ROW) {
      child->seekTo(readOffset_ + numUnread, false);
    } else if (child->fileType().type()->kind() == TypeKind::ROW) {
      reinterpret_cast<StructColumnReader*>(child)->seekToEndOfPresetNulls();
    }
  }
  readOffset_ += numUnread;
  formatData_->as<ParquetData>().skipNulls(numUnread, false);
}

void StructColumnReader::setNullsFromRepDefs(PageReader& pageReader) {
  if (levelInfo_.def_level == 0) {
    return;
  }
  auto repDefRange = pageReader.repDefRange();
  int32_t numRepDefs = pageReader.repDefOutputSize(levelMode_, levelInfo_);
  auto bits = prepareRepDefNulls(numRepDefs);
  bits.values_read = pageReader.getLengthsAndNulls(
      levelMode_,
      levelInfo_,
      repDefRange.first,
      repDefRange.second,
      numRepDefs,
      nullptr,
      reinterpret_cast<uint64_t*>(bits.valid_bits),
      0);
  setNullsFromRepDefOutput(bits);
}

arrow::ValidityBitmapInputOutput StructColumnReader::prepareRepDefNulls(
    int32_t maxItems) {
  dwio::common::ensureCapacity<uint64_t>(
      nullsInReadRange_, bits::nwords(maxItems), &memoryPool_);
  arrow::ValidityBitmapInputOutput output;
  output.values_read_upper_bound = maxItems;
  output.values_read = 0;
  output.null_count = 0;
  output.valid_bits =
      reinterpret_cast<uint8_t*>(nullsInReadRange_->asMutable<uint64_t>());
  output.valid_bits_offset = 0;
  return output;
}

void StructColumnReader::setNullsFromRepDefOutput(
    const arrow::ValidityBitmapInputOutput& bits) {
  formatData_->as<ParquetData>().setNulls(
      nullsInReadRange(), static_cast<int32_t>(bits.values_read));
}

void StructColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result,
    dwio::common::BufferedInput& input) const {
  for (const auto& child : children_) {
    child->filterRowGroups(rowGroupSize, context, result, input);
  }
}

} // namespace bytedance::bolt::parquet
