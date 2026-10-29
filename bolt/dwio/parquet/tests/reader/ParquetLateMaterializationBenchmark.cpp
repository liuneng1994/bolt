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

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include <folly/Benchmark.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/hash/Hash.h>
#include <folly/init/Init.h>
#include <folly/json.h>
#include <gflags/gflags.h>

#include "bolt/common/base/GlobalParameters.h"
#include "bolt/common/caching/ScanTracker.h"
#include "bolt/common/file/File.h"
#include "bolt/common/io/IoStatistics.h"
#include "bolt/dwio/common/DirectBufferedInput.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/Statistics.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/expression/ExprToSubfieldFilter.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

DEFINE_int32(
    bolt_benchmark_lm_rounds,
    7,
    "Number of alternating measured rounds per benchmark case");
DEFINE_int32(
    bolt_benchmark_lm_row_groups,
    4,
    "Number of row groups in the generated Parquet file");
DEFINE_int32(
    bolt_benchmark_lm_rows_per_group,
    32768,
    "Number of rows in each generated row group");
DEFINE_int32(
    bolt_benchmark_lm_payload_columns,
    20,
    "Number of payload columns in the generated Parquet file");
DEFINE_int32(
    bolt_benchmark_lm_payload_bytes,
    32,
    "Bytes in each generated payload value");
DEFINE_int32(
    bolt_benchmark_lm_remote_delay_us,
    0,
    "Delay injected once per physical file read");
DEFINE_bool(
    bolt_benchmark_lm_write_offset_index,
    false,
    "Write an OffsetIndex for every generated Parquet column");
DEFINE_string(
    bolt_benchmark_lm_selection_pattern,
    "prefix",
    "Selection layout: prefix or page_clusters");
DEFINE_string(
    bolt_benchmark_lm_output,
    "/tmp/bolt_parquet_late_materialization_benchmark.json",
    "Output path for raw benchmark JSON");

namespace bytedance::bolt::parquet {
namespace {

using Clock = std::chrono::steady_clock;
using common::ScanSpec;
using dwio::common::ColumnSelector;
using dwio::common::DirectBufferedInput;
using dwio::common::IoReadClass;
using dwio::common::MetricsLog;
using dwio::common::RowReaderOptions;
using dwio::common::WriteFileSink;

constexpr int32_t kLoadQuantum = 8 << 20;
constexpr int32_t kBatchSize = 65536;

class DelayedReadFile final : public ReadFile {
 public:
  DelayedReadFile(const std::string& path, int32_t delayUs)
      : delegate_(std::make_shared<LocalReadFile>(path)), delayUs_(delayUs) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* FOLLY_NONNULL buffer) const override {
    delay();
    auto result = delegate_->pread(offset, length, buffer);
    bytesRead_ += result.size();
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    delay();
    auto result = delegate_->preadv(offset, buffers);
    bytesRead_ += result;
    return result;
  }

  bool shouldCoalesce() const override {
    return true;
  }

  uint64_t size() const override {
    return delegate_->size();
  }

  uint64_t memoryUsage() const override {
    return delegate_->memoryUsage();
  }

  std::string getName() const override {
    return delegate_->getName();
  }

  uint64_t getNaturalReadSize() const override {
    return delegate_->getNaturalReadSize();
  }

 private:
  void delay() const {
    if (delayUs_ > 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(delayUs_));
    }
  }

  const std::shared_ptr<LocalReadFile> delegate_;
  const int32_t delayUs_;
};

struct RunResult {
  int64_t wallNanos;
  uint64_t rawBytesRead;
  uint64_t fileBytesRead;
  uint64_t prefetchBytes;
  uint64_t demandBytes;
  uint64_t prefetchCount;
  uint64_t demandCount;
  uint64_t outputRows;
  uint64_t totalPages;
  uint64_t filteredOutPages;
  uint64_t offsetIndexBytesRead;
  uint64_t offsetIndexPagesSkipped;
  uint64_t offsetIndexFallbacks;
};

class LateMaterializationBenchmark {
 public:
  LateMaterializationBenchmark()
      : rootPool_(memory::memoryManager()->addRootPool(
            "ParquetLateMaterializationBenchmark")),
        leafPool_(
            rootPool_->addLeafChild("ParquetLateMaterializationBenchmark")),
        vectorMaker_(leafPool_.get()),
        rowType_(makeRowType()),
        filePath_(directory_->path + "/late_materialization.parquet") {
    writeFile();
  }

  uint64_t fileSize() const {
    return std::filesystem::file_size(filePath_);
  }

  RunResult run(int32_t payloadWidth, int32_t selectivity, bool splitIo) {
    BOLT_CHECK_GT(payloadWidth, 0);
    BOLT_CHECK_LE(payloadWidth, FLAGS_bolt_benchmark_lm_payload_columns);
    BOLT_CHECK_GT(selectivity, 0);
    BOLT_CHECK_LE(selectivity, 100);

    std::vector<std::string> names{"filter_key"};
    std::vector<TypePtr> types{BIGINT()};
    for (int32_t i = 0; i < payloadWidth; ++i) {
      names.push_back(fmt::format("payload_{}", i));
      types.push_back(VARCHAR());
    }
    auto selectedType = ROW(std::move(names), std::move(types));
    dwio::common::RuntimeStatistics runtimeStats;
    auto scanSpec =
        makeScanSpec(selectedType, selectivity, splitIo, &runtimeStats);

    auto ioStats = std::make_shared<io::IoStatistics>();
    auto tracker = std::make_shared<cache::ScanTracker>(
        "late-materialization-benchmark", nullptr, kLoadQuantum);
    auto readFile = std::make_shared<DelayedReadFile>(
        filePath_, FLAGS_bolt_benchmark_lm_remote_delay_us);
    std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor;
    std::shared_ptr<connector::AsyncThreadCtx> asyncThreadCtx;
    if (FLAGS_bolt_benchmark_lm_remote_delay_us > 0) {
      ioExecutor = std::make_unique<folly::IOThreadPoolExecutor>(
          32,
          std::make_shared<folly::NamedThreadFactory>(kAsyncPreloadThreadName));
      asyncThreadCtx =
          std::make_shared<connector::AsyncThreadCtx>(1ULL << 30, true);
    }
    dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    readerOptions.setLoadQuantum(kLoadQuantum)
        .setMaxCoalesceBytes(128 << 20)
        .setMaxCoalesceDistance(512 << 10)
        .setPrefetchRowGroups(1)
        .setEstimatedReadLatencyUs(FLAGS_bolt_benchmark_lm_remote_delay_us);

    const auto start = Clock::now();
    auto input = std::make_unique<DirectBufferedInput>(
        readFile,
        MetricsLog::voidLog(),
        1,
        tracker,
        1,
        ioStats,
        ioExecutor.get(),
        readerOptions,
        asyncThreadCtx);
    auto reader =
        std::make_unique<ParquetReader>(std::move(input), readerOptions);

    RowReaderOptions rowReaderOptions;
    rowReaderOptions.select(
        std::make_shared<ColumnSelector>(selectedType, selectedType->names()));
    rowReaderOptions.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOptions);

    VectorPtr result = BaseVector::create(selectedType, 0, leafPool_.get());
    uint64_t outputRows = 0;
    while (rowReader->next(kBatchSize, result)) {
      auto* row = result->asUnchecked<RowVector>();
      outputRows += row->size();
      for (column_index_t i = 1; i < row->childrenSize(); ++i) {
        auto loaded = row->childAt(i)->loadedVector();
        folly::doNotOptimizeAway(loaded->retainedSize());
      }
    }
    rowReader->updateRuntimeStats(runtimeStats);
    const auto wallNanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               Clock::now() - start)
                               .count();

    return RunResult{
        .wallNanos = wallNanos,
        .rawBytesRead = ioStats->rawBytesRead(),
        .fileBytesRead = readFile->bytesRead(),
        .prefetchBytes = ioStats->prefetch().sum(),
        .demandBytes = ioStats->read().sum(),
        .prefetchCount = ioStats->prefetch().count(),
        .demandCount = ioStats->read().count(),
        .outputRows = outputRows,
        .totalPages = runtimeStats.totalPages,
        .filteredOutPages = runtimeStats.filteredOutPages,
        .offsetIndexBytesRead = runtimeStats.offsetIndexBytesRead,
        .offsetIndexPagesSkipped = runtimeStats.offsetIndexPagesSkipped,
        .offsetIndexFallbacks = runtimeStats.offsetIndexFallbacks,
    };
  }

 private:
  RowTypePtr makeRowType() const {
    std::vector<std::string> names{"filter_key"};
    std::vector<TypePtr> types{BIGINT()};
    for (int32_t i = 0; i < FLAGS_bolt_benchmark_lm_payload_columns; ++i) {
      names.push_back(fmt::format("payload_{}", i));
      types.push_back(VARCHAR());
    }
    return ROW(std::move(names), std::move(types));
  }

  std::shared_ptr<ScanSpec> makeScanSpec(
      const RowTypePtr& selectedType,
      int32_t selectivity,
      bool splitIo,
      dwio::common::RuntimeStatistics* runtimeStats) {
    auto scanSpec = std::make_shared<ScanSpec>("root");
    scanSpec->setRuntimeStatistics(runtimeStats);
    for (column_index_t i = 0; i < selectedType->size(); ++i) {
      scanSpec->addField(selectedType->nameOf(i), i);
    }
    auto* filterSpec = scanSpec->childByName("filter_key");
    filterSpec->setFilter(exec::lessThan(static_cast<int64_t>(selectivity)));
    scanSpec->resetCachedValues(true);

    if (splitIo) {
      filterSpec->setIoReadClassRecursively(IoReadClass::kFilter);
      for (column_index_t i = 1; i < selectedType->size(); ++i) {
        scanSpec->childByName(selectedType->nameOf(i))
            ->setIoReadClassRecursively(IoReadClass::kOutput);
      }
    }
    return scanSpec;
  }

  std::string payloadValue(int32_t rowGroup, vector_size_t row, int32_t column)
      const {
    uint64_t x = folly::hash::twang_mix64(
        (static_cast<uint64_t>(rowGroup) << 48) ^
        (static_cast<uint64_t>(column) << 32) ^ static_cast<uint64_t>(row));
    std::string value;
    while (value.size() < FLAGS_bolt_benchmark_lm_payload_bytes) {
      value += fmt::format("{:016x}", x);
      x = folly::hash::twang_mix64(x + 0x9e3779b97f4a7c15ULL);
    }
    value.resize(FLAGS_bolt_benchmark_lm_payload_bytes);
    return value;
  }

  RowVectorPtr makeRowGroup(int32_t rowGroup) {
    std::vector<VectorPtr> children;
    children.push_back(vectorMaker_.flatVector<int64_t>(
        FLAGS_bolt_benchmark_lm_rows_per_group, [](vector_size_t row) {
          if (FLAGS_bolt_benchmark_lm_selection_pattern == "page_clusters") {
            constexpr int32_t kRowsPerPage = 2048;
            return static_cast<int64_t>((row / kRowsPerPage * 37) % 100);
          }
          return static_cast<int64_t>(
              row * 100 / FLAGS_bolt_benchmark_lm_rows_per_group);
        }));
    for (int32_t column = 0; column < FLAGS_bolt_benchmark_lm_payload_columns;
         ++column) {
      children.push_back(vectorMaker_.flatVector<std::string>(
          FLAGS_bolt_benchmark_lm_rows_per_group,
          [this, rowGroup, column](vector_size_t row) {
            return payloadValue(rowGroup, row, column);
          }));
    }
    return vectorMaker_.rowVector(std::move(children));
  }

  void writeFile() {
    auto writeFile = std::make_unique<LocalWriteFile>(filePath_, true, false);
    auto sink =
        std::make_unique<WriteFileSink>(std::move(writeFile), filePath_);
    WriterOptions options;
    options.enableDictionary = false;
    options.compression = common::CompressionKind_SNAPPY;
    options.dataPageSize = 64 << 10;
    options.maxRowsPerDataPage = 2048;
    options.writePageIndex = FLAGS_bolt_benchmark_lm_write_offset_index;
    options.memoryPool = rootPool_.get();
    Writer writer(std::move(sink), options, rowType_);
    for (int32_t rowGroup = 0; rowGroup < FLAGS_bolt_benchmark_lm_row_groups;
         ++rowGroup) {
      writer.write(makeRowGroup(rowGroup));
      writer.flush();
    }
    writer.close();
  }

  const std::shared_ptr<memory::MemoryPool> rootPool_;
  const std::shared_ptr<memory::MemoryPool> leafPool_;
  test::VectorMaker vectorMaker_;
  const RowTypePtr rowType_;
  const std::shared_ptr<exec::test::TempDirectoryPath> directory_ =
      exec::test::TempDirectoryPath::create();
  const std::string filePath_;
};

folly::dynamic resultToDynamic(
    const RunResult& result,
    int32_t round,
    int32_t order,
    bool splitIo) {
  return folly::dynamic::object("round", round)("order", order)(
      "mode", splitIo ? "split" : "legacy")("wall_ns", result.wallNanos)(
      "raw_bytes_read", result.rawBytesRead)(
      "file_bytes_read", result.fileBytesRead)(
      "prefetch_bytes", result.prefetchBytes)(
      "demand_bytes", result.demandBytes)(
      "prefetch_count", result.prefetchCount)(
      "demand_count", result.demandCount)("output_rows", result.outputRows)(
      "total_pages", result.totalPages)(
      "filtered_out_pages", result.filteredOutPages)(
      "offset_index_bytes_read", result.offsetIndexBytesRead)(
      "offset_index_pages_skipped", result.offsetIndexPagesSkipped)(
      "offset_index_fallbacks", result.offsetIndexFallbacks);
}

} // namespace
} // namespace bytedance::bolt::parquet

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::memory::MemoryManager::initialize({});
  bytedance::bolt::parquet::LateMaterializationBenchmark benchmark;

  folly::dynamic report = folly::dynamic::object("schema_version", 1)(
      "rounds", FLAGS_bolt_benchmark_lm_rounds)(
      "row_groups", FLAGS_bolt_benchmark_lm_row_groups)(
      "rows_per_group", FLAGS_bolt_benchmark_lm_rows_per_group)(
      "payload_columns", FLAGS_bolt_benchmark_lm_payload_columns)(
      "payload_bytes", FLAGS_bolt_benchmark_lm_payload_bytes)(
      "remote_delay_us", FLAGS_bolt_benchmark_lm_remote_delay_us)(
      "write_offset_index", FLAGS_bolt_benchmark_lm_write_offset_index)(
      "selection_pattern", FLAGS_bolt_benchmark_lm_selection_pattern)(
      "file_bytes", benchmark.fileSize())("cases", folly::dynamic::array);

  const std::vector<int32_t> widths{1, 8, 20};
  const std::vector<int32_t> selectivities{1, 10, 50, 100};
  for (auto width : widths) {
    if (width > FLAGS_bolt_benchmark_lm_payload_columns) {
      continue;
    }
    for (auto selectivity : selectivities) {
      auto legacyWarmup = benchmark.run(width, selectivity, false);
      auto splitWarmup = benchmark.run(width, selectivity, true);
      BOLT_CHECK_EQ(legacyWarmup.outputRows, splitWarmup.outputRows);

      folly::dynamic runs = folly::dynamic::array;
      for (int32_t round = 0; round < FLAGS_bolt_benchmark_lm_rounds; ++round) {
        for (int32_t order = 0; order < 2; ++order) {
          const bool splitIo = (round % 2 == 0) ? order == 1 : order == 0;
          auto result = benchmark.run(width, selectivity, splitIo);
          BOLT_CHECK_EQ(result.outputRows, legacyWarmup.outputRows);
          runs.push_back(bytedance::bolt::parquet::resultToDynamic(
              result, round, order, splitIo));
        }
      }
      report["cases"].push_back(folly::dynamic::object("payload_width", width)(
          "selectivity_pct", selectivity)("runs", std::move(runs)));
    }
  }

  const auto json = folly::toPrettyJson(report);
  std::ofstream output(FLAGS_bolt_benchmark_lm_output);
  BOLT_CHECK(output.good(), "Cannot open benchmark output");
  output << json << '\n';
  output.close();
  std::cout << json << std::endl;
  return 0;
}
