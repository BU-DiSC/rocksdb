//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/fade.h"

#include "db/db_test_util.h"
#include "db/tombstone_dpt.h"
#include "db/write_batch_internal.h"
#include "env/composite_env_wrapper.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/mock_time_env.h"
#include "test_util/testharness.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

namespace ROCKSDB_NAMESPACE {

TEST(FadeLevelDeadlinesTest, ExponentialSplit) {
  // d_0 = 1110 * 9 / 999 = 10, d_1 = 100, d_2 = 1000; cumulative sums.
  std::vector<uint64_t> d = FadeLevelDeadlines(1110, 10.0, 3);
  ASSERT_EQ(d, (std::vector<uint64_t>{10, 110, 1110}));
}

TEST(FadeLevelDeadlinesTest, LastDeadlineIsThreshold) {
  for (int levels = 1; levels <= 7; ++levels) {
    std::vector<uint64_t> d = FadeLevelDeadlines(3600, 10.0, levels);
    ASSERT_EQ(d.size(), static_cast<size_t>(levels));
    ASSERT_EQ(d.back(), 3600u);
    for (size_t i = 1; i < d.size(); ++i) {
      ASSERT_LE(d[i - 1], d[i]);
    }
  }
}

TEST(FadeLevelDeadlinesTest, UniformWhenSizeRatioNotAboveOne) {
  ASSERT_EQ(FadeLevelDeadlines(300, 1.0, 3),
            (std::vector<uint64_t>{100, 200, 300}));
  ASSERT_EQ(FadeLevelDeadlines(300, 0.5, 3),
            (std::vector<uint64_t>{100, 200, 300}));
}

TEST(FadeLevelDeadlinesTest, DegenerateInputs) {
  ASSERT_TRUE(FadeLevelDeadlines(3600, 10.0, 0).empty());
  ASSERT_TRUE(FadeLevelDeadlines(0, 10.0, 3).empty());
  ASSERT_EQ(FadeLevelDeadlines(3600, 10.0, 1), (std::vector<uint64_t>{3600}));
}

TEST(FadeLevelDeadlinesTest, HugeValuesDoNotOverflow) {
  const uint64_t dth = std::numeric_limits<uint64_t>::max() / 2;
  std::vector<uint64_t> d = FadeLevelDeadlines(dth, 10.0, 7);
  ASSERT_EQ(d.back(), dth);
  ASSERT_LE(d.front(), d.back());
}

TEST(FadeExpiryTest, Boundary) {
  // Unknown or absent tombstone time never expires.
  ASSERT_FALSE(IsTombstoneExpired(0, 1000, 10));
  // Clock behind the tombstone time never expires.
  ASSERT_FALSE(IsTombstoneExpired(1000, 900, 10));
  ASSERT_FALSE(IsTombstoneExpired(1000, 1010, 10));
  ASSERT_TRUE(IsTombstoneExpired(1000, 1011, 10));
}

TEST(FadeLevelFractionsTest, MatchesDeadlines) {
  std::vector<long double> f = FadeLevelFractions(10.0, 3);
  ASSERT_EQ(f.size(), 3U);
  ASSERT_EQ(f.back(), 1.0L);
  ASSERT_EQ(FadeDeadline(1110, f[0]), 10U);
  ASSERT_EQ(FadeDeadline(1110, f[1]), 110U);
  ASSERT_EQ(FadeDeadline(1110, f[2]), 1110U);
}

TEST(TombstoneDptMapTest, AddExtendsRuns) {
  TombstoneDptMap m;
  m.Add(10, 5, 100);
  m.Add(11, 5, 100);
  m.Add(9, 5, 100);
  ASSERT_EQ(m.Size(), 1U);
  ASSERT_EQ(m.entries()[0], (TombstoneDpt{9, 11, 5, 100}));

  // A different DPT starts a new run; filling the gap does not merge it.
  m.Add(13, 7, 100);
  m.Add(12, 5, 100);
  ASSERT_EQ(m.Size(), 2U);
  ASSERT_EQ(m.entries()[0], (TombstoneDpt{9, 12, 5, 100}));
  ASSERT_EQ(m.entries()[1], (TombstoneDpt{13, 13, 7, 100}));

  // Out-of-order insertion stays sorted.
  m.Add(3, 1, 50);
  ASSERT_EQ(m.entries()[0], (TombstoneDpt{3, 3, 1, 50}));

  // A covered seqno makes the entry's deadline earlier.
  m.Add(10, 2, 90);
  ASSERT_EQ(m.entries()[1], (TombstoneDpt{9, 12, 2, 90}));
  // The combined deadline runs through the earlier start and the earlier end:
  // start min(90, 80) = 80, end min(92, 80 + 50) = 92.
  m.Add(11, 50, 80);
  ASSERT_EQ(m.entries()[1], (TombstoneDpt{9, 12, 12, 80}));

  // Joining two runs with the same DPT and time.
  TombstoneDptMap j;
  j.Add(1, 5, 100);
  j.Add(3, 5, 100);
  j.Add(2, 5, 100);
  ASSERT_EQ(j.Size(), 1U);
  ASSERT_EQ(j.entries()[0], (TombstoneDpt{1, 3, 5, 100}));
}

TEST(TombstoneDptMapTest, Find) {
  TombstoneDptMap m;
  ASSERT_EQ(m.Find(1), nullptr);
  m.Add(TombstoneDpt{10, 20, 5, 100});
  m.Add(TombstoneDpt{30, 30, 6, 100});
  ASSERT_EQ(m.Find(9), nullptr);
  ASSERT_EQ(m.Find(10)->dpt, 5U);
  ASSERT_EQ(m.Find(20)->dpt, 5U);
  ASSERT_EQ(m.Find(21), nullptr);
  ASSERT_EQ(m.Find(30)->dpt, 6U);
  ASSERT_EQ(m.Find(31), nullptr);
}

TEST(TombstoneDptMapTest, MergeCombinesOverlaps) {
  TombstoneDptMap a;
  a.Add(TombstoneDpt{10, 20, 5, 100});
  a.Add(TombstoneDpt{40, 50, 9, 300});
  TombstoneDptMap b;
  b.Add(TombstoneDpt{15, 25, 8, 50});
  b.Add(TombstoneDpt{30, 35, 1, 400});
  a.Merge(b);
  ASSERT_EQ(a.Size(), 3U);
  // Start min(100, 50) = 50, end min(105, 58) = 58.
  ASSERT_EQ(a.entries()[0], (TombstoneDpt{10, 25, 8, 50}));
  ASSERT_EQ(a.entries()[1], (TombstoneDpt{30, 35, 1, 400}));
  ASSERT_EQ(a.entries()[2], (TombstoneDpt{40, 50, 9, 300}));

  // An entry spanning several others swallows them.
  a.Add(TombstoneDpt{20, 45, 3, 200});
  ASSERT_EQ(a.Size(), 1U);
  ASSERT_EQ(a.entries()[0], (TombstoneDpt{10, 50, 8, 50}));
}

TEST(TombstoneDptMapTest, ShrinkIsConservative) {
  TombstoneDptMap m;
  for (uint64_t i = 0; i < 100; ++i) {
    m.Add(i * 10, 1000 - i, 5000 + i);
  }
  m.Shrink(8);
  ASSERT_LE(m.Size(), 8U);
  for (uint64_t i = 0; i < 100; ++i) {
    const TombstoneDpt* e = m.Find(i * 10);
    ASSERT_NE(e, nullptr);
    // The deadline is linear in the level fraction, so checking both ends
    // covers every level.
    ASSERT_LE(e->write_time, 5000 + i);
    ASSERT_LE(e->write_time + e->dpt, 5000 + i + 1000 - i);
  }
}

TEST(TombstoneDptMapTest, EncodeDecode) {
  TombstoneDptMap m;
  m.Add(TombstoneDpt{10, 20, 5, 100});
  m.Add(TombstoneDpt{kMaxSequenceNumber - 1, kMaxSequenceNumber, 6, 7});
  std::string encoded;
  m.EncodeTo(&encoded);
  TombstoneDptMap d;
  Slice in(encoded);
  ASSERT_OK(d.DecodeFrom(&in));
  ASSERT_TRUE(in.empty());
  ASSERT_EQ(d, m);

  Slice truncated(encoded.data(), encoded.size() - 1);
  ASSERT_TRUE(d.DecodeFrom(&truncated).IsCorruption());

  // Overlapping entries are rejected.
  std::string bad;
  PutVarint64(&bad, 2);
  for (int i = 0; i < 2; ++i) {
    PutVarint64(&bad, 10);
    PutVarint64(&bad, 5);
    PutVarint64(&bad, 1);
    PutVarint64(&bad, 1);
  }
  Slice bad_in(bad);
  ASSERT_TRUE(d.DecodeFrom(&bad_in).IsCorruption());
}

// Counts compactions and flushes by reason.
TEST(DeletePersistenceMarkerTest, RoundTrip) {
  const std::string blob =
      WriteBatch::DeletePersistenceThresholdLogData(3600, 1234567);
  uint64_t dpt = 0;
  uint64_t write_time = 0;
  ASSERT_TRUE(WriteBatchInternal::ParseDeletePersistenceThreshold(
      blob, &dpt, &write_time));
  ASSERT_EQ(dpt, 3600u);
  ASSERT_EQ(write_time, 1234567u);
}

TEST(DeletePersistenceMarkerTest, RejectsOtherBlobs) {
  uint64_t dpt = 0;
  uint64_t write_time = 0;
  ASSERT_FALSE(WriteBatchInternal::ParseDeletePersistenceThreshold(
      "", &dpt, &write_time));
  ASSERT_FALSE(WriteBatchInternal::ParseDeletePersistenceThreshold(
      "replication timestamp", &dpt, &write_time));
  std::string blob = WriteBatch::DeletePersistenceThresholdLogData(3600, 7);
  // Truncated.
  ASSERT_FALSE(WriteBatchInternal::ParseDeletePersistenceThreshold(
      Slice(blob.data(), blob.size() - 1), &dpt, &write_time));
  // Trailing bytes.
  blob.push_back('x');
  ASSERT_FALSE(WriteBatchInternal::ParseDeletePersistenceThreshold(
      blob, &dpt, &write_time));
}

class CompactionReasonListener : public EventListener {
 public:
  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    counts_[ci.compaction_reason]++;
  }

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& fi) override {
    std::lock_guard<std::mutex> lock(mutex_);
    flush_counts_[fi.flush_reason]++;
  }

  int Count(CompactionReason reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    return counts_[reason];
  }

  int Count(FlushReason reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    return flush_counts_[reason];
  }

 private:
  std::mutex mutex_;
  std::map<CompactionReason, int> counts_;
  std::map<FlushReason, int> flush_counts_;
};

class DBFadeTest : public DBTestBase {
 public:
  DBFadeTest() : DBTestBase("db_fade_test", /*env_do_fsync=*/false) {
    mock_clock_ = std::make_shared<MockSystemClock>(env_->GetSystemClock());
    mock_clock_->SetCurrentTime(kMockStartTime);
    mock_env_ = std::make_unique<CompositeEnvWrapper>(env_, mock_clock_);
  }

 protected:
  // Large enough that time arithmetic never underflows.
  static constexpr uint64_t kMockStartTime = 10000000;
  static constexpr uint64_t kDth = 1000;
  // The seqno-to-time mapping records an entry every
  // dth / kMaxSeqnoTimePairsPerCF seconds.
  static constexpr uint64_t kCadence = kDth / kMaxSeqnoTimePairsPerCF;

  std::unique_ptr<Env> mock_env_;
  std::shared_ptr<MockSystemClock> mock_clock_;

  void SetUp() override {
    mock_clock_->InstallTimedWaitFixCallback();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::StartPeriodicTaskScheduler:Init",
        [mock_clock = mock_clock_](void* arg) {
          auto periodic_task_scheduler_ptr =
              static_cast<PeriodicTaskScheduler*>(arg);
          periodic_task_scheduler_ptr->TEST_OverrideTimer(mock_clock.get());
        });
    SyncPoint::GetInstance()->EnableProcessing();
    mock_clock_->SetCurrentTime(kMockStartTime);
  }

  void TearDown() override {
    // The DB must not outlive mock_env_.
    Close();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  Options FadeOptions() {
    Options options = CurrentOptions();
    options.env = mock_env_.get();
    options.compaction_style = kCompactionStyleLevel;
    options.delete_persistence_threshold = kDth;
    options.disable_auto_compactions = true;
    options.level_compaction_dynamic_level_bytes = false;
    options.num_levels = 4;
    return options;
  }

  uint64_t Now() {
    int64_t t = 0;
    EXPECT_OK(mock_clock_->GetCurrentTime(&t));
    return static_cast<uint64_t>(t);
  }

  // Advance mock time, letting periodic tasks (seqno-to-time recording, FADE
  // trigger) run.
  void Sleep(uint64_t seconds) {
    for (uint64_t i = 0; i < seconds; i += kCadence) {
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kCadence));
      });
    }
  }

  // Thresholds recorded in the default column family's active memtable.
  static std::vector<TombstoneDpt> MemtableDpts(DB* db) {
    auto* cfh = static_cast<ColumnFamilyHandleImpl*>(db->DefaultColumnFamily());
    return cfh->cfd()->mem()->GetTombstoneDpts().entries();
  }

  std::vector<std::vector<FileMetaData>> Files() {
    std::vector<std::vector<FileMetaData>> files;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
    return files;
  }
};

TEST_F(DBFadeTest, FlushRecordsOldestTombstoneTime) {
  DestroyAndReopen(FadeOptions());
  Sleep(10 * kCadence);
  const uint64_t put_time = Now();
  ASSERT_OK(Put("a", "v"));
  Sleep(2 * kCadence);
  const uint64_t delete_time = Now();
  ASSERT_OK(Delete("b"));
  Sleep(2 * kCadence);
  ASSERT_OK(Delete("c"));
  // Flush before the L0 deadline (90s) so that FADE does not flush first.
  Sleep(2 * kCadence);
  ASSERT_OK(Flush());

  ASSERT_OK(Put("d", "v"));
  ASSERT_OK(Flush());

  auto files = Files();
  ASSERT_EQ(files[0].size(), 2u);
  // L0 files are ordered newest first.
  ASSERT_EQ(files[0][0].oldest_tombstone_time, 0u);
  // The mapping dates a write no later than it happened, and no earlier than
  // the first recording of the preceding seqno.
  const uint64_t t = files[0][1].oldest_tombstone_time;
  ASSERT_LE(t, delete_time);
  ASSERT_GE(t, put_time);
}

TEST_F(DBFadeTest, OldestTombstoneTimeSurvivesReopen) {
  Options options = FadeOptions();
  DestroyAndReopen(options);
  Sleep(10 * kCadence);
  ASSERT_OK(Delete("b"));
  Sleep(10 * kCadence);
  ASSERT_OK(Flush());
  const uint64_t t = Files()[0][0].oldest_tombstone_time;
  ASSERT_NE(t, 0u);

  Reopen(options);
  ASSERT_EQ(Files()[0][0].oldest_tombstone_time, t);
}

TEST_F(DBFadeTest, CompactionKeepsOldestTombstoneTime) {
  Options options = FadeOptions();
  DestroyAndReopen(options);
  // Bottom data so that tombstones are not dropped when compacting to L1.
  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Put("z", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);

  Sleep(10 * kCadence);
  const uint64_t first_delete = Now();
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Delete("d"));
  ASSERT_OK(Flush());
  const uint64_t flushed_time = Files()[0][0].oldest_tombstone_time;
  ASSERT_NE(flushed_time, 0u);
  Sleep(10 * kCadence);
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Flush());

  // The L0 files overlap, so this is a real compaction rather than a trivial
  // move. L3 overlaps too, so the tombstones survive it.
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_EQ("0,1,0,1", FilesPerLevel());
  const uint64_t t = Files()[1][0].oldest_tombstone_time;
  ASSERT_LE(t, first_delete);
  ASSERT_EQ(t, flushed_time);
}

TEST_F(DBFadeTest, PicksExpiredL0File) {
  Options options = FadeOptions();
  options.disable_auto_compactions = false;
  options.level0_file_num_compaction_trigger = 10;
  // Four tree levels with T = 2: deadlines {66, 200, 466, 1000}.
  options.max_bytes_for_level_multiplier = 2;
  auto listener = std::make_shared<CompactionReasonListener>();
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Put("z", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);

  Sleep(10 * kCadence);
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());
  Sleep(100);

  // A new version recomputes the expired files.
  ASSERT_OK(Put("q", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(listener->Count(CompactionReason::kDeletePersistence), 1);
  // The tombstone moved to L1, where its deadline has not passed yet.
  ASSERT_EQ("1,1,0,1", FilesPerLevel());
}

TEST_F(DBFadeTest, TombstonesReachLastLevelWithoutWrites) {
  Options options = FadeOptions();
  options.disable_auto_compactions = false;
  options.level0_file_num_compaction_trigger = 10;
  options.max_bytes_for_level_multiplier = 2;
  auto listener = std::make_shared<CompactionReasonListener>();
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Put("b", "v"));
  ASSERT_OK(Put("z", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);

  Sleep(10 * kCadence);
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  // No more writes: only the periodic trigger can notice expiry. The
  // tombstone must be gone from the tree within the threshold, plus one
  // trigger period of slack per level.
  Sleep(kDth + 4 * kCadence);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GE(listener->Count(CompactionReason::kDeletePersistence), 3);
  ASSERT_EQ("0,0,0,1", FilesPerLevel());
  ASSERT_EQ(Files()[3][0].num_deletions, 0u);
  ASSERT_EQ("NOT_FOUND", Get("b"));
}

TEST_F(DBFadeTest, FlushesMemtableWithExpiredTombstone) {
  Options options = FadeOptions();
  options.disable_auto_compactions = false;
  options.level0_file_num_compaction_trigger = 10;
  options.max_bytes_for_level_multiplier = 2;
  auto listener = std::make_shared<CompactionReasonListener>();
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Put("b", "v"));
  ASSERT_OK(Put("z", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);

  // The tombstone stays in the memtable: no flush, no further writes.
  Sleep(10 * kCadence);
  ASSERT_OK(Delete("b"));
  Sleep(kDth + 4 * kCadence);
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(listener->Count(FlushReason::kDeletePersistence), 1);
  ASSERT_EQ("0,0,0,1", FilesPerLevel());
  ASSERT_EQ(Files()[3][0].num_deletions, 0u);
  ASSERT_EQ("NOT_FOUND", Get("b"));
}

TEST_F(DBFadeTest, MemtableWithoutTombstoneNotFlushed) {
  Options options = FadeOptions();
  auto listener = std::make_shared<CompactionReasonListener>();
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "v"));
  Sleep(2 * kDth);
  ASSERT_EQ(listener->Count(FlushReason::kDeletePersistence), 0);
  ASSERT_EQ("", FilesPerLevel());
}

TEST_F(DBFadeTest, ZeroThresholdNeverPicks) {
  Options options = FadeOptions();
  options.delete_persistence_threshold = 0;
  options.disable_auto_compactions = false;
  options.level0_file_num_compaction_trigger = 10;
  auto listener = std::make_shared<CompactionReasonListener>();
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Put("z", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());
  mock_clock_->MockSleepForSeconds(10 * static_cast<int>(kDth));
  ASSERT_OK(Put("q", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(listener->Count(CompactionReason::kDeletePersistence), 0);
  ASSERT_EQ("2,0,0,1", FilesPerLevel());
}

TEST_F(DBFadeTest, MarkerSetsDptOfFollowingDeletes) {
  Options options = FadeOptions();
  options.delete_persistence_threshold = 0;
  DestroyAndReopen(options);
  const SequenceNumber first = db_->GetLatestSequenceNumber() + 1;

  WriteBatch batch;
  ASSERT_OK(batch.Delete("a"));  // first: before any marker
  ASSERT_OK(batch.PutDeletePersistenceThreshold(100, 5000));
  ASSERT_OK(batch.Delete("b"));                 // first + 1
  ASSERT_OK(batch.SingleDelete("c"));           // first + 2
  ASSERT_OK(batch.Put("d", "v"));               // first + 3
  ASSERT_OK(batch.DeleteRange("e", "f"));       // first + 4
  ASSERT_OK(batch.PutDeletePersistenceThreshold(0, 5000));
  ASSERT_OK(batch.Delete("g"));                 // first + 5
  ASSERT_OK(batch.PutDeletePersistenceThreshold(50, 6000));
  ASSERT_OK(batch.Delete("h"));                 // first + 6
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::vector<TombstoneDpt> expected = {{first + 1, first + 2, 100, 5000},
                                        {first + 4, first + 4, 100, 5000},
                                        {first + 6, first + 6, 50, 6000}};
  ASSERT_EQ(MemtableDpts(db_), expected);
}

TEST_F(DBFadeTest, MarkerDoesNotCarryIntoNextBatch) {
  Options options = FadeOptions();
  options.delete_persistence_threshold = 0;
  DestroyAndReopen(options);
  const SequenceNumber first = db_->GetLatestSequenceNumber() + 1;

  WriteBatch with_marker;
  ASSERT_OK(with_marker.PutDeletePersistenceThreshold(100, 5000));
  ASSERT_OK(with_marker.Delete("a"));
  ASSERT_OK(db_->Write(WriteOptions(), &with_marker));
  ASSERT_OK(Delete("b"));

  std::vector<TombstoneDpt> expected = {{first, first, 100, 5000}};
  ASSERT_EQ(MemtableDpts(db_), expected);
}

TEST_F(DBFadeTest, MarkerSurvivesWalReplay) {
  Options options = FadeOptions();
  options.delete_persistence_threshold = 0;
  // Keep the replayed tombstone in the memtable.
  options.avoid_flush_during_recovery = true;
  DestroyAndReopen(options);
  const SequenceNumber first = db_->GetLatestSequenceNumber() + 1;

  WriteBatch batch;
  ASSERT_OK(batch.PutDeletePersistenceThreshold(100, Now()));
  ASSERT_OK(batch.Delete("a"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  const uint64_t write_time = Now();
  Sleep(5 * kCadence);
  Reopen(options);

  std::vector<TombstoneDpt> expected = {{first, first, 100, write_time}};
  ASSERT_EQ(MemtableDpts(db_), expected);
}

class DBFadeTwoPhaseTest
    : public DBFadeTest,
      public testing::WithParamInterface<TxnDBWritePolicy> {};

// A prepared transaction rebuilt during recovery keeps its marker. Only
// WRITE_COMMITTED writes the memtable after recovery: replayed writes are
// flushed during recovery, since 2PC forces avoid_flush_during_recovery off.
TEST_P(DBFadeTwoPhaseTest, MarkerSurvivesRecovery) {
  const TxnDBWritePolicy policy = GetParam();
  Options options = FadeOptions();
  options.delete_persistence_threshold = 0;
  DestroyAndReopen(options);
  Close();
  TransactionDBOptions txn_db_options;
  txn_db_options.write_policy = policy;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname_, &txn_db));
  std::unique_ptr<TransactionDB> txn_db_guard(txn_db);

  WriteOptions write_options;
  write_options.sync = true;
  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn->SetName("t"));
  ASSERT_OK(txn->Put("a", "v"));
  txn->PutLogData(WriteBatch::DeletePersistenceThresholdLogData(100, 5000));
  ASSERT_OK(txn->Delete("b"));
  ASSERT_OK(txn->Prepare());
  delete txn;
  static_cast<PessimisticTransactionDB*>(txn_db)->TEST_Crash();
  txn_db_guard.reset();

  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname_, &txn_db));
  txn_db_guard.reset(txn_db);
  txn = txn_db->GetTransactionByName("t");
  ASSERT_NE(txn, nullptr);
  ASSERT_OK(txn->Commit());
  delete txn;
  auto entries = MemtableDpts(txn_db);
  ASSERT_EQ(entries.size(), 1u);
  ASSERT_EQ(entries[0].first_seqno, entries[0].last_seqno);
  ASSERT_EQ(entries[0].dpt, 100u);
  ASSERT_EQ(entries[0].write_time, 5000u);
  std::string value;
  ASSERT_TRUE(txn_db->Get(ReadOptions(), "b", &value).IsNotFound());
  ASSERT_OK(txn_db->Get(ReadOptions(), "a", &value));
}

INSTANTIATE_TEST_CASE_P(
    DBFadeTwoPhaseTest, DBFadeTwoPhaseTest,
    testing::Values(WRITE_COMMITTED));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  RegisterCustomObjects(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
