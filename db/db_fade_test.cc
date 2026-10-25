//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/fade.h"

#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "port/stack_trace.h"
#include "test_util/mock_time_env.h"
#include "test_util/testharness.h"

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

// Counts compactions by reason.
class CompactionReasonListener : public EventListener {
 public:
  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    counts_[ci.compaction_reason]++;
  }

  int Count(CompactionReason reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    return counts_[reason];
  }

 private:
  std::mutex mutex_;
  std::map<CompactionReason, int> counts_;
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
  Sleep(10 * kCadence);
  const uint64_t delete_time = Now();
  ASSERT_OK(Delete("b"));
  Sleep(10 * kCadence);
  ASSERT_OK(Delete("c"));
  Sleep(10 * kCadence);
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  RegisterCustomObjects(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
