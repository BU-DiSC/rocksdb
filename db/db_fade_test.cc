//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/fade.h"

#include "port/stack_trace.h"
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
