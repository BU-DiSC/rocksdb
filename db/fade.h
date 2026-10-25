//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// FADE (FAst DEletes) delete-persistence helpers, after Lethe (Sarkar et al.,
// SIGMOD 2020, section 4.1). FADE bounds how long a tombstone can live in the
// LSM tree by giving every tree level a time-to-live d_i that grows with the
// size ratio T, such that the TTLs of all levels sum to the delete persistence
// threshold D_th:
//
//   d_0 = D_th * (T - 1) / (T^L - 1),  d_i = T * d_{i-1}
//
// A file at tree level k has an expired tombstone once its oldest tombstone is
// older than the cumulative deadline d_0 + ... + d_k.

#pragma once

#include <cmath>
#include <cstdint>
#include <limits>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Returns the cumulative deadline, in seconds since the tombstone was written,
// for each of the `num_tree_levels` levels of the tree (level 0 first). The
// last entry always equals `dth`. When `size_ratio` is not above 1 the
// threshold is split uniformly across levels. Returns an empty vector when
// FADE is disabled (`dth` is 0) or the tree has no levels.
inline std::vector<uint64_t> FadeLevelDeadlines(uint64_t dth, double size_ratio,
                                                int num_tree_levels) {
  std::vector<uint64_t> deadlines;
  if (dth == 0 || num_tree_levels <= 0) {
    return deadlines;
  }
  deadlines.reserve(num_tree_levels);
  const long double total = static_cast<long double>(dth);
  const long double levels = static_cast<long double>(num_tree_levels);
  const long double ratio = static_cast<long double>(size_ratio);
  for (int k = 0; k < num_tree_levels; ++k) {
    long double cumulative;
    if (ratio <= 1.0L) {
      cumulative = total * (k + 1) / levels;
    } else {
      // Sum of d_0 * T^j for j in [0, k], in closed form.
      cumulative = total * (std::pow(ratio, k + 1) - 1.0L) /
                   (std::pow(ratio, levels) - 1.0L);
    }
    if (!(cumulative < total)) {
      cumulative = total;
    }
    deadlines.push_back(static_cast<uint64_t>(cumulative));
  }
  deadlines.back() = dth;
  return deadlines;
}

// Whether a file whose oldest tombstone was written at `oldest_tombstone_time`
// has outlived `deadline` at time `now`. A time of 0 means the file has no
// tombstones or the time is unknown, which never expires.
inline bool IsTombstoneExpired(uint64_t oldest_tombstone_time, uint64_t now,
                               uint64_t deadline) {
  return oldest_tombstone_time != 0 && now >= oldest_tombstone_time &&
         now - oldest_tombstone_time > deadline;
}

}  // namespace ROCKSDB_NAMESPACE
