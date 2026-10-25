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

#include "db/dbformat.h"
#include "db/seqno_to_time_mapping.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Returns, for each of the `num_tree_levels` levels of the tree (level 0
// first), the fraction of a threshold that a tombstone may have spent by the
// time it leaves that level. The last entry is exactly 1. When `size_ratio`
// is not above 1 the threshold is split uniformly across levels. Returns an
// empty vector when the tree has no levels.
inline std::vector<long double> FadeLevelFractions(double size_ratio,
                                                   int num_tree_levels) {
  std::vector<long double> fractions;
  if (num_tree_levels <= 0) {
    return fractions;
  }
  fractions.reserve(num_tree_levels);
  const long double levels = static_cast<long double>(num_tree_levels);
  const long double ratio = static_cast<long double>(size_ratio);
  for (int k = 0; k < num_tree_levels; ++k) {
    long double fraction;
    if (ratio <= 1.0L) {
      fraction = (k + 1) / levels;
    } else {
      // Sum of d_0 * T^j for j in [0, k], in closed form.
      fraction =
          (std::pow(ratio, k + 1) - 1.0L) / (std::pow(ratio, levels) - 1.0L);
    }
    if (!(fraction < 1.0L)) {
      fraction = 1.0L;
    }
    fractions.push_back(fraction);
  }
  fractions.back() = 1.0L;
  return fractions;
}

// The part of `dpt` given by `fraction`, in whole seconds. A fraction of 1
// yields exactly `dpt`.
inline uint64_t FadeDeadline(uint64_t dpt, long double fraction) {
  if (!(fraction < 1.0L)) {
    return dpt;
  }
  return static_cast<uint64_t>(static_cast<long double>(dpt) * fraction);
}

// Returns the cumulative deadline, in seconds since the tombstone was written,
// for each of the `num_tree_levels` levels of the tree (level 0 first). The
// last entry always equals `dth`. Returns an empty vector when FADE is
// disabled (`dth` is 0) or the tree has no levels.
inline std::vector<uint64_t> FadeLevelDeadlines(uint64_t dth, double size_ratio,
                                                int num_tree_levels) {
  std::vector<uint64_t> deadlines;
  if (dth == 0) {
    return deadlines;
  }
  for (long double f : FadeLevelFractions(size_ratio, num_tree_levels)) {
    deadlines.push_back(FadeDeadline(dth, f));
  }
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

// Estimates the write time of the tombstone with sequence number `seqno`.
// The seqno-to-time mapping yields a time no later than the actual write,
// so the estimate errs towards expiring early. Returns 0 when there is no
// tombstone (`seqno` is kMaxSequenceNumber), and `fallback` when there is no
// mapping or the mapping does not cover `seqno`.
inline uint64_t EstimateTombstoneTime(const SeqnoToTimeMapping* mapping,
                                      SequenceNumber seqno,
                                      uint64_t fallback) {
  if (seqno == kMaxSequenceNumber) {
    return 0;
  }
  uint64_t t = kUnknownTimeBeforeAll;
  if (mapping != nullptr && !mapping->Empty()) {
    t = mapping->GetProximalTimeBeforeSeqno(seqno);
  }
  return t != kUnknownTimeBeforeAll ? t : fallback;
}

}  // namespace ROCKSDB_NAMESPACE
