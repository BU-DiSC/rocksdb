//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Per-tombstone delete persistence thresholds. A write batch can set a DPT
// for the deletes that follow it (WriteBatch::SetDeletePersistenceThreshold).
// Since the tombstone format is unchanged, the DPT and write time of each such
// tombstone live in a side map keyed by sequence number, which travels from
// the memtable to the SST files' metadata and on through compaction.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// The tombstones with sequence numbers in [first_seqno, last_seqno] must be
// removed within `dpt` seconds of `write_time`; at a level that gets fraction
// f of the threshold, by write_time + dpt * f. When entries are merged to
// bound memory, the merged deadline is at or before those of the merged
// entries for every f in [0, 1].
struct TombstoneDpt {
  SequenceNumber first_seqno = 0;
  SequenceNumber last_seqno = 0;
  uint64_t dpt = 0;
  uint64_t write_time = 0;

  bool operator==(const TombstoneDpt& other) const {
    return first_seqno == other.first_seqno &&
           last_seqno == other.last_seqno && dpt == other.dpt &&
           write_time == other.write_time;
  }
};

// Sorted, non-overlapping runs of tombstone sequence numbers with their DPT.
// Not thread safe.
class TombstoneDptMap {
 public:
  // Entries kept per SST file.
  static constexpr size_t kMaxEntriesPerFile = 64;

  bool Empty() const { return entries_.empty(); }
  size_t Size() const { return entries_.size(); }
  const std::vector<TombstoneDpt>& entries() const { return entries_; }
  void Clear() { entries_.clear(); }

  // Records the tombstone with sequence number `seqno`. Extends a
  // neighbouring run with the same DPT and write time when possible.
  void Add(SequenceNumber seqno, uint64_t dpt, uint64_t write_time);

  // Adds an entry, merging it with any entries it overlaps.
  void Add(const TombstoneDpt& entry);

  // Adds all entries of `other`.
  void Merge(const TombstoneDptMap& other);

  // The entry covering `seqno`, or nullptr.
  const TombstoneDpt* Find(SequenceNumber seqno) const;

  // Merges neighbouring entries until at most `max_entries` remain.
  void Shrink(size_t max_entries);

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  bool operator==(const TombstoneDptMap& other) const {
    return entries_ == other.entries_;
  }

 private:
  std::vector<TombstoneDpt> entries_;
};

}  // namespace ROCKSDB_NAMESPACE
