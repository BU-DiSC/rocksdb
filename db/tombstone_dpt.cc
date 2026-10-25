//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/tombstone_dpt.h"

#include <algorithm>
#include <limits>

#include "db/dbformat.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

namespace {
bool SameDpt(const TombstoneDpt& e, uint64_t dpt, uint64_t write_time) {
  return e.dpt == dpt && e.write_time == write_time;
}

uint64_t SaturatingAdd(uint64_t a, uint64_t b) {
  return a > std::numeric_limits<uint64_t>::max() - b
             ? std::numeric_limits<uint64_t>::max()
             : a + b;
}

// Combines the DPT and write time of `other` into `into`. The deadline at
// level fraction f in [0, 1] is write_time + dpt * f, a line in f. The
// combined line runs through the minimum of both lines at f = 0 and at f = 1,
// so it is at or below both of them everywhere on [0, 1].
void CombineDpt(TombstoneDpt* into, uint64_t dpt, uint64_t write_time) {
  const uint64_t start = std::min(into->write_time, write_time);
  const uint64_t end = std::min(SaturatingAdd(into->write_time, into->dpt),
                                SaturatingAdd(write_time, dpt));
  into->write_time = start;
  into->dpt = end - start;
}

void Combine(TombstoneDpt* into, const TombstoneDpt& other) {
  into->first_seqno = std::min(into->first_seqno, other.first_seqno);
  into->last_seqno = std::max(into->last_seqno, other.last_seqno);
  CombineDpt(into, other.dpt, other.write_time);
}
}  // namespace

void TombstoneDptMap::Add(SequenceNumber seqno, uint64_t dpt,
                          uint64_t write_time) {
  auto next = std::upper_bound(
      entries_.begin(), entries_.end(), seqno,
      [](SequenceNumber s, const TombstoneDpt& e) { return s < e.first_seqno; });
  if (next != entries_.begin()) {
    auto prev = std::prev(next);
    if (prev->last_seqno >= seqno) {
      // Already covered, e.g. by a merged entry.
      CombineDpt(&*prev, dpt, write_time);
      return;
    }
    if (prev->last_seqno + 1 == seqno && SameDpt(*prev, dpt, write_time)) {
      prev->last_seqno = seqno;
      if (next != entries_.end() && next->first_seqno == seqno + 1 &&
          SameDpt(*next, dpt, write_time)) {
        prev->last_seqno = next->last_seqno;
        entries_.erase(next);
      }
      return;
    }
  }
  if (next != entries_.end() && next->first_seqno == seqno + 1 &&
      SameDpt(*next, dpt, write_time)) {
    next->first_seqno = seqno;
    return;
  }
  entries_.insert(next, TombstoneDpt{seqno, seqno, dpt, write_time});
}

void TombstoneDptMap::Add(const TombstoneDpt& entry) {
  // First entry that ends at or after the new one starts.
  auto begin = std::lower_bound(entries_.begin(), entries_.end(),
                                entry.first_seqno,
                                [](const TombstoneDpt& e, SequenceNumber s) {
                                  return e.last_seqno < s;
                                });
  auto end = begin;
  TombstoneDpt combined = entry;
  while (end != entries_.end() && end->first_seqno <= entry.last_seqno) {
    Combine(&combined, *end);
    ++end;
  }
  begin = entries_.erase(begin, end);
  entries_.insert(begin, combined);
}

void TombstoneDptMap::Merge(const TombstoneDptMap& other) {
  if (entries_.empty()) {
    entries_ = other.entries_;
    return;
  }
  for (const TombstoneDpt& e : other.entries_) {
    Add(e);
  }
}

const TombstoneDpt* TombstoneDptMap::Find(SequenceNumber seqno) const {
  auto next = std::upper_bound(
      entries_.begin(), entries_.end(), seqno,
      [](SequenceNumber s, const TombstoneDpt& e) { return s < e.first_seqno; });
  if (next == entries_.begin()) {
    return nullptr;
  }
  const TombstoneDpt& prev = *std::prev(next);
  return prev.last_seqno >= seqno ? &prev : nullptr;
}

void TombstoneDptMap::Shrink(size_t max_entries) {
  if (max_entries == 0) {
    max_entries = 1;
  }
  while (entries_.size() > max_entries) {
    std::vector<TombstoneDpt> merged;
    merged.reserve((entries_.size() + 1) / 2);
    for (size_t i = 0; i < entries_.size(); i += 2) {
      TombstoneDpt e = entries_[i];
      if (i + 1 < entries_.size()) {
        Combine(&e, entries_[i + 1]);
      }
      merged.push_back(e);
    }
    entries_ = std::move(merged);
  }
}

void TombstoneDptMap::EncodeTo(std::string* dst) const {
  PutVarint64(dst, entries_.size());
  for (const TombstoneDpt& e : entries_) {
    PutVarint64(dst, e.first_seqno);
    PutVarint64(dst, e.last_seqno - e.first_seqno);
    PutVarint64(dst, e.dpt);
    PutVarint64(dst, e.write_time);
  }
}

Status TombstoneDptMap::DecodeFrom(Slice* src) {
  entries_.clear();
  uint64_t count = 0;
  if (!GetVarint64(src, &count)) {
    return Status::Corruption("tombstone DPT count");
  }
  for (uint64_t i = 0; i < count; ++i) {
    TombstoneDpt e;
    uint64_t span = 0;
    if (!GetVarint64(src, &e.first_seqno) || !GetVarint64(src, &span) ||
        !GetVarint64(src, &e.dpt) || !GetVarint64(src, &e.write_time)) {
      return Status::Corruption("tombstone DPT entry");
    }
    if (span > kMaxSequenceNumber - e.first_seqno) {
      return Status::Corruption("tombstone DPT range");
    }
    e.last_seqno = e.first_seqno + span;
    if (!entries_.empty() && entries_.back().last_seqno >= e.first_seqno) {
      return Status::Corruption("tombstone DPT order");
    }
    entries_.push_back(e);
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
