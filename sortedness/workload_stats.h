/*
 *  Created on: Oct 9, 2019
 *  Author: Guanting Chen
 */

#ifndef WORKLOAD_STATS_H_
#define WORKLOAD_STATS_H_

#include <iostream>
#include <fstream>
#include <vector>
#include <assert.h>

enum QueryType : char {
  INSERT = 'I',
  UPDATE = 'U',
  DELETE = 'D',
  LOOKUP = 'Q',
  RANGE_DELETE = 'R',
  RANGE_LOOKUP = 'S',
  NONE = 0x00
};
// 4 Bytes per point lookup/delete entry
struct BaseEntry{
  uint32_t key;
  BaseEntry() : key(0) {}
  virtual ~BaseEntry() = default;
  explicit BaseEntry(uint32_t k) : key(k){}
};
// 4 + value size per insert/update query
struct Entry : public BaseEntry{
  std::string value;
  Entry() : BaseEntry(), value("") {}
  explicit Entry(uint32_t k) : BaseEntry(k), value("") {}
  explicit Entry(uint32_t k,  std::string v) : BaseEntry(k), value(v) {}
};
// 8 Bytes per range query;
struct RangeEntry : public BaseEntry {
  uint32_t range;
  RangeEntry() : BaseEntry(), range(0) {}
  explicit RangeEntry(uint32_t start_key, uint32_t diff)
   : BaseEntry(start_key), range(diff) {
    assert(range >= 0 && "Key range must >= 0");
  }
};

// Store descriptions and apointer to an query entry for efficient use 
// Maximal overhead: 17 Bytes per query;
struct QueryDescriptor {
  uint64_t seq;   // >0 && =index + 1;
  QueryType type;
  BaseEntry *entry_ptr;
  QueryDescriptor() : seq(0), type(NONE), entry_ptr(nullptr) {}
  explicit QueryDescriptor(uint64_t seq_, QueryType ktype, BaseEntry *entry) 
   : seq(seq_), type(ktype), entry_ptr(entry) {}
};

// Store in-memory workload and relative stats 
struct WorkloadDescriptor {
  std::string path_;       // workload path
  uint64_t total_num = 0;
  uint64_t actual_total_num = 0;	
  uint64_t insert_num = 0;
  uint64_t actual_insert_num = 0;		// for pseudo zero result point lookup
  uint64_t update_num = 0;
  uint64_t plookup_num = 0;
  uint64_t rlookup_num = 0;
  uint64_t pdelete_num = 0;
  uint64_t rdelete_num = 0;
  // double plookup_hit_rate = 0.2;		// percentage of zero result point lookup
  std::vector<QueryDescriptor> queries;
  WorkloadDescriptor() : path_("workload.txt") {}
  explicit WorkloadDescriptor(std::string path) : path_(path) {}
};

// Keep track of all performance metrics during queries execution
struct QueryTracker {
  uint64_t total_completed = 0;
  uint64_t inserts_completed = 0;
  uint64_t updates_completed = 0;
  uint64_t point_deletes_completed = 0;
  uint64_t range_deletes_completed = 0;
  uint64_t point_lookups_completed = 0;
  uint64_t zero_point_lookups_completed = 0;
  uint64_t range_lookups_completed = 0;

  // Cumulative latency cost
  uint64_t inserts_cost = 0;
  uint64_t updates_cost = 0;
  uint64_t point_deletes_cost = 0;
  uint64_t range_deletes_cost = 0;
  uint64_t point_lookups_cost = 0;
  uint64_t zero_point_lookups_cost = 0;
  uint64_t range_lookups_cost = 0;
  uint64_t workload_exec_time = 0;
  uint64_t experiment_exec_time = 0;

  // Lookup related, bloom, IO
  uint64_t bloom_memtable_hit_count = 0;    // retrieve filter block from memory and key exists (could be false positive)
  uint64_t bloom_memtable_miss_count = 0;   // retrieve filter block from memory and key not exist
  uint64_t bloom_sst_hit_count = 0;         // retrieve filter block from sst(I/O) and key exists (could be false positive)
  uint64_t bloom_sst_miss_count = 0;        // retrieve filter block from sst(I/O) and key not exist
  uint64_t get_from_memtable_time = 0;          // total nanos spent on querying memtables
  uint64_t get_from_memtable_count = 0;         // number of mem tables queried
  uint64_t get_from_output_files_time = 0;      // total nanos reading from output files
  uint64_t filter_block_read_count = 0;         // total number of filter block reads
  uint64_t bytes_read = 0;                      // total bytes by IO read
  uint64_t read_nanos = 0;                      // total time by IO read
  uint64_t bytes_written = 0;                   // total bytes by IO write
  uint64_t write_nanos = 0;                     // total time by IO wirte
  uint64_t cpu_write_nanos = 0;
  uint64_t cpu_read_nanos = 0;

  // Compaction cost
  double read_amp = 0;
  double write_amp = 0;
  double space_amp = 0;
  uint64_t stalls = 0;

};

// Preload workload into memory,
// which is stored in a WorkloadDescriptor
void loadWorkload(WorkloadDescriptor *wd);

// Dump stats from a single track into a cumulative sample
// to compute cumulative and average result
void dumpStats(QueryTracker *sample, const QueryTracker *single);

#endif /*WORKLOAD_STATS_H_*/