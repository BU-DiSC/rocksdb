// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iomanip>
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "../util/string_util.h"
#include "args.hxx"
#include "aux_time.h"
#include "emu_environment.h"
#include "workload_stats.h"
#include "emu_util.h"

using namespace rocksdb;

// Specify your path of workload file here
std::string workloadPath = "./workload.txt";
std::string kDBPath = "./db_working_home";
QueryTracker query_stats;

int parse_arguments2(int argc, char *argv[], EmuEnv* _env);
void printEmulationOutput(const EmuEnv* _env, const QueryTracker *track, uint16_t n = 1);
void configOptions(EmuEnv* _env, Options *op, BlockBasedTableOptions *table_op, WriteOptions *write_op, ReadOptions *read_op, FlushOptions *flush_op);
int runWorkload(DB *&db, const EmuEnv* _env, const Options *op,
                const BlockBasedTableOptions *table_op, const WriteOptions *write_op, 
                const ReadOptions *read_op, const FlushOptions *flush_op,
                const WorkloadDescriptor *wd, QueryTracker *query_track);   // run_workload internal
int runExperiments(EmuEnv* _env);    // API

/*
  CrimsonDBFiltersPolicy* get_filters_policy(EmuEnv* _env);
  void query(EmuEnv* _env, vector<string>& existing_keys);
  Status bulk_load(EmuEnv* _env, long &total_bulk_loaded_entries);
  void sand();
  long deletes(EmuEnv* _env);
  long updates(EmuEnv* _env);
*/
//moved in exp_infrastructure
//void collect_existing_entries(EmuEnv* _env, vector<string>& existing_keys );
//std::string get_time_string();

int main(int argc, char *argv[]) {
  // check emu_environment.h for the contents of EmuEnv and also the definitions of the singleton experimental environment 
  EmuEnv* _env = EmuEnv::getInstance();
  // parse the command line arguments
  if (parse_arguments2(argc, argv, _env)) {
    exit(1);
  }

  my_clock start_time, end_time;
  std::cout << "Starting experiments ..."<<std::endl;
  if (my_clock_get_time(&start_time) == -1) {
    std::cerr << "Failed to get experiment start time" << std::endl;
  }
  int s = runExperiments(_env); 
  if (my_clock_get_time(&end_time) == -1) {
    std::cerr << "Failed to get experiment end time" << std::endl;
  }
  query_stats.experiment_exec_time = getclock_diff_ns(start_time, end_time);

  std::cout << std::endl << std::fixed << std::setprecision(2) 
            << "===== End of all experiments in "
            << static_cast<double>(query_stats.experiment_exec_time)/1000000 << "ms !! ===== "<< std::endl;
  
  // show average results for the number of experiment runs
  printEmulationOutput(_env, &query_stats, _env->experiment_runs);
  std::cout << "===== Average stats of " << _env->experiment_runs << " runs ====="  << std::endl;

/*
  if (_env->destroy) {
    DestroyDB(_env->path, Options());
  }

  srand((unsigned int)time(NULL));
  
  //this is not a parameter yet 
  _env->use_block_based_filter = false;

  //measure the load time and entries
  long bulk_loaded_entries=0;

  // if (_env->verbosity >= 1)
    std::cerr << "Bulk loading DB ... " << std::endl << std::flush;

  bulk_load(_env, bulk_loaded_entries);

  vector<string> existing_keys;

  if (_env->nonzero_to_zero_ratio>0)
  {
    // if (_env->verbosity >= 1)
      std::cerr << "Collecting existing keys ... " << std::endl << std::flush;
    collect_existing_entries(_env, existing_keys);
  }

  
  if (_env->clean_caches_for_experiments)
  {
    std::cerr << "Emptying the caches ... " << std::endl << std::flush;
    system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
  }
  
  // Issuing QUERIES
  if (_env->num_queries>0)
  {
    // if (_env->verbosity >= 1) 
      std::cerr << "Issuing queries ... " << std::endl << std::flush;
    query(_env, existing_keys);
  }

  if (_env->clean_caches_for_experiments)
  {
    std::cerr << "Emptying the caches ... " << std::endl << std::flush;
    system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
  }  

  // Issuing UPDATES
  if (_env->num_updates>0)
  {
    // if (_env->verbosity >= 1) 
      std::cerr << "Issuing updates ... " << std::endl << std::flush;
    long updated = updates(_env);
    assert(_env->num_updates == updated);
  }  

  //// ENABLING DELETES
  if (_env->num_deletes>0)
  {
    // if (_env->verbosity >= 1) 
      std::cerr << "Issuing deletes ... " << std::endl << std::flush;
    //assert(_env->num_deletes <= _env->num_inserts);
    long deleted = deletes(_env);
    assert(_env->num_deletes == deleted);
  } 

  // if (_env->verbosity >= 1) 
    std::cerr << "End of experiment!" << std::endl << std::flush;
*/
  return 0;
}

void configOptions(EmuEnv* _env, Options *op, BlockBasedTableOptions *table_op, WriteOptions *write_op, ReadOptions *read_op, FlushOptions *flush_op) {
    // Experiment settings
    _env->experiment_runs = (_env->experiment_runs >= 1) ? _env->experiment_runs : 1;
    // *op = Options();
    op->write_buffer_size = _env->buffer_size;
    op->max_write_buffer_number = _env->max_write_buffer_number;   // min 2
    
    switch (_env->memtable_factory) {
      case 1:
        op->memtable_factory = std::shared_ptr<SkipListFactory>(new SkipListFactory); break;
      case 2:
        op->memtable_factory = std::shared_ptr<VectorRepFactory>(new VectorRepFactory); break;
      case 3:
        op->memtable_factory.reset(NewHashSkipListRepFactory()); break;
      case 4:
        op->memtable_factory.reset(NewHashLinkListRepFactory()); break;
      default:
        std::cerr << "error: memtable_factory" << std::endl;
    }

    // Compaction
    switch (_env->compaction_pri) {
      case 1:
        op->compaction_pri = kMinOverlappingRatio; break;
      case 2:
        op->compaction_pri = kByCompensatedSize; break;
      case 3:
        op->compaction_pri = kOldestLargestSeqFirst; break;
      case 4:
        op->compaction_pri = kOldestSmallestSeqFirst; break;
      default:
        std::cerr << "error: compaction_pri" << std::endl;
    }

    op->max_bytes_for_level_multiplier = _env->size_ratio;
    op->target_file_size_base = _env->file_size;
    op->level_compaction_dynamic_level_bytes = _env->level_compaction_dynamic_level_bytes;
    switch (_env->compaction_style) {
      case 1:
        op->compaction_style = kCompactionStyleLevel; break;
      case 2:
        op->compaction_style = kCompactionStyleUniversal; break;
      case 3:
        op->compaction_style = kCompactionStyleFIFO; break;
      case 4:
        op->compaction_style = kCompactionStyleNone; break;
      default:
        std::cerr << "error: compaction_style" << std::endl;
    }
    
    op->disable_auto_compactions = _env->disable_auto_compactions;
    if (_env->compaction_filter == 0) {
      ;// do nothing
    } else {
      ;// invoke manual compaction_filter
    }
    if (_env->compaction_filter_factory == 0) {
      ;// do nothing
    } else {
      ;// invoke manual compaction_filter_factory
    }
    switch (_env->access_hint_on_compaction_start) {
      case 1:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::NONE; break;
      case 2:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::NORMAL; break;
      case 3:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::SEQUENTIAL; break;
      case 4:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::WILLNEED; break;
      default:
        std::cerr << "error: access_hint_on_compaction_start" << std::endl;
    }
    
    op->level0_file_num_compaction_trigger = _env->level0_file_num_compaction_trigger;
    op->level0_slowdown_writes_trigger = _env->level0_slowdown_writes_trigger;
    op->level0_stop_writes_trigger = _env->level0_stop_writes_trigger;
    op->target_file_size_multiplier = _env->target_file_size_multiplier;
    op->max_background_jobs = _env->max_background_jobs;
    op->max_compaction_bytes = _env->max_compaction_bytes;
    op->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio;;
    if (_env->merge_operator == 0) {
      ;// do nothing
    } 
    else {
      ;// use custom merge operator
    }
    op->soft_pending_compaction_bytes_limit = _env->soft_pending_compaction_bytes_limit;    // No pending compaction anytime, try and see
    op->hard_pending_compaction_bytes_limit = _env->hard_pending_compaction_bytes_limit;    // No pending compaction anytime, try and see
    op->periodic_compaction_seconds = _env->periodic_compaction_seconds;
    op->use_direct_io_for_flush_and_compaction = _env->use_direct_io_for_flush_and_compaction;
    op->num_levels = _env->num_levels;


    //Compression
    switch (_env->compression) {
      case 1:
        op->compression = kNoCompression; break;
      case 2:
        op->compression = kSnappyCompression; break;
      case 3:
        op->compression = kZlibCompression; break;
      case 4:
        op->compression = kBZip2Compression; break;
      case 5:
      op->compression = kLZ4Compression; break;
      case 6:
      op->compression = kLZ4HCCompression; break;
      case 7:
      op->compression = kXpressCompression; break;
      case 8:
      op->compression = kZSTD; break;
      case 9:
      op->compression = kZSTDNotFinalCompression; break;
      case 10:
      op->compression = kDisableCompressionOption; break;

      default:
        std::cerr << "error: compression" << std::endl;
    }

  // table_options.enable_index_compression = kNoCompression;

  // Other CFOptions
  switch (_env->comparator) {
      case 1:
        op->comparator = BytewiseComparator(); break;
      case 2:
        op->comparator = ReverseBytewiseComparator(); break;
      case 3:
        // use custom comparator
        break;
      default:
        std::cerr << "error: comparator" << std::endl;
    }

  op->max_sequential_skip_in_iterations = _env-> max_sequential_skip_in_iterations;
  op->memtable_prefix_bloom_size_ratio = _env-> memtable_prefix_bloom_size_ratio;    // disabled
  op->paranoid_file_checks = _env->paranoid_file_checks;
  op->optimize_filters_for_hits = _env->optimize_filters_for_hits;
  op->inplace_update_support = _env->inplace_update_support;
  op->inplace_update_num_locks = _env->inplace_update_num_locks;
  op->report_bg_io_stats = _env->report_bg_io_stats;
  op->max_successive_merges = _env->max_successive_merges;   // read-modified-write related

  //Other DBOptions
  op->create_if_missing = _env->create_if_missing;
  op->delayed_write_rate = _env->delayed_write_rate;
  op->max_open_files = _env->max_open_files;
  op->max_file_opening_threads = _env->max_file_opening_threads;
  op->bytes_per_sync = _env->bytes_per_sync;
  op->stats_persist_period_sec = _env->stats_persist_period_sec;
  op->enable_thread_tracking = _env->enable_thread_tracking;
  op->stats_history_buffer_size = _env->stats_history_buffer_size;
  op->allow_concurrent_memtable_write = _env->allow_concurrent_memtable_write;
  op->dump_malloc_stats = _env->dump_malloc_stats;
  op->use_direct_reads = _env->use_direct_reads;
  op->avoid_flush_during_shutdown = _env->avoid_flush_during_shutdown;
  op->advise_random_on_open = _env->advise_random_on_open;
  op->delete_obsolete_files_period_micros = _env->delete_obsolete_files_period_micros;   // 6 hours
  op->allow_mmap_reads = _env->allow_mmap_reads;
  op->allow_mmap_writes = _env->allow_mmap_writes;

  //TableOptions
  table_op->no_block_cache = _env->no_block_cache; // TBC
  if (_env->block_cache == 0) {
      ;// do nothing
  } else {
      ;// invoke manual block_cache
  }

  if (_env->bits_per_key == 0) {
      ;// do nothing
  } else {
    table_op->filter_policy.reset(NewBloomFilterPolicy(_env->bits_per_key, false));    // currently build full filter instead of blcok-based filter
  }

  table_op->cache_index_and_filter_blocks = _env->cache_index_and_filter_blocks;
  table_op->cache_index_and_filter_blocks_with_high_priority = _env->cache_index_and_filter_blocks_with_high_priority;    // Deprecated by no_block_cache
  table_op->read_amp_bytes_per_bit = _env->read_amp_bytes_per_bit;
  
  switch (_env->data_block_index_type) {
      case 1:
        table_op->data_block_index_type = BlockBasedTableOptions::kDataBlockBinarySearch; break;
      case 2:
        table_op->data_block_index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash; break;
      default:
        std::cerr << "error: TableOptions::data_block_index_type" << std::endl;
  }
  switch (_env->index_type) {
      case 1:
        table_op->index_type = BlockBasedTableOptions::kBinarySearch; break;
      case 2:
        table_op->index_type = BlockBasedTableOptions::kHashSearch; break;
      case 3:
        table_op->index_type = BlockBasedTableOptions::kTwoLevelIndexSearch; break;
      default:
        std::cerr << "error: TableOptions::index_type" << std::endl;
  }
  table_op->partition_filters = _env->partition_filters;
  table_op->block_size = _env->entries_per_page * _env->entry_size;
  table_op->metadata_block_size = _env->metadata_block_size;
  table_op->pin_top_level_index_and_filter = _env->pin_top_level_index_and_filter;
  
  switch (_env->index_shortening) {
      case 1:
        table_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kNoShortening; break;
      case 2:
        table_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators; break;
      case 3:
        table_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kShortenSeparatorsAndSuccessor; break;
      default:
        std::cerr << "error: TableOptions::index_shortening" << std::endl;
  }
  table_op->block_size_deviation = _env->block_size_deviation;
  table_op->enable_index_compression = _env->enable_index_compression;
  // Set all table options
  op->table_factory.reset(NewBlockBasedTableFactory(*table_op));

  //WriteOptions
  write_op->sync = _env->sync; // make every write wait for sync with log (so we see real perf impact of insert)
  write_op->low_pri = _env->low_pri; // every insert is less important than compaction
  write_op->disableWAL = _env->disableWAL; 
  write_op->no_slowdown = _env->no_slowdown; // enabling this will make some insertions fail
  write_op->ignore_missing_column_families = _env->ignore_missing_column_families;
  
  //ReadOptions
  read_op->verify_checksums = _env->verify_checksums;
  read_op->fill_cache = _env->fill_cache;
  read_op->iter_start_seqnum = _env->iter_start_seqnum;
  read_op->ignore_range_deletions = _env->ignore_range_deletions;
  switch (_env->read_tier) {
    case 1:
      read_op->read_tier = kReadAllTier; break;
    case 2:
      read_op->read_tier = kBlockCacheTier; break;
    case 3:
      read_op->read_tier = kPersistedTier; break;
    case 4:
      read_op->read_tier = kMemtableTier; break;
    default:
      std::cerr << "error: ReadOptions::read_tier" << std::endl;
  }

  //FlushOptions
  flush_op->wait = _env->wait;
  flush_op->allow_write_stall = _env->allow_write_stall;


  // op->max_write_buffer_number_to_maintain = 0;    // immediately freed after flushed
  // op->db_write_buffer_size = 0;   // disable
  // op->arena_block_size = 0;
  // op->memtable_huge_page_size = 0;
    
  // Compaction options
    // op->min_write_buffer_number_to_merge = 1;
    // op->compaction_readahead_size = 0;
    // op->max_bytes_for_level_multiplier_additional = std::vector<int>(op->num_levels, 1);
    // op->max_subcompactions = 1;   // no subcomapctions
    // op->avoid_flush_during_recovery = false;
    // op->atomic_flush = false;
    // op->new_table_reader_for_compaction_inputs = false;   // forced to true when using direct_IO_read
    // compaction_options_fifo;
  
  // Compression options
    // // L0 - L6: noCompression
    // op->compression_per_level = {CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression,
    //                                   CompressionType::kNoCompression};

  // op->sample_for_compression = 0;    // disabled
  // op->bottommost_compression = kDisableCompressionOption;

// Log Options
  // op->max_total_wal_size = 0;
  // op->db_log_dir = "";
  // op->max_log_file_size = 0;
  // op->wal_bytes_per_sync = 0;
  // op->strict_bytes_per_sync = false;
  // op->manual_wal_flush = false;
  // op->WAL_ttl_seconds = 0;
  // op->WAL_size_limit_MB = 0;
  // op->keep_log_file_num = 1000;
  // op->log_file_time_to_roll = 0;
  // op->recycle_log_file_num = 0;
  // op->info_log_level = nullptr;
  
// Other CFOptions
  // op->prefix_extractor = nullptr;
  // op->bloom_locality = 0;
  // op->memtable_whole_key_filtering = false;
  // op->snap_refresh_nanos = 100 * 1000 * 1000;  
  // op->memtable_insert_with_hint_prefix_extractor = nullptr;
  // op->force_consistency_checks = false;

// Other DBOptions
  // op->stats_dump_period_sec = 600;   // 10min
  // op->persist_stats_to_disk = false;
  // op->enable_pipelined_write = false;
  // op->table_cache_numshardbits = 6;
  // op->fail_iflush_options_file_error = false;
  // op->writable_file_max_buffer_size = 1024 * 1024;
  // op->write_thread_slow_yield_usec = 100;
  // op->enable_write_thread_adaptive_yield = true;
  // op->unordered_write = false;
  // op->preserve_deletes = false;
  // op->paranoid_checks = true;
  // op->two_write_queues = false;
  // op->use_fsync = true;
  // op->random_access_max_buffer_size = 1024 * 1024;
  // op->skip_stats_update_on_db_open = false;
  // op->error_if_exists = false;
  // op->manifest_preallocation_size = 4 * 1024 * 1024;
  // op->max_manifest_file_size = 1024 * 1024 * 1024;
  // op->is_fd_close_on_exec = true;
  // op->use_adaptive_mutex = false;
  // op->create_missing_column_families = false;
  // op->allow_ingest_behind = false;
  // op->avoid_unnecessary_blocking_io = false;
  // op->allow_fallocate = true;
  // op->allow_2pc = false;
  // op->write_thread_max_yield_usec = 100;*/

}

// Run rocksdb experiments for experiment_runs
// 1.Initiate experiments environment and rocksDB options
// 2.Preload workload into memory
// 3.Run workload and collect stas for each run
int runExperiments(EmuEnv* _env) {
  DB* db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;
  WorkloadDescriptor wd(workloadPath);
  // init RocksDB configurations and experiment settings
  configOptions(_env, &options, &table_options, &write_options, &read_options, &flush_options);
  // parsing workload
  loadWorkload(&wd);
  
  // Starting experiments
  assert(_env->experiment_runs >= 1);
  for (int i = 0; i < _env->experiment_runs; ++i) {
    // Reopen DB
    if (_env->destroy) {
      //DestroyDB(kDBPath, options);
      DestroyDB(_env->path, options);
    }
    //Status s = DB::Open(options, kDBPath, &db);
    Status s = DB::Open(options, _env->path, &db);
    if (!s.ok()) std::cerr << s.ToString() << std::endl;
    assert(s.ok());
    
    // Prepare Perf and I/O stats
    QueryTracker *query_track = new QueryTracker();   // stats tracker for each run
    SetPerfLevel(kEnableTimeExceptForMutex);
    get_perf_context()->Reset();
    get_iostats_context()->Reset();
    // Run workload
    runWorkload(db, _env, &options, &table_options, &write_options, &read_options, &flush_options, &wd, query_track);

    // Collect stats after per run
    SetPerfLevel(kDisable);
    query_track->workload_exec_time = query_track->inserts_cost + query_track->updates_cost + query_track->point_deletes_cost 
                                    + query_track->range_deletes_cost + query_track->point_lookups_cost + query_track->zero_point_lookups_cost
                                    + query_track->range_lookups_cost;
    query_track->get_from_memtable_count = get_perf_context()->get_from_memtable_count;
    query_track->get_from_memtable_time = get_perf_context()->get_from_memtable_time;
    query_track->get_from_output_files_time = get_perf_context()->get_from_output_files_time;
    query_track->filter_block_read_count = get_perf_context()->filter_block_read_count;
    query_track->bloom_memtable_hit_count = get_perf_context()->bloom_memtable_hit_count;
    query_track->bloom_memtable_miss_count = get_perf_context()->bloom_memtable_miss_count;
    query_track->bloom_sst_hit_count = get_perf_context()->bloom_sst_hit_count;
    query_track->bloom_sst_miss_count = get_perf_context()->bloom_sst_miss_count;
    query_track->bytes_read = get_iostats_context()->bytes_read;
    query_track->read_nanos = get_iostats_context()->read_nanos;
    query_track->cpu_read_nanos = get_iostats_context()->cpu_read_nanos;
    query_track->bytes_written = get_iostats_context()->bytes_written;
    query_track->write_nanos = get_iostats_context()->write_nanos;
    query_track->cpu_write_nanos = get_iostats_context()->cpu_write_nanos;

    // Space amp
    uint64_t live_sst_size = 0;
    db->GetIntProperty("rocksdb.live-sst-files-size", &live_sst_size);
    uint64_t calculate_size = 1024 * (query_track->inserts_completed - query_track->point_deletes_completed);
    query_track->space_amp = static_cast<double>(live_sst_size) / calculate_size;

    std::map<std::string, std::string> cfstats;
    db->GetMapProperty("rocksdb.cfstats", &cfstats);
//    for (std::map<std::string, std::string>::iterator it=cfstats.begin(); it !=cfstats.end(); ++it)
//    std::cout << it->first << " => " << it->second << '\n';

    // Write amp
    query_track->write_amp = std::stod(cfstats.find("compaction.Sum.WriteAmp")->second);

    // TODO:: wrong read amp
    query_track->read_amp = std::stod(cfstats.find("compaction.Sum.Rnp1GB")->second)
        / std::stod(cfstats.find("compaction.Sum.RnGB")->second);

    // stalls triggered by compactions
    query_track->stalls = std::stod(cfstats.find("io_stalls.total_stop")->second);

    if (_env->verbosity > 0) {
      printEmulationOutput(_env, query_track);
    } else if (_env->verbosity > 1) {
      std::string state;
      db->GetProperty("rocksdb.cfstats-no-file-histogram", &state);
      std::cout << state << std::endl;
    }

    dumpStats(&query_stats, query_track);    // dump stat of each run into acmulative stat
    CloseDB(db, flush_options);
    std::cout << "End of experiment run: " << i+1 << std::endl;
    std::cout << std::endl;
  }
  return 0;
}

// Run a workload from memory
// The workload is stored in WorkloadDescriptor
// Use QueryTracker to record performance for each query operation
int runWorkload(DB *&db, const EmuEnv* _env, const Options *op, const BlockBasedTableOptions *table_op, 
                const WriteOptions *write_op, const ReadOptions *read_op, const FlushOptions *flush_op,
                const WorkloadDescriptor *wd, QueryTracker *query_track) {
  Status s;
  Iterator *it = db-> NewIterator(*read_op); // for range reads
  uint64_t counter = 0, mini_counter = 0; // tracker for progress bar. TODO: avoid using these two 
  my_clock start_clock, end_clock;    // clock to get query time
  // Clear System page cache before running
  if (_env->clear_sys_page_cache) { 
    std::cout << "\nClearing system page cache before experiment ..."; 
    fflush(stdout);
    clearPageCache();
    get_perf_context()->Reset();
    get_iostats_context()->Reset();
    std::cout << " OK!" << std::endl;
  }
  
  for (const auto &qd : wd->queries) {
    // Reopen DB and clear cache before bulk reading
    // if (counter == wd->insert_num) {
    //   std::cout << "\nRe-opening DB and clearing system page cache ...";
    //   fflush(stdout);
    //   ReopenDB(db, *op, *flush_op);
    //   clearPageCache();
    //   get_perf_context()->Reset();
    //   get_iostats_context()->Reset();
    //   std::cout << " OK!" << std::endl;
    // }
    uint32_t key, start_key, end_key;
    std::string value;
    Entry *entry_ptr = nullptr;
    RangeEntry *rentry_ptr = nullptr;

    switch (qd.type) {
      case INSERT:
        ++counter;
        assert(counter = qd.seq);
        if (query_track->inserts_completed == wd->actual_insert_num) break;
        entry_ptr = dynamic_cast<Entry*>(qd.entry_ptr);
        key = entry_ptr->key;
        value = entry_ptr->value;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << " " << value << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = db->Put(*write_op, ToString(key), value);
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        assert(s.ok());
        query_track->inserts_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->total_completed;
        ++query_track->inserts_completed;
        break;

      case UPDATE:
        ++counter;
        assert(counter = qd.seq);
        entry_ptr = dynamic_cast<Entry*>(qd.entry_ptr);
        key = entry_ptr->key;
        value = entry_ptr->value;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << " " << value << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = db->Put(*write_op, ToString(key), value);
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->updates_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->updates_completed;
        ++query_track->total_completed;
        break;

      case DELETE:
        ++counter;
        assert(counter = qd.seq);
        key = qd.entry_ptr->key;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = db->Delete(*write_op, ToString(key));
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->point_deletes_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->point_deletes_completed;
        ++query_track->total_completed;
        break;

      case RANGE_DELETE:
        ++counter;
        assert(counter = qd.seq);
        rentry_ptr = dynamic_cast<RangeEntry*>(qd.entry_ptr);
        start_key = rentry_ptr->key;
        end_key = start_key + rentry_ptr->range;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << start_key << " " << end_key << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = db->DeleteRange(*write_op, db->DefaultColumnFamily(), ToString(start_key), ToString(end_key));
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->range_deletes_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->range_deletes_completed;
        ++query_track->total_completed;
        break;

      case LOOKUP:
        ++counter;
        assert(counter = qd.seq);
        // for pseudo zero-reuslt point lookup
        // if (query_track->point_lookups_completed + query_track->zero_point_lookups_completed >= 10) break;
        key = qd.entry_ptr->key;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        s = db->Get(*read_op, ToString(key), &value);
        // assert(s.ok());
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        if (!s.ok()) {    // zero_reuslt_point_lookup
          if (_env->verbosity == 2) std::cerr << s.ToString() << "key = " << key << std::endl;
          query_track->zero_point_lookups_cost += getclock_diff_ns(start_clock, end_clock);
          ++query_track->zero_point_lookups_completed;
          ++query_track->total_completed;
          break;
        } 
        query_track->point_lookups_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->point_lookups_completed;
        ++query_track->total_completed;
        break;

      case RANGE_LOOKUP:
        ++counter; 
        assert(counter = qd.seq);
        rentry_ptr = dynamic_cast<RangeEntry*>(qd.entry_ptr);
        start_key = rentry_ptr->key;
        end_key = start_key + rentry_ptr->range;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << start_key << " " << end_key << "" << std::endl;
        it->Refresh();    // to update a stale iterator view
        assert(it->status().ok());
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        for (it->Seek(ToString(start_key)); it->Valid(); it->Next()) {
          // std::cout << "found key = " << it->key().ToString() << std::endl;
          if(it->key() == ToString(end_key)) {    // TODO: check correntness
            break;
          }
        }
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        if (!it->status().ok()) {
          std::cerr << it->status().ToString() << std::endl;
        }
        query_track->range_lookups_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->range_lookups_completed;
        ++query_track->total_completed;
        break;

      default:
          std::cerr << "Unknown query type: " << static_cast<char>(qd.type) << std::endl;
    }
    showProgress(wd->total_num, counter, mini_counter);
  }
  // flush and wait for the steady state
  db->Flush(*flush_op);
  FlushMemTableMayAllComplete(db);
  CompactionMayAllComplete(db);
  return 0;
}

//
//void query(ExpEnv* _env, vector<string>& existing_keys ) {
  
  // Options options;
  // options.create_if_missing = false;
  // options.use_direct_reads = true;
  // //options.IncreaseParallelism(6);
  // options.num_levels = _env->rocksDB_max_levels;

  // rocksdb::BlockBasedTableOptions table_options;
  // CrimsonDBFiltersPolicy* filters_policy = get_filters_policy(_env);

  // filter_stats* fs = new filter_stats();
  // filters_policy->set_stats_collection(fs);
  
  // table_options.filter_policy.reset(filters_policy);
  // table_options.no_block_cache = true;
  // //table_options.cache_index_and_filter_blocks = true;
  // //table_options.cache_index_and_filter_blocks_with_high_priority = true;
  // //options.optimize_filters_for_hits = false;
  // options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  

  // //_env->file_size is defaulting to : std::numeric_limits<uint64_t>::max()
  // FluidLSMTree* tree = new FluidLSMTree(_env->T, _env->K, _env->Z, _env->file_size, options);

  // /*filters_policy->show_hypothetical_FPRs(7, 3, 2.0, 1.0, 1.0);
  // std::cerr << endl;
  // filters_policy->show_hypothetical_FPRs(7, 5, 2.0, 1.0, 1.0);
  // std::cerr << endl;
  // filters_policy->show_hypothetical_FPRs(7, 7, 2.0, 1.0, 1.0);*/

  // stats_collector* rsc = new stats_collector(tree);
  // options.listeners.emplace_back(tree);
  // options.listeners.emplace_back(rsc);
  // options.num_levels = _env->rocksDB_max_levels;
  // DB* db = nullptr;
  // //system("sudo hdparm -W 1 /dev/sda3");
  // //system("sudo hdparm -A 1 /dev/sda3");
  // Status s = DB::OpenForReadOnly(options, _env->path, &db);

  // if (!s.ok()) {
  //   cerr << "Problem opening DB. Closing " << s.ToString() << ".\n";
  //   delete db;
  //   delete fs;
  //   exit(0);
  // }

  // filters_policy->set_db(db, tree);
  // tree->setFiltersPolicy(filters_policy);
  // tree->buildStructure(db);
  // filters_policy->init_filters(tree->largestOccupiedLevel());

  // if (_env->debugging) {
  //   tree->printFluidLSM(db);
  // }

  // if (_env->verbosity >= 1)
  //   std::cerr << "Issuing " << _env->num_queries << " queries ..." << std::endl;


  // my_clock clk_start, clk_end;

  // my_clock_get_time(&clk_start);
  
  // // verify the values are still there
  // std::string key, prefixed_key, value;
  
  // //Start measuring the IOs
  // _env->measure_IOs = true;

  // for (int i = 0; i < _env->num_queries; ++i) 
  // {
  //   Status s;
  //   if (i%100==0)  
  //     std::cerr << ".";
  //   double random = (double)rand() / RAND_MAX;
  //   if (existing_keys.size() > 0 && random < _env->nonzero_to_zero_ratio / (_env->nonzero_to_zero_ratio + 1)) {
  //     key = existing_keys.back();
  //     existing_keys.pop_back();
  //     s = db->Get(ReadOptions(), key, &value);
  //     assert(s.ok());
  //   }
  //   else {
  //     key = DataGenerator::generate_key();
  //     prefixed_key = _env->key_prefix_for_entries_to_target_in_queries + key;
  //     s = db->Get(ReadOptions(), key, &value);
      
  //     //this below is issuing a read to lower (bigger levels) for the non-zero queries (if the above query did not find anything in the top levels)
  //     //because of the prefix it should not issue any IOs for zero result queries
  //     if (!s.ok()) {
  //       s = db->Get(ReadOptions(), prefixed_key, &value);
  //     }
  //   }

  //   if (s.ok() && fs) {
  //     fs->true_positives += s.ok();
  //   }
  //   //assert(value == std::string(500, 'a' + (i % 26)));
  // }

  // std::cerr << std::endl << std::flush;

  // if (_env->verbosity >= 1 && _env->measure_IOs)
  //   std::cerr << "\n TOTAL I/Os " << _env->total_IOs << std::endl;

  // //Stop measuring the IOs
  // _env->measure_IOs=false;


  // my_clock_get_time(&clk_end);

  // if (_env->verbosity >= 1)
  //   std::cerr << "Queries finished." << std::endl;


  // //assert(existing_keys.size() < 10);

  // //return the experiment duration in seconds.
  // double experiment_time = getclock_diff_s(clk_start,clk_end); 

  // if (_env->verbosity >= 1) {
  //   std::cerr << "query time: " << experiment_time << " s" << std::endl;
  //   fs->print();
  //   std::cerr << std::endl;
  // }

  // //prints experiment output for queries
  // bool is_read_experiment=true;
  // _env->experiment_starting_time = get_time_string();
  // print_experiment_output(_env, fs, experiment_time,is_read_experiment, tree, rsc);

  // db->Close();
  // delete db;
  // delete fs;
//}
//

/*
long deletes(ExpEnv* _env) {
  if (_env->debugging)
    cerr << "Deleting from existing DB ...\n";

  Options options;
  options.create_if_missing = false;
  options.compaction_style = kCompactionStyleNone;
  options.write_buffer_size =  _env->buffer_size;

  options.compression = kNoCompression; // eventually remove this
  options.use_direct_reads = true;
  options.num_levels = _env->rocksDB_max_levels;
  options.allow_mmap_writes = false;
  options.new_table_reader_for_compaction_inputs = true; //try turning it on and off
  options.compaction_readahead_size = _env->compaction_readahead_size_KB*1024; //min(2MB,file_size)
  options.writable_file_max_buffer_size = 2 * options.compaction_readahead_size;
  options.allow_mmap_reads=false;
  options.allow_mmap_writes=false;
  options.use_direct_io_for_flush_and_compaction=true;
  
  // options.use_fsync=true;

  options.IncreaseParallelism(1); //threads using for compaction

  

  rocksdb::BlockBasedTableOptions table_options;

  CrimsonDBFiltersPolicy* filters_policy = get_filters_policy(_env);

  filter_stats* fs = new filter_stats();
  filters_policy->set_stats_collection(fs);
  table_options.filter_policy.reset(filters_policy);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  //_env->file_size is defaulting to std::numeric_limits<uint64_t>::max()
  FluidLSMTree* tree = new FluidLSMTree(_env->T, _env->K, _env->Z, _env->file_size, options);


  tree->set_num_parallel_compactions_allowed(1);
  tree->set_debug_mode(_env->debugging);
  stats_collector* rsc = new stats_collector(tree);
  options.listeners.emplace_back(tree);
  options.listeners.emplace_back(rsc);
  options.num_levels = _env->rocksDB_max_levels;
  
  DB* db = nullptr;
  Status s = DB::Open(options, _env->path, &db);
  //Status s1 = s;
  tree->set_db(db);

  if (!s.ok()) {
    cerr << s.ToString() << endl;
    cerr << "Problem opening DB. Closing.\n";
    delete db;
    return -1;
  }

  filters_policy->set_db(db, tree);
  tree->setFiltersPolicy(filters_policy);

  tree->buildStructure(db);
  if (_env->verbosity>=1)
    std::cerr << "Tree has 0..." << tree->largestOccupiedLevel() << " levels" << std::endl;
  filters_policy->init_filters(tree->largestOccupiedLevel());

  // if (_env->num_levels==-1)
  // {
  //   //this means that we insert using N so we should derive the number of levels
  //   //calculate derived levels with taking into account the new inserts
  //   //_env->derived_num_levels = ceil( log( ( (_env->N+_env->num_inserts) * _env->entry_size * (_env->T-1)) / ( _env->buffer_size * _env->T )  ) / log(_env->T) ) ;
  //   //_env->derived_num_levels = tree->largestOccupiedLevel() + 1;
  //   if (_env->verbosity >= 1)
  //     cerr << "derived levels are : " << _env->derived_num_levels << endl;
  //   // filters_policy->init_filters(_env->derived_num_levels);
  // }
  // else
  //   filters_policy->init_filters(_env->num_levels);

  //calling buildStructure(db) to convert RocksDB to FluidLSM internal structure 
  // tree->buildStructure(db);

  if (_env->verbosity >= 1) {
    filters_policy->print();
  }

  assert(s.ok());
  assert(db);

  WriteOptions write_options;
  write_options.sync = false; // make every write wait for sync with log (so we see real perf impact of insert)
  write_options.low_pri = true; // every insert is less important than compaction
  write_options.disableWAL = false; 
  write_options.no_slowdown = false; // enabling this will make some insertions fail
  
  long total_deleted=_env->num_deletes;
  long num_failed=0;

  my_clock clk_start;
  my_clock clk_end;

  my_clock_get_time(&clk_start);

  if (_env->verbosity >= 1)
    std::cerr << "Issuing " << total_deleted << " deletes " << std::endl;


  long buffer_size_elements=_env->buffer_size/_env->entry_size;
  if (_env->verbosity >= 1)
    std::cerr << "Buffer fits " << buffer_size_elements << " elements " << std::endl;

  //Start measuring the IOs
  _env->measure_IOs = true;

  std::string value = "";
  long delete_count = 0;
  long delete_attempt = 0;
  size_t progress_meter = 1;

  std::ofstream result_data; //DELETE EVENTUALLY
  result_data.open ("./deleted.csv"); //DELETE EVENTUALLY
  for (long i = 0; i < 9223372036854775807; i++) {
    if (delete_count == _env->num_deletes) break;

    pair<string, string> entry = DataGenerator::generate_key_val_pair(_env->entry_size);
    ++delete_attempt;
    
    s = db->Get(ReadOptions(), entry.first, &value);
    if (value != ""){
      ++delete_count;
      result_data << entry.first << endl; //DELETE EVENTUALLY
      s = db->Delete(write_options, entry.first); 
      //cout << "Found and deleted " << entry.first << endl;
      value = "";
      
      if ((delete_count*100)/_env->num_deletes == progress_meter) {
        std::cout << "." << std::flush;
        ++progress_meter;
      }
      
    }
    num_failed += !s.ok();
    if (!s.ok()) 
      //std::cerr << s.ToString() << std::endl; // error message for non-existent delete requests

    //wait for all pending mergings every 
    if (i%(buffer_size_elements)==0)
    {
      if (_env->show_progress)
        std::cerr << "." << std::flush;
      if (_env->verbosity == 2)
        std::cerr << "Finish merging before moving on ... (" << i << "/" << _env->num_deletes << ")" << std::endl;
      tree->finish_all_merge_operations(db);
    }
  }
  result_data.close(); //DELETE EVENTUALLY
  
  if (_env->show_progress)
    std::cerr << std::endl << std::flush;

  if (_env->verbosity >= 1 && _env->measure_IOs)
    std::cerr << "\nTOTAL I/Os " << _env->total_IOs << std::endl;

  //Stop measuring the IOs
  _env->measure_IOs = false;


  if (_env->verbosity >= 1)
    std::cerr << "\nDeletes completed!" << std::endl;

  db->SyncWAL();
  //I am including the WAL as part of the timing but not the db->close.
  my_clock_get_time(&clk_end);

  FlushOptions ops;
  ops.wait = true;
  db->Flush(ops);
  tree->set_num_parallel_compactions_allowed(0);

  tree->finish_all_merge_operations(db);

  double experiment_time = getclock_diff_s(clk_start,clk_end);
    
  if (_env->verbosity >= 1)
    std::cerr << "Deleted " << delete_count << "/" << _env->num_deletes << " (" << delete_attempt << ")" << " in " << experiment_time << " seconds" << std::endl;

  if (_env->verbosity >= 1)
  {
    tree->buildStructure(db);
    //tree->printFluidLSM(db);
  }

  tree->buildStructure(db);
  if (_env->verbosity>=1)
    std::cerr << "Tree has 0..." << tree->largestOccupiedLevel() << " levels" << std::endl;
  _env->derived_num_levels=tree->largestOccupiedLevel();


  //prints experiment output for deletes
  bool is_read_experiment=false;
  _env->experiment_starting_time = get_time_string();
  if (_env->N!=-1)
    _env->N-=_env->num_deletes;
  else if (_env->derived_N!=-1)
    _env->derived_N-=_env->num_deletes;
  print_experiment_output(_env, fs, experiment_time, is_read_experiment, tree, rsc);


  if (_env->verbosity >= 1)
    std::cerr << "Closing DB ... " << std::endl;

  total_deleted = delete_attempt - num_failed;

  if (_env->verbosity >= 1) {
    tree->printFluidLSM(db);
  }

  

  db->Close();
  delete db;
  delete fs;

  if (_env->verbosity >= 1)
    std::cerr << "DB closed ... " << std::endl;
  return total_deleted;
}
*/

/*
long updates(ExpEnv* _env) {
  if (_env->debugging)
    cerr << "Updating (inserting duplicates) in existing DB ...\n";

  Options options;
  options.create_if_missing = false;
  options.compaction_style = kCompactionStyleNone;
  options.write_buffer_size =  _env->buffer_size;

  options.compression = kNoCompression; // eventually remove this
  options.use_direct_reads = true;
  options.num_levels = _env->rocksDB_max_levels;
  options.allow_mmap_writes = false;
  options.new_table_reader_for_compaction_inputs = true; //try turning it on and off
  options.compaction_readahead_size = _env->compaction_readahead_size_KB*1024; //min(2MB,file_size)
  options.writable_file_max_buffer_size = 2 * options.compaction_readahead_size;
  options.allow_mmap_reads = false;
  options.allow_mmap_writes = false;
  options.use_direct_io_for_flush_and_compaction = true;
  // options.use_fsync = true;

  options.IncreaseParallelism(1); //threads using for compaction

  rocksdb::BlockBasedTableOptions table_options;

  CrimsonDBFiltersPolicy* filters_policy = get_filters_policy(_env);

  filter_stats* fs = new filter_stats();
  filters_policy->set_stats_collection(fs);
  table_options.filter_policy.reset(filters_policy);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  //_env->file_size is defaulting to std::numeric_limits<uint64_t>::max()
  FluidLSMTree* tree = new FluidLSMTree(_env->T, _env->K, _env->Z, _env->file_size, options);

  tree->set_num_parallel_compactions_allowed(1);
  tree->set_debug_mode(_env->debugging);
  stats_collector* rsc = new stats_collector(tree);
  options.listeners.emplace_back(tree);
  options.listeners.emplace_back(rsc);
  options.num_levels = _env->rocksDB_max_levels;
  
  DB* db = nullptr;
  Status s = DB::Open(options, _env->path, &db);
  tree->set_db(db);

  if (!s.ok()) {
    cerr << s.ToString() << endl;
    cerr << "Problem opening DB. Closing.\n";
    delete db;
    return -1;
  }

  filters_policy->set_db(db, tree);
  tree->setFiltersPolicy(filters_policy);

  tree->buildStructure(db);
  if (_env->verbosity>=1)
    std::cerr << "Tree has 0..." << tree->largestOccupiedLevel() << " levels" << std::endl;
  filters_policy->init_filters(tree->largestOccupiedLevel());

  // if (_env->num_levels==-1)
  // {
  //   //this means that we insert using N so we should derive the number of levels
  //   //calculate derived levels with taking into account the new inserts
  //   //_env->derived_num_levels = ceil( log( ( (_env->N+_env->num_inserts) * _env->entry_size * (_env->T-1)) / ( _env->buffer_size * _env->T )  ) / log(_env->T) ) ;
  //   //_env->derived_num_levels = tree->largestOccupiedLevel() + 1;
  //   if (_env->verbosity >= 1)
  //     cerr << "derived levels are : " << _env->derived_num_levels << endl;
  //   // filters_policy->init_filters(_env->derived_num_levels);
  // }
  // else
  //   filters_policy->init_filters(_env->num_levels);

  //calling buildStructure(db) to convert RocksDB to FluidLSM internal structure 
  // tree->buildStructure(db);

  if (_env->verbosity >= 1) {
    filters_policy->print();
  }

  assert(s.ok());
  assert(db);

  WriteOptions write_options;
  write_options.sync = false; //make every write wait for sync with log (so we see real perf impact of insert)
  write_options.low_pri = true; // every insert is less important than compaction
  write_options.disableWAL = false; 
  write_options.no_slowdown = false; // enabling this will make some insertions fail
  
  long total_updated = _env->num_updates;
  long num_failed = 0;

  my_clock clk_start;
  my_clock clk_end;

  my_clock_get_time(&clk_start);

  if (_env->verbosity >= 1)
    std::cerr << "Issuing " << total_updated << " updates " << std::endl;


  long buffer_size_elements=_env->buffer_size/_env->entry_size;
  if (_env->verbosity >= 1)
    std::cerr << "Buffer fits " << buffer_size_elements << " elements " << std::endl;

  //Start measuring the IOs
  _env->measure_IOs = true;

  std::string value = "";
  long update_count = 0;
  long update_attempt = 0;
  size_t progress_meter = 1;

  std::ofstream result_data; //DELETE EVENTUALLY
  result_data.open ("./updated.csv"); //DELETE EVENTUALLY
  for (long i = 0; i < 9223372036854775807; i++) {
    if (update_count == _env->num_updates) break;
     
    pair<string, string> entry = DataGenerator::generate_key_val_pair(_env->entry_size);
    ++update_attempt;

    s = db->Get(ReadOptions(), entry.first, &value);
    if (value != ""){
      ++update_count;
      result_data << entry.first << endl; //DELETE EVENTUALLY
      s = db->Put(write_options, entry.first, entry.second);
      //cout << "Updating key " << entry.first << endl;
      //cout << "update_count " << update_count << "\t"; cout << "_env->num_updates " << _env->num_updates << endl;
      value = "";

      if ((update_count*100)/_env->num_updates == progress_meter) {
        std::cout << "." << std::flush;
        ++progress_meter;
      }
    }
    
    num_failed += !s.ok();
    if (!s.ok()) 
      //std::cerr << s.ToString() << std::endl; // error message for update requests on non-existent keys

    //wait for all pending mergings every 
    if (i%(buffer_size_elements)==0)
    {
      if (_env->show_progress)
        std::cerr << "." << std::flush;
      if (_env->verbosity == 2)
        std::cerr << "Finish merging before moving on ... (" << i << "/" << _env->num_updates << ")" << std::endl;
      tree->finish_all_merge_operations(db);
    }
  }
  result_data.close(); //DELETE EVENTUALLY
  
  if (_env->show_progress)
    std::cerr << std::endl << std::flush;

  if (_env->verbosity >= 1 && _env->measure_IOs)
    std::cerr << "\n TOTAL I/Os " << _env->total_IOs << std::endl;

  //Stop measuring the IOs
  _env->measure_IOs = false;


  if (_env->verbosity >= 1)
    std::cerr << "\n Updates completed!" << std::endl;

  db->SyncWAL();
  //I am including the WAL as part of the timing but not the db->close.
  my_clock_get_time(&clk_end);

  FlushOptions ops;
  ops.wait = true;
  db->Flush(ops);
  tree->set_num_parallel_compactions_allowed(0);

  tree->finish_all_merge_operations(db);

  double experiment_time = getclock_diff_s(clk_start,clk_end);
    
  if (_env->verbosity >= 1)
    std::cerr << "Updated " << update_count << "/" << _env->num_updates << " (" << update_attempt << ")" << " in " << experiment_time << " seconds" << std::endl;

  if (_env->verbosity >= 1)
  {
    tree->buildStructure(db);
    //tree->printFluidLSM(db);
  }

  tree->buildStructure(db);
  if (_env->verbosity>=1)
    std::cerr << "Tree has 0..." << tree->largestOccupiedLevel() << " levels" << std::endl;
  _env->derived_num_levels=tree->largestOccupiedLevel();


  //prints experiment output for updates
  bool is_read_experiment=false;
  _env->experiment_starting_time = get_time_string();
  if (_env->N!=-1)
    _env->N+=_env->num_updates;
  else if (_env->derived_N!=-1)
    _env->derived_N+=_env->num_updates;
  print_experiment_output(_env,fs,experiment_time,is_read_experiment, tree, rsc);

  if (_env->verbosity >= 1)
    std::cerr << "Closing DB ... " << std::endl;

  total_updated = update_attempt - num_failed;

  if (_env->verbosity >= 1) {
    tree->printFluidLSM(db);
  }

  db->Close();
  delete db;
  delete fs;

  if (_env->verbosity >= 1)
    std::cerr << "DB closed ... " << std::endl;
  return total_updated;
}
*/

//
//Status bulk_load(ExpEnv* _env, long &total_bulk_loaded_entries) {
  // Options options;
  // options.create_if_missing = true;
  // /*options.compaction_style = kCompactionStyleLevel;
  // options.max_bytes_for_level_multiplier = T;
  // options.max_write_buffer_number = 2;
  // options.min_write_buffer_number_to_merge = 0;
  // options.max_write_buffer_number_to_maintain = 0;
  // options.level0_file_num_compaction_trigger = T;
  // options.write_buffer_size = 1024 * 1024;*/

  // options.compaction_style = kCompactionStyleNone;
  // options.write_buffer_size =  _env->buffer_size * 2;
  
  
  // options.compression = kNoCompression; // eventually remove this
  // options.use_direct_reads = false;
  // options.num_levels = _env->rocksDB_max_levels;
  // options.allow_mmap_writes = true;
  // options.new_table_reader_for_compaction_inputs=true;
  // options.compaction_readahead_size = 1024 * 1024 * 4;
  // //options.allow_mmap_reads = true;
  // //options.is_fd_close_on_exec = false;

  // options.IncreaseParallelism(4);

  // rocksdb::BlockBasedTableOptions table_options;

  // CrimsonDBFiltersPolicy* filters_policy = get_filters_policy(_env);


  // filter_stats* fs = new filter_stats();
  // filters_policy->set_stats_collection(fs);
  // table_options.filter_policy.reset(filters_policy);
  // options.table_factory.reset(NewBlockBasedTableFactory(table_options));


  // //_env->file_size is defaulting to : std::numeric_limits<uint64_t>::max()
  // FluidLSMTreeBulkLoader* loader = new FluidLSMTreeBulkLoader(_env->T, _env->K, _env->Z, _env->file_size, options);
  // loader->set_batch_size(1);
  // loader->set_num_parallel_compactions_allowed(1);
  // loader->set_debug_mode(_env->debugging);
  // stats_collector* rsc = new stats_collector(loader);
  // options.listeners.emplace_back(loader);
  // options.listeners.emplace_back(rsc);

  // options.PrepareForBulkLoad();

  // my_clock clk_start, clk_end; 
  // my_clock_get_time(&clk_start);

  // options.num_levels = _env->rocksDB_max_levels;
  // DB* db = nullptr;
  // Status s = DB::Open(options, _env->path, &db);
  // loader->set_db(db);

  // if (!s.ok()) {
  //   cerr << s.ToString() << endl;
  //   cerr << "Problem opening DB. Closing.\n";
  //   delete db;
  //   return s;
  // }

  // filters_policy->set_db(db, loader);
  // loader->setFiltersPolicy(filters_policy);

  // if (_env->num_levels==-1)
  // {
  //   //this means that we insert using N so we should derive the number of levels
    
  //   _env->derived_num_levels = ceil( log( (_env->N * _env->entry_size * (_env->T-1)) / ( _env->buffer_size * _env->T )  ) / log(_env->T) ) ;
  //   if (_env->derived_num_levels < 0)
  //     _env->derived_num_levels=0;
  //   if (_env->verbosity >= 1 && _env->show_progress)
  //     cerr << "derived levels are : " << _env->derived_num_levels << endl;
  //   filters_policy->init_filters(_env->derived_num_levels);
  // }
  // else
  //   filters_policy->init_filters(_env->num_levels);

  // loader->buildStructure(db);
  // if (loader->largestOccupiedLevel() > 0) {
  //   //infer the number of elements, this is really just an estimation at this point, because it assumes all levels are full
  //   _env->derived_N=0;
  //   long cur_level_size=_env->buffer_size/_env->entry_size;
  //   for (int i=0;i<=loader->largestOccupiedLevel();i++)
  //   {
  //     _env->derived_N+=cur_level_size;
  //     cur_level_size*=_env->T;
  //   }
  //   delete db;
  //   delete fs;
  //   // if (_env->verbosity >= 1) 
  //     cerr << "Database already contains some data. Continuing using existing DB. Estimating data size to:  "<< _env->derived_N << endl;
  //   return s.Aborted();
  // }

  // if (_env->debugging) {
  //   filters_policy->print();
  // }

  // assert(s.ok());
  // assert(db);

  // loader->set_key_prefix_for_X_smallest_levels(_env->target_level_for_non_zero_result_point_lookups, _env->key_prefix_for_entries_to_target_in_queries);

  // total_bulk_loaded_entries=0;
  // assert((_env->num_levels==-1 && _env->N!=-1) || (_env->num_levels!=-1 && _env->N==-1));
  // if (_env->num_levels!=-1 && _env->N==-1)
  //   total_bulk_loaded_entries=loader->bulk_load_levels(db, "default", _env);
  // else //if (_env->num_levels==-1 && _env->N!=-1)
  //   total_bulk_loaded_entries=loader->bulk_load_entries(db, "default", _env);

  // //update the N value
  // if (_env->derived_N==-1)
  //   _env->N=total_bulk_loaded_entries;
  // else
  //   _env->derived_N=total_bulk_loaded_entries;


  // if (_env->verbosity >= 1) {
  //   std::cerr << "Printing LSM after bulk load!" << std::endl;
  //   loader->printFluidLSM(db);
  //   std::cerr << "End of LSM" << std::endl;
  // }
  // db->SyncWAL();

  // db->Close();

  // my_clock_get_time(&clk_start);
  // double load_time = getclock_diff_s(clk_start,clk_end);
  
  // _env->derived_N=total_bulk_loaded_entries;
  
  // if (_env->verbosity >= 1)
  //   std::cerr << "Loaded " << total_bulk_loaded_entries << " entries in " << load_time << " time" << std::endl;
  // delete db;
  // delete fs;
  // return s;
//}
//


int parse_arguments2(int argc, char *argv[], EmuEnv* _env) {
  args::ArgumentParser parser("RocksDB_parser.", "");

  args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);
  args::Group group4(parser, "Optional switches and parameters:", args::Group::Validators::DontCare);
/*
  args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::AtMostOne);
  args::Group group2(parser, "Path is needed:", args::Group::Validators::All);
  args::Group group3(parser, "This group is all exclusive (either N or L):", args::Group::Validators::Xor);
  args::Group group4(parser, "Optional switches and parameters:", args::Group::Validators::DontCare);
  args::Group group5(parser, "Optional less frequent switches and parameters:", args::Group::Validators::DontCare);
*/

  args::ValueFlag<int> size_ratio_cmd(group1, "T", "The size ratio of two adjacent levels  [def: 2]", {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(group1, "P", "The number of pages that can fit into a buffer [def: 128]", {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(group1, "B", "The number of entries that fit into a page [def: 128]", {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(group1, "E", "The size of a key-value pair inserted into DB [def: 128 B]", {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(group1, "M", "The size of a buffer that is configured manually [def: 2 MB]", {'M', "memory_size"});
  args::ValueFlag<int> file_to_memtable_size_ratio_cmd(group1, "file_to_memtable_size_ratio", "The size of a file over the size of configured buffer size [def: 1]", {'f', "file_to_memtable_size_ratio"});
  args::ValueFlag<long> file_size_cmd(group1, "file_size", "The size of a file that is configured manually [def: 2 MB]", {'F', "file_size"});
  args::ValueFlag<int> compaction_pri_cmd(group1, "compaction_pri", "[Compaction priority: 1 for kMinOverlappingRatio, 2 for kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for kOldestSmallestSeqFirst; def: 2]", {'C', "compaction_pri"});
  args::ValueFlag<int> bits_per_key_cmd(group1, "bits_per_key", "The number of bits per key assigned to Bloom filter [def: 0]", {'b', "bits_per_key"});
  args::ValueFlag<int> experiment_runs_cmd(group1, "experiment_runs", "The number of experiments repeated each time [def: 1]", {'R', "run"});
//  args::ValueFlag<long> num_inserts_cmd(group1, "inserts", "The number of unique inserts to issue in the experiment [def: 0]", {'i', "inserts"});
  args::Flag clear_sys_page_cache_cmd(group4, "clear_sys_page_cache", "Clear system page cache before experiments", {"cc", "clear_cache"});
  args::Flag destroy_cmd(group4, "destroy_db", "Destroy and recreate the database", {"dd", "destroy_db"});
  args::Flag direct_reads_cmd(group4, "use_direct_reads", "Use direct reads", {"dr", "use_direct_reads"});
  args::ValueFlag<int> verbosity_cmd(group4, "verbosity", "The verbosity level of execution [0,1,2; def: 0]", {'V', "verbosity"});
  args::ValueFlag<std::string> path_cmd(group4, "path", "path for writing the DB and all the metadata files", {'p', "path"});

/*
  args::ValueFlag<long> num_elements_cmd(group3, "N", "The number of elements to be inserted [def: -1]", {'N', "num_elements"});
  args::ValueFlag<int> num_levels_cmd(group3, "L", "The number of levels to fill up with data [def: -1]", {'L', "num_levels"});

  args::ValueFlag<int> FPR_optimization_level_cmd(group4, "FPR_opt_level", "The Bloom filter's false positive rate (FPR) optimisation level. 0 for fixed FPRs. 1 to optimize for zero-result lookups. 2 to optimize based on the ratio of existing to zero result lookups.", {'o', "FPR-optimization-level"});
  args::HelpFlag help(group4, "help", "Display this help menu", {'h', "help"});
  args::Flag destroy_cmd(group4, "destroy", "destroy and recreate the database", {'d', "destroy"});
  args::Flag debug_cmd(group4, "debug", "Print debugging information while running", {"debug"});
  args::ValueFlag<double> bits_per_entry_cmd(group4, "bits_per_entry", "The number of bits per entry across all Bloom filters [def: 5]", {'b', "bits-per-entry"});
  args::ValueFlag<std::string> exp_name_cmd(group4, "experiment_name", "Name of the experiment to later label the result set", {'n', "name"});
  args::ValueFlag<int> num_queries_cmd(group4, "#queries", "The number of queries to issue in the experiment [def: 1000]", {'q', "queries"});
  args::ValueFlag<int> num_runs_K_cmd(group4, "K", "The number of sorted runs in the internal levels (K) [def: 1]", {'K', "num_runs_K"});
  args::ValueFlag<int> num_runs_Z_cmd(group4, "Z", "The number of sorted runs in the largest level (Z) [def: 1]", {'Z', "num_runs_Z"});
  args::ValueFlag<int> entry_size_cmd(group4, "E", "The entry size E (in bytes) [def: 1024]", {'E', "entry_size"});
  args::ValueFlag<int> target_level_cmd(group4, "target_level", "The target level to which we issue the non-zero-result point lookups", {"target_level"});
  args::ValueFlag<double> nonzero_to_zero_ratio_cmd(group4, "v", "The ratio between non-zero to zero result point lookups", {'v', "non_zero_to_zero_ratio"});
  args::ValueFlag<int> verbosity_cmd(group4, "verbosity", "The verbosity level of execution [0,1,2, def:0]", {'V', "verbosity"});
  args::ValueFlag<int> rocks_db_max_num_levels_cmd(group5, "max_rocksdb_levels", "The maximum number of RocksDB levels needed (for allocation) [def: 1000]", {"rocks_db_max_num_levels"});
  args::Flag show_progress_cmd(group5, "show_progress", "Print one . every 1000 inserts/queries", {"show_progress"});
  args::Flag clean_caches_cmd(group5, "clean_caches", "Clean caches before experiments", {"clean_caches"});
  args::ValueFlag<int> comp_read_size_cmd(group5, "comp_read_size", "Compaction readahead size (for flash use 64 for disk 2048) [def: 2048]", {'r',"comp_read_size"});
  args::Flag print_IOs_per_file_cmd(group5, "print_file_IO", "Print IO count per file during destructor", {"print_file_IO"});
  args::ValueFlag<int> num_deletes_cmd(group5, "#deletes", "The number of deletes to issue in the experiment [def: 0]", {'x',"deletes"});
  args::ValueFlag<int> num_updates_cmd(group5, "#updates", "The number of updates (inserts against existing keys) to issue in the experiment [def: 0]", {'u',"updates"});
*/

  try {
      parser.ParseCLI(argc, argv);
  }
  catch (args::Help&) {
      std::cout << parser;
      exit(0);
      // return 0;
  }
  catch (args::ParseError& e) {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }
  catch (args::ValidationError& e) {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }

  _env->size_ratio = size_ratio_cmd ? args::get(size_ratio_cmd) : 2;
  _env->buffer_size_in_pages = buffer_size_in_pages_cmd ? args::get(buffer_size_in_pages_cmd) : 128;
  _env->entries_per_page = entries_per_page_cmd ? args::get(entries_per_page_cmd) : 128;
  _env->entry_size = entry_size_cmd ? args::get(entry_size_cmd) : 128;
  _env->buffer_size = buffer_size_cmd ? args::get(buffer_size_cmd) : _env->buffer_size_in_pages * _env->entries_per_page * _env->entry_size;
  _env->file_to_memtable_size_ratio = file_to_memtable_size_ratio_cmd ? args::get(file_to_memtable_size_ratio_cmd) : 1;
  _env->file_size = file_size_cmd ? args::get(file_size_cmd) : _env->file_to_memtable_size_ratio * _env-> buffer_size;
  _env->verbosity = verbosity_cmd ? args::get(verbosity_cmd) : 0;
  _env->compaction_pri = compaction_pri_cmd ? args::get(compaction_pri_cmd) : 1;
  _env->bits_per_key = bits_per_key_cmd ? args::get(bits_per_key_cmd) : 0;
  _env->experiment_runs = experiment_runs_cmd ? args::get(experiment_runs_cmd) : 1;
//  _env->num_inserts = num_inserts_cmd ? args::get(num_inserts_cmd) : 0;
  _env->clear_sys_page_cache = clear_sys_page_cache_cmd ? true : false;
  _env->destroy = destroy_cmd ? true : false;
  _env->use_direct_reads = direct_reads_cmd ? true : false;
  _env->path = path_cmd ? args::get(path_cmd) : kDBPath;
/*
  _env->destroy = destroy_cmd ? true : false;
  _env->debugging = debug_cmd ? true : false;
  _env->num_levels = num_levels_cmd ? args::get(num_levels_cmd) : -1;
  _env->num_bits_per_entry = bits_per_entry_cmd ? args::get(bits_per_entry_cmd) : 5;
  _env->FPR_optimization_level= FPR_optimization_level_cmd ? args::get(FPR_optimization_level_cmd) : 1;
  _env->num_queries = num_queries_cmd ? args::get(num_queries_cmd) : 1000;
  _env->entry_size = entry_size_cmd ? args::get(entry_size_cmd) : 1024;
  _env->N = num_elements_cmd ? args::get(num_elements_cmd) : -1;
  _env->K = num_runs_K_cmd ? args::get(num_runs_K_cmd) : 1;
  _env->Z = num_runs_Z_cmd ? args::get(num_runs_Z_cmd) : 1;
  _env->nonzero_to_zero_ratio = nonzero_to_zero_ratio_cmd ? args::get(nonzero_to_zero_ratio_cmd) : 0;
  _env->experiment_name = exp_name_cmd ? args::get(exp_name_cmd) : "no_exp_name";
  _env->verbosity = verbosity_cmd ? args::get(verbosity_cmd) : 0;
  _env->max_levels = max_num_levels_cmd ? args::get(max_num_levels_cmd) : 1000;
  _env->target_level_for_non_zero_result_point_lookups = target_level_cmd ? args::get(target_level_cmd) : _env->num_levels;
  _env->show_progress = show_progress_cmd ? true : false;
  _env->clean_caches_for_experiments = clean_caches_cmd ? true : false;
  _env->compaction_readahead_size_KB = comp_read_size_cmd ? args::get(comp_read_size_cmd) : 2048;
  _env->print_IOs_per_file = print_IOs_per_file_cmd ? true : false;
  _env->num_deletes = num_deletes_cmd ? args::get(num_deletes_cmd) : 0;
  _env->num_updates = num_updates_cmd ? args::get(num_updates_cmd) : 0;
*/

/*
  if (_env->N==-1 && _env->num_levels==-1)
  {
    std::cerr << "Specify exactly one of -L and -N." << endl;
    exit(-1);
  }
  
  if (_env->K < 1 || _env->K > _env->T || _env->Z < 1 || _env->Z > _env->T)
  {
    std::cerr << "Check T, K, Z." << endl;
    exit(-1);
  }

  if (_env->debugging)
  {
    std::cerr << "verbosity=2 because it is debugging" << std::endl;
    _env->verbosity=2;
    std::cerr << "show_progress=true because it is debugging" << std::endl;
    _env->show_progress=true;
    std::cerr << "print_IOs_per_file=true because it is debugging" << std::endl;
    _env->print_IOs_per_file=true;
  }  
*/
  
/*
  if (_env->verbosity >= 1) {
    std::cerr << destroy_cmd.Name() << ": " << (_env->destroy ? "yes" : "no") << endl;
    std::cerr << debug_cmd.Name() << ": " << (_env->debugging ? "yes" : "no") << endl;
    std::cerr << num_levels_cmd.Name() << ": " << _env->num_levels << endl;
    std::cerr << bits_per_entry_cmd.Name() << ": " << _env->num_bits_per_entry << endl;
    std::cerr << FPR_optimization_level_cmd.Name() << ": " << _env->FPR_optimization_level << endl;
    std::cerr << path_cmd.Name() << ": " << _env->path << endl;
    std::cerr << num_queries_cmd.Name() << ": " << _env->num_queries << endl;
    std::cerr << num_inserts_cmd.Name() << ": " << _env->num_inserts << endl;
    std::cerr << size_ratio_cmd.Name() << ": " << _env->T << endl;
    std::cerr << num_runs_K_cmd.Name() << ": " << _env->K << endl;
    std::cerr << num_runs_Z_cmd.Name() << ": " << _env->Z << endl;
    std::cerr << buffer_size_cmd.Name() << ": " << _env->buffer_size << endl;
    std::cerr << entry_size_cmd.Name() << ": " << _env->entry_size << endl;
    std::cerr << num_elements_cmd.Name() << ": " << _env->N << endl;
    std::cerr << verbosity_cmd.Name() << ": " << _env->verbosity << endl;
    std::cerr << nonzero_to_zero_ratio_cmd.Name() << ": " << _env->nonzero_to_zero_ratio << endl;
    std::cerr << target_level_cmd.Name() << ": " << _env->target_level_for_non_zero_result_point_lookups << endl;
    std::cerr << rocks_db_max_num_levels_cmd.Name() << ": " << _env->rocksDB_max_levels << endl;
    std::cerr << show_progress_cmd.Name() << ": " << _env->show_progress << endl;
    std::cerr << clean_caches_cmd.Name() << ": " << _env->clean_caches_for_experiments << endl;
    std::cerr << comp_read_size_cmd.Name() << ": " << _env->compaction_readahead_size_KB << endl;
    std::cerr << print_IOs_per_file_cmd.Name() << ": " << _env->print_IOs_per_file << endl;
    std::cerr << num_deletes_cmd.Name() << ": " << _env->num_deletes << endl;
    std::cerr << num_updates_cmd.Name() << ": " << _env->num_updates << endl;
  }
*/

/*
  // Memory allocation
    op->write_buffer_size = _env-> buffer_size;
    op->max_write_buffer_number = 2;   // min 2
    op->memtable_factory = std::shared_ptr<SkipListFactory>(new SkipListFactory);
    // op->max_write_buffer_number_to_maintain = 0;    // immediately freed after flushed
    // op->db_write_buffer_size = 0;   // disable
    // op->arena_block_size = 0;
    // op->memtable_huge_page_size = 0;

    // Compaction
    op->compaction_pri= kMinOverlappingRatio;  //kOldestLargestSeqFirst kOldestSmallestSeqFirst kByCompensatedSize
    op->max_bytes_for_level_multiplier = _env->size_ratio;
    op->target_file_size_base = _env->buffer_size;    // ?
    op->level_compaction_dynamic_level_bytes = false;
    op->compaction_style = kCompactionStyleLevel;
    op->disable_auto_compactions = false;
    op->compaction_filter = nullptr;
    op->compaction_filter_factory = nullptr;
    op->access_hint_on_compaction_start = NORMAL;
    op->level0_file_num_compaction_trigger = 1;
    op->target_file_size_multiplier = 1;
    op->max_background_jobs = 1;
    op->max_compaction_bytes = 0 ;
    op->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio;
    op->merge_operator = nullptr;
    op->soft_pending_compaction_bytes_limit = 0;    // No pending compaction anytime, try and see
    op->hard_pending_compaction_bytes_limit = 0;    // No pending compaction anytime, try and see
    op->periodic_compaction_seconds = 0;
    op->use_direct_io_for_flush_and_compaction = false;
    op->num_levels = 7;
    // op->min_write_buffer_number_to_merge = 1;
    // op->compaction_readahead_size = 0;
    // op->max_bytes_for_level_multiplier_additional = std::vector<int>(op->num_levels, 1);
    // op->max_subcompactions = 1;   // no subcomapctions
    // op->avoid_flush_during_recovery = false;
    // op->atomic_flush = false;
    // op->new_table_reader_for_compaction_inputs = false;   // forced to true when using direct_IO_read
    // compaction_options_fifo;

    // TableOptions
    BlockBasedTableOptions table_options = BlockBasedTableOptions();
    table_options.block_size = _env->table_block_size;
    table_options.filter_policy = NewBloomFilterPolicy(_env->num_bits_per_entry, false);    // currently build full filter instead of blcok-based filter
    table_options.no_block_cache = true;
    table_options.block_cache = nullptr;
    table_options.cache_index_and_filter_blocks = false;
    table_options.cache_index_and_filter_blocks_with_high_priority = true;    // Deprecated by no_block_cache
    table_options.read_amp_bytes_per_bit = 4;   // temporarily 4
    table_options.data_block_index_type = kDataBlockBinarySearch;
    table_options.index_type = kBinarySearch;
    table_options.partition_filters = false;
    table_options.metadata_block_size = 4096;   // currently deprecated by data_block_index_type
    table_options.pin_top_level_index_and_filter = false;
    table_options.index_shortening = kNoShortening;
    table_options.block_size_deviation = 0;   
    // table_options.flush_block_policy_factory = nullptr;
    // table_options.block_align = false;
    // table_options.block_cache_compressed = nullptr;
    // table_options.block_restart_interval = 16;
    // table_options.index_block_restart_interval = 1;
    // table_options.format_version = 2;
    // table_options.verify_compression = false;
    // table_options.data_block_hash_table_util_ratio = 0.75;
    // table_options.checksum = kCRC32c;
    // table_options.whole_key_filtering = true;
    op->table_factory.reset(NewBlockBasedTableFactory(table_options));

    //Compression
    op->compression = kNoCompression;
    // L0 - L6: noCompression
    op->compression_per_level = {CompressionType::kNoCompression,
                                      CompressionType::kNoCompression,
                                      CompressionType::kNoCompression,
                                      CompressionType::kNoCompression,
                                      CompressionType::kNoCompression,
                                      CompressionType::kNoCompression,
                                      CompressionType::kNoCompression};



    // Read
    bool checksums = true;
    bool fill_cache = false; //data block/index block read for this iteration will not be cached
    *read_op = ReadOptions(checksums, fill_cache);
    read_op->iter_start_seqnum = 0;
    read_op->ignore_range_deletions = false;
    read_op->read_tier = kReadAllTier;
    // read_op->readahead_size = 0;
    // read_op->tailing = false;    
    // read_op->total_order_seek = false;
    // read_op->max_skippable_internal_keys = 0;
    // read_op->prefix_same_as_start = false;
    // read_op->pin_data = false;
    // read_op->background_purge_on_iterator_cleanup = false;
    // read_op->table_filter;   // assign a callback function
    // read_op->snapshot = nullptr;
    // read_op->iterate_lower_bound = nullptr;
    // read_op->iterate_upper_bound = nullptr;

    // Write
    *write_op = WriteOptions();
    write_op->low_pri = true; // every insert is less important than compaction
    write_op->sync = false; // make every write wait for sync with log (so we see real perf impact of insert)
    write_op->disableWAL = false;
    write_op->no_slowdown = false; // enabling this will make some insertions fail 
    write_op->ignore_missing_column_families = false;




    // Others
    op->level_compaction_dynamic_level_bytes = false;
    op->use_direct_reads = true;
    // op->use_direct_io_for_flush_and_compaction = true;
    // options.avoid_flush_during_shutdown = true;
    op->max_open_files = 20;    // default: -1 keep opening, or 20
    op->max_file_opening_threads = 1;
    // ReadOptions::ignore_range_deletions = false;
    op->sst_file_manager.reset(NewSstFileManager(op.env));
    // op.statistics = rocksdb::CreateDBStatistics();

    Options op;
  BlockBasedTableOptions table_options;
  // Compression
  op.compression = kNoCompression;
  op.compression_per_level = {};   // not necessary to define this var when compression = kNoCompression
  table_options.enable_index_compression = kNoCompression;
  // op.sample_for_compression = 0;    // disabled
  // op.bottommost_compression = kDisableCompressionOption;

  // Log Options
  // op.max_total_wal_size = 0;
  // op.db_log_dir = "";
  // op.max_log_file_size = 0;
  // op.wal_bytes_per_sync = 0;
  // op.strict_bytes_per_sync = false;
  // op.manual_wal_flush = false;
  // op.WAL_ttl_seconds = 0;
  // op.WAL_size_limit_MB = 0;
  // op.keep_log_file_num = 1000;
  // op.log_file_time_to_roll = 0;
  // op.recycle_log_file_num = 0;
  // op.info_log_level = nullptr;

  // Other CFOptions
  op.comparator = BytewiseComparator();
  op.max_sequential_skip_in_iterations = 8;
  op.memtable_prefix_bloom_size_ratio = 0;    // disabled
  op.num_levels = 999;
  op.level0_stop_writes_trigger = 2;
  op.paranoid_file_checks = false;
  op.optimize_filters_for_hits = false;
  op.inplace_update_support = false;
  op.inplace_update_num_locks = 10000;
  op.report_bg_io_stats = true;
  op.max_successive_merges = 0;   // read-modified-write related
  // op.prefix_extractor = nullptr;
  // op.bloom_locality = 0;
  // op.memtable_whole_key_filtering = false;
  // op.snap_refresh_nanos = 100 * 1000 * 1000;  
  // op.memtable_insert_with_hint_prefix_extractor = nullptr;
  // op.force_consistency_checks = false;

  //Other DBOptions
  op.create_if_missing = true;
  op.delayed_write_rate = 0;
  op.max_open_files = -1;
  op.max_file_opening_threads = 16;
  op.bytes_per_sync = 0;
  op.stats_persist_period_sec = 600;
  op.enable_thread_tracking = false;
  op.stats_history_buffer_size = 1024 * 1024;
  op.allow_concurrent_memtable_write = false;
  op.dump_malloc_stats = false;
  op.use_direct_reads = true;
  op.avoid_flush_during_shutdown = false;
  op.advise_random_on_open = true;
  op.delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;   // 6 hours
  op.allow_mmap_reads = false;
  op.allow_mmap_writes = false;
  // op.stats_dump_period_sec = 600;   // 10min
  // op.persist_stats_to_disk = false;
  // op.enable_pipelined_write = false;
  // op.table_cache_numshardbits = 6;
  // op.fail_iflush_options_file_error = false;
  // op.writable_file_max_buffer_size = 1024 * 1024;
  // op.write_thread_slow_yield_usec = 100;
  // op.enable_write_thread_adaptive_yield = true;
  // op.unordered_write = false;
  // op.preserve_deletes = false;
  // op.paranoid_checks = true;
  // op.two_write_queues = false;
  // op.use_fsync = true;
  // op.random_access_max_buffer_size = 1024 * 1024;
  // op.skip_stats_update_on_db_open = false;
  // op.error_if_exists = false;
  // op.manifest_preallocation_size = 4 * 1024 * 1024;
  // op.max_manifest_file_size = 1024 * 1024 * 1024;
  // op.is_fd_close_on_exec = true;
  // op.use_adaptive_mutex = false;
  // op.create_missing_column_families = false;
  // op.allow_ingest_behind = false;
  // op.avoid_unnecessary_blocking_io = false;
  // op.allow_fallocate = true;
  // op.allow_2pc = false;
  // op.write_thread_max_yield_usec = 100;
*/

  return 0;
}

/*
void sand() {
  Options options;
  options.write_buffer_size = 1024 * 1024 * 2;
  FluidLSMTreeBulkLoader* loader = new FluidLSMTreeBulkLoader(2, 1, 1, pow(2, 10), options);
  IterativelyOptimizingFilterPolicy* p = new IterativelyOptimizingFilterPolicy(5, false, 0);
  p->set_db(nullptr, loader);

  p->set_existing_to_zero_point_lookup_ratio(0);
  p->init_filters(10);
  p->print();
  std::cerr << endl;

  p->set_existing_to_zero_point_lookup_ratio(1);
  p->init_filters(10);
  p->print();
  std::cerr << endl;

  p->set_existing_to_zero_point_lookup_ratio(10);
  p->init_filters(10);
  p->print();
  std::cerr << endl;

  p->set_existing_to_zero_point_lookup_ratio(100);
  p->init_filters(10);
  p->print();
  std::cerr << endl;


  p->set_existing_to_zero_point_lookup_ratio(1000);
  p->init_filters(10);
  p->print();
  std::cerr << endl;

}
*/

void printEmulationOutput(const EmuEnv* _env, const QueryTracker *track, uint16_t runs) {
  int l = 16;
  std::cout << std::endl;
  std::cout << "-----LSM state-----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "T" << std::setfill(' ') << std::setw(l) 
                                                  << "P" << std::setfill(' ') << std::setw(l) 
                                                  << "B" << std::setfill(' ') << std::setw(l) 
                                                  << "E" << std::setfill(' ') << std::setw(l) 
                                                  << "M" << std::setfill(' ') << std::setw(l) 
                                                  << "f" << std::setfill(' ') << std::setw(l) 
                                                  << "file_size" << std::setfill(' ') << std::setw(l) 
                                                  << "compaction_pri" << std::setfill(' ') << std::setw(l) 
                                                  << "bpk" << std::setfill(' ') << std::setw(l);
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << _env->size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size_in_pages;  
  std::cout << std::setfill(' ') << std::setw(l) << _env->entries_per_page;
  std::cout << std::setfill(' ') << std::setw(l) << _env->entry_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->file_to_memtable_size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->file_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->compaction_pri;
  std::cout << std::setfill(' ') << std::setw(l) << _env->bits_per_key;
  std::cout << std::endl;

  std::cout << std::endl;
  std::cout << "----- Query summary -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "#I" << std::setfill(' ') << std::setw(l)
                                                << "#U" << std::setfill(' ') << std::setw(l)
                                                << "#D" << std::setfill(' ') << std::setw(l)
                                                << "#R" << std::setfill(' ') << std::setw(l)
                                                << "#Q" << std::setfill(' ') << std::setw(l)
                                                << "#Z" << std::setfill(' ') << std::setw(l)
                                                << "#S" << std::setfill(' ') << std::setw(l)
                                                << "#TOTAL" << std::setfill(' ') << std::setw(l);;            
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << track->inserts_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->updates_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->point_deletes_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->range_deletes_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->point_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->zero_point_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->range_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->total_completed/runs;
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "I" << std::setfill(' ') << std::setw(l)
                                                << "U" << std::setfill(' ') << std::setw(l)
                                                << "D" << std::setfill(' ') << std::setw(l)
                                                << "R" << std::setfill(' ') << std::setw(l)
                                                << "Q" << std::setfill(' ') << std::setw(l)
                                                << "Z" << std::setfill(' ') << std::setw(l)
                                                << "S" << std::setfill(' ') << std::setw(l)
                                                << "TOTAL" << std::setfill(' ') << std::setw(l);   

  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->inserts_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->updates_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->point_deletes_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_deletes_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->point_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->zero_point_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->workload_exec_time)/runs/1000000;
  std::cout << std::endl;

  std::cout << "----- Latency(ms/op) -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "avg_I" << std::setfill(' ') << std::setw(l)
                                                << "avg_U" << std::setfill(' ') << std::setw(l)
                                                << "avg_D" << std::setfill(' ') << std::setw(l)
                                                << "avg_R" << std::setfill(' ') << std::setw(l)
                                                << "avg_Q" << std::setfill(' ') << std::setw(l)
                                                << "avg_Z" << std::setfill(' ') << std::setw(l)
                                                << "avg_S" << std::setfill(' ') << std::setw(l)
                                                << "avg_query" << std::setfill(' ') << std::setw(l);   

  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->inserts_cost) / track->inserts_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->updates_cost) / track->updates_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->point_deletes_cost) / track->point_deletes_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_deletes_cost) / track->range_deletes_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->point_lookups_cost) / track->point_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->zero_point_lookups_cost) / track->zero_point_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_lookups_cost) / track->range_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->workload_exec_time) / track->total_completed / 1000000;
  std::cout << std::endl;

  std::cout << "----- Throughput(op/ms) -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "avg_I" << std::setfill(' ') << std::setw(l)
            << "avg_U" << std::setfill(' ') << std::setw(l)
            << "avg_D" << std::setfill(' ') << std::setw(l)
            << "avg_R" << std::setfill(' ') << std::setw(l)
            << "avg_Q" << std::setfill(' ') << std::setw(l)
            << "avg_Z" << std::setfill(' ') << std::setw(l)
            << "avg_S" << std::setfill(' ') << std::setw(l)
            << "avg_query" << std::setfill(' ') << std::setw(l);

  std::cout << std::endl;

  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->inserts_completed*1000000) / track->inserts_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->updates_completed*1000000) / track->updates_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->point_deletes_completed*1000000) / track->point_deletes_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->range_deletes_completed*1000000) / track->range_deletes_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->point_lookups_completed*1000000) / track->point_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->zero_point_lookups_completed*1000000) / track->zero_point_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->range_lookups_completed*1000000) / track->range_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->total_completed*1000000) / track->workload_exec_time;
  std::cout << std::endl;

  std::cout << "----- Compaction costs -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "avg_space_amp" << std::setfill(' ') << std::setw(l)
            << "avg_write_amp" << std::setfill(' ') << std::setw(l)
            << "avg_read_amp" << std::setfill(' ') << std::setw(l)
            << "avg_stalls" << std::setfill(' ') << std::setw(l);

  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << track->space_amp/runs;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << track->write_amp/runs;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << track->read_amp/runs;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << track->stalls/runs;
  std::cout << std::endl;

  if (_env->verbosity >= 1) {
    std::cout << std::endl;
    std::cout << "-----I/O stats-----" << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << "mem_get_count" << std::setfill(' ') << std::setw(l)
                                                  << "mem_get_time" << std::setfill(' ') << std::setw(l)
                                                  << "sst_get_time" << std::setfill(' ') << std::setw(l)
                                                  << "bloom_accesses" << std::setfill(' ') << std::setw(l)
                                                  << "mem_bloom_hit" << std::setfill(' ') << std::setw(l)
                                                  << "mem_bloom_miss" << std::setfill(' ') << std::setw(l)
                                                  << "sst_bloom_hit" << std::setfill(' ') << std::setw(l)
                                                  << "sst_bloom_miss" << std::setfill(' ') << std::setw(l);  
    std::cout << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << track->get_from_memtable_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->get_from_memtable_time/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->get_from_output_files_time/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->filter_block_read_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bloom_memtable_hit_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bloom_memtable_miss_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bloom_sst_hit_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bloom_sst_miss_count/runs;
    std::cout << std::endl;

    std::cout << std::setfill(' ') << std::setw(l) << "read_bytes" << std::setfill(' ') << std::setw(l)
                                                  << "read_nanos" << std::setfill(' ') << std::setw(l)
                                                  << "cpu_read_nanos" << std::setfill(' ') << std::setw(l)
                                                  << "write_bytes" << std::setfill(' ') << std::setw(l)
                                                  << "write_nanos" << std::setfill(' ') << std::setw(l)
                                                  << "cpu_write_nanos" << std::setfill(' ') << std::setw(l);  
    std::cout << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << track->bytes_read/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->read_nanos/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->cpu_read_nanos/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bytes_written/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->write_nanos/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->cpu_write_nanos/runs;
    std::cout << std::endl;
  }
}


