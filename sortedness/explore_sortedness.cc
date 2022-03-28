#include <cassert>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>

#include "../util/string_util.h"
#include "args.hxx"
#include "aux_time.h"
#include "progressbar.hpp"
// #include "emu_util.h"
#include "env.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/table.h"
#include "stats.h"
#include "tabulate.hpp"
#include "workload_stats.h"

using namespace rocksdb;

std::string workloadPath = "";
std::string kDBPath = "db_working_home";

struct experiment_stats {
  int num_to_be_inserted = 0;
  int num_to_lookup = 0;
  unsigned long long insert_time = 0;
  int num_inserts = 0;
  unsigned long long pl_time = 0;  // point lookups
  int num_positive_pl = 0;
  int num_empty_pl = 0;
} experiment_stats;

int parse_arguments2(int argc, char *argv[], EmuEnv *_env);

char *EncodeVarint32(char *dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char *ptr = reinterpret_cast<unsigned char *>(dst);
  static const int B = 128;
  if (v < (1 << 7)) {
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }

  return reinterpret_cast<char *>(ptr);
}

void intToByte(int n, char bytes[]) {
  //   for (int i = 0; i < 4; i++)
  //     byteArr[3 - i] = (val >> (i * 8));
  //   std::cout << "done converting" << endl;
  bytes[0] = (n >> 24) & 0xFF;
  bytes[1] = (n >> 16) & 0xFF;
  bytes[2] = (n >> 8) & 0xFF;
  bytes[3] = n & 0xFF;
}

void configOptions(EmuEnv *_env, Options *op, BlockBasedTableOptions *table_op,
                   WriteOptions *write_op, ReadOptions *read_op,
                   FlushOptions *flush_op) {
  // Experiment settings
  _env->experiment_runs =
      (_env->experiment_runs >= 1) ? _env->experiment_runs : 1;
  // *op = Options();
  op->write_buffer_size = _env->buffer_size;
  std::cout << "BUFFER SIZE = " << op->write_buffer_size << std::endl;
  op->max_write_buffer_number = _env->max_write_buffer_number;  // min 2

  // enable trivial move here
    op->compaction_options_universal.allow_trivial_move = false;

  switch (_env->memtable_factory) {
    case 1:
      op->memtable_factory =
          std::shared_ptr<SkipListFactory>(new SkipListFactory);
      break;
    case 2:
      op->memtable_factory =
          std::shared_ptr<VectorRepFactory>(new VectorRepFactory);
      break;
    case 3:
      op->memtable_factory.reset(NewHashSkipListRepFactory());
      break;
    case 4:
      op->memtable_factory.reset(NewHashLinkListRepFactory());
      break;
    default:
      std::cerr << "error: memtable_factory" << std::endl;
  }

  // Compaction
  switch (_env->compaction_pri) {
    case 1:
      op->compaction_pri = kMinOverlappingRatio;
      break;
    case 2:
      op->compaction_pri = kByCompensatedSize;
      break;
    case 3:
      op->compaction_pri = kOldestLargestSeqFirst;
      break;
    case 4:
      op->compaction_pri = kOldestSmallestSeqFirst;
      break;
    default:
      std::cerr << "error: compaction_pri" << std::endl;
  }

  op->max_bytes_for_level_multiplier = _env->size_ratio;
  op->target_file_size_base = _env->file_size;
  op->level_compaction_dynamic_level_bytes =
      _env->level_compaction_dynamic_level_bytes;
  switch (_env->compaction_style) {
    case 1:
      op->compaction_style = kCompactionStyleLevel;
      break;
    case 2:
      op->compaction_style = kCompactionStyleUniversal;
      break;
    case 3:
      op->compaction_style = kCompactionStyleFIFO;
      break;
    case 4:
      op->compaction_style = kCompactionStyleNone;
      break;
    default:
      std::cerr << "error: compaction_style" << std::endl;
  }

  op->disable_auto_compactions = _env->disable_auto_compactions;
  if (_env->compaction_filter == 0) {
    ;  // do nothing
  } else {
    ;  // invoke manual compaction_filter
  }
  if (_env->compaction_filter_factory == 0) {
    ;  // do nothing
  } else {
    ;  // invoke manual compaction_filter_factory
  }
  switch (_env->access_hint_on_compaction_start) {
    case 1:
      op->access_hint_on_compaction_start = DBOptions::AccessHint::NONE;
      break;
    case 2:
      op->access_hint_on_compaction_start = DBOptions::AccessHint::NORMAL;
      break;
    case 3:
      op->access_hint_on_compaction_start = DBOptions::AccessHint::SEQUENTIAL;
      break;
    case 4:
      op->access_hint_on_compaction_start = DBOptions::AccessHint::WILLNEED;
      break;
    default:
      std::cerr << "error: access_hint_on_compaction_start" << std::endl;
  }

  op->level0_file_num_compaction_trigger =
      _env->level0_file_num_compaction_trigger;
  op->level0_slowdown_writes_trigger = _env->level0_slowdown_writes_trigger;
  op->level0_stop_writes_trigger = _env->level0_stop_writes_trigger;
  op->target_file_size_multiplier = _env->target_file_size_multiplier;
  op->max_background_jobs = _env->max_background_jobs;
  op->max_compaction_bytes = _env->max_compaction_bytes;
  op->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio;
  ;
  if (_env->merge_operator == 0) {
    ;  // do nothing
  } else {
    ;  // use custom merge operator
  }
  op->soft_pending_compaction_bytes_limit =
      _env->soft_pending_compaction_bytes_limit;  // No pending compaction
                                                  // anytime, try and see
  op->hard_pending_compaction_bytes_limit =
      _env->hard_pending_compaction_bytes_limit;  // No pending compaction
                                                  // anytime, try and see
  op->periodic_compaction_seconds = _env->periodic_compaction_seconds;
  op->use_direct_io_for_flush_and_compaction =
      _env->use_direct_io_for_flush_and_compaction;
  op->num_levels = _env->num_levels;

  // Compression
  switch (_env->compression) {
    case 1:
      op->compression = kNoCompression;
      break;
    case 2:
      op->compression = kSnappyCompression;
      break;
    case 3:
      op->compression = kZlibCompression;
      break;
    case 4:
      op->compression = kBZip2Compression;
      break;
    case 5:
      op->compression = kLZ4Compression;
      break;
    case 6:
      op->compression = kLZ4HCCompression;
      break;
    case 7:
      op->compression = kXpressCompression;
      break;
    case 8:
      op->compression = kZSTD;
      break;
    case 9:
      op->compression = kZSTDNotFinalCompression;
      break;
    case 10:
      op->compression = kDisableCompressionOption;
      break;

    default:
      std::cerr << "error: compression" << std::endl;
  }

  // table_options.enable_index_compression = kNoCompression;

  // Other CFOptions
  switch (_env->comparator) {
    case 1:
      op->comparator = BytewiseComparator();
      break;
    case 2:
      op->comparator = ReverseBytewiseComparator();
      break;
    case 3:
      // use custom comparator
      break;
    default:
      std::cerr << "error: comparator" << std::endl;
  }

  op->max_sequential_skip_in_iterations =
      _env->max_sequential_skip_in_iterations;
  op->memtable_prefix_bloom_size_ratio =
      _env->memtable_prefix_bloom_size_ratio;  // disabled
  op->paranoid_file_checks = _env->paranoid_file_checks;
  op->optimize_filters_for_hits = _env->optimize_filters_for_hits;
  op->inplace_update_support = _env->inplace_update_support;
  op->inplace_update_num_locks = _env->inplace_update_num_locks;
  op->report_bg_io_stats = _env->report_bg_io_stats;
  op->max_successive_merges =
      _env->max_successive_merges;  // read-modified-write related

  // Other DBOptions
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
  op->delete_obsolete_files_period_micros =
      _env->delete_obsolete_files_period_micros;  // 6 hours
  op->allow_mmap_reads = _env->allow_mmap_reads;
  op->allow_mmap_writes = _env->allow_mmap_writes;

  // TableOptions
  table_op->no_block_cache = _env->no_block_cache;  // TBC
  if (_env->block_cache == 0) {
    ;  // do nothing
  } else {
    ;  // invoke manual block_cache
  }

  if (_env->bits_per_key == 0) {
    ;  // do nothing
  } else {
    table_op->filter_policy.reset(NewBloomFilterPolicy(
        _env->bits_per_key,
        false));  // currently build full filter instead of blcok-based filter
  }

  table_op->cache_index_and_filter_blocks = _env->cache_index_and_filter_blocks;
  table_op->cache_index_and_filter_blocks_with_high_priority =
      _env->cache_index_and_filter_blocks_with_high_priority;  // Deprecated by
                                                               // no_block_cache
  table_op->read_amp_bytes_per_bit = _env->read_amp_bytes_per_bit;

  switch (_env->data_block_index_type) {
    case 1:
      table_op->data_block_index_type =
          BlockBasedTableOptions::kDataBlockBinarySearch;
      break;
    case 2:
      table_op->data_block_index_type =
          BlockBasedTableOptions::kDataBlockBinaryAndHash;
      break;
    default:
      std::cerr << "error: TableOptions::data_block_index_type" << std::endl;
  }
  switch (_env->index_type) {
    case 1:
      table_op->index_type = BlockBasedTableOptions::kBinarySearch;
      break;
    case 2:
      table_op->index_type = BlockBasedTableOptions::kHashSearch;
      break;
    case 3:
      table_op->index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
      break;
    default:
      std::cerr << "error: TableOptions::index_type" << std::endl;
  }
  table_op->partition_filters = _env->partition_filters;
  table_op->block_size = _env->entries_per_page * _env->entry_size;
  table_op->metadata_block_size = _env->metadata_block_size;
  table_op->pin_top_level_index_and_filter =
      _env->pin_top_level_index_and_filter;

  switch (_env->index_shortening) {
    case 1:
      table_op->index_shortening =
          BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
      break;
    case 2:
      table_op->index_shortening =
          BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators;
      break;
    case 3:
      table_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::
          kShortenSeparatorsAndSuccessor;
      break;
    default:
      std::cerr << "error: TableOptions::index_shortening" << std::endl;
  }
  table_op->block_size_deviation = _env->block_size_deviation;
  table_op->enable_index_compression = _env->enable_index_compression;
  // Set all table options
  op->table_factory.reset(NewBlockBasedTableFactory(*table_op));

  // WriteOptions
  write_op->sync = _env->sync;  // make every write wait for sync with log (so
                                // we see real perf impact of insert)
  write_op->low_pri =
      _env->low_pri;  // every insert is less important than compaction
  write_op->disableWAL = _env->disableWAL;
  write_op->no_slowdown =
      _env->no_slowdown;  // enabling this will make some insertions fail
  write_op->ignore_missing_column_families =
      _env->ignore_missing_column_families;

  // ReadOptions
  read_op->verify_checksums = _env->verify_checksums;
  read_op->fill_cache = _env->fill_cache;
  read_op->iter_start_seqnum = _env->iter_start_seqnum;
  read_op->ignore_range_deletions = _env->ignore_range_deletions;
  switch (_env->read_tier) {
    case 1:
      read_op->read_tier = kReadAllTier;
      break;
    case 2:
      read_op->read_tier = kBlockCacheTier;
      break;
    case 3:
      read_op->read_tier = kPersistedTier;
      break;
    case 4:
      read_op->read_tier = kMemtableTier;
      break;
    default:
      std::cerr << "error: ReadOptions::read_tier" << std::endl;
  }

  // FlushOptions
  flush_op->wait = _env->wait;
  flush_op->allow_write_stall = _env->allow_write_stall;

  // statistics
  op->statistics = CreateDBStatistics();
  op->statistics->set_stats_level(StatsLevel::kAll);
}

int performIngestions(DB *&db, int *data, const WriteOptions *write_op,
                      const ReadOptions *read_op, bool show_progress) {
  if (show_progress) cout << "Inserts\t";

  uint64_t progress_counter = 0;
  int n = experiment_stats.num_to_be_inserted;
  progressbar bar(n);
  bar.set_todo_char(" ");
  bar.set_done_char("█");

  char **array_key = new char *[n];

  for (int i = 0; i < experiment_stats.num_to_be_inserted; i++) {
    int intKey = data[i] + 1;
    array_key[i] = new char[4];
    memcpy(array_key[i], &intKey, sizeof(intKey));
    //   intToByte(intKey, array_key[i]);
  }


  std::cout << "==============================================================="
            << std::endl;
  for (int i = 0; i < n; i++) {
    if (show_progress) bar.update();
    // int intKey = data[i] + 1;
    // convert each integer to byte array

    // char key[4];
    // memset(key, '\0', sizeof(char) * 4);
    // EncodeVarint32(key, intKey);
    // intToByte(intKey, key);
    // insert into the lsm tree
    my_clock start_time, end_time;
    if (my_clock_get_time(&start_time) == -1) {
      std::cerr << "Failed to get experiment start time" << std::endl;
    }

    // Status s = db->Put(*write_op, array_key[i], "Character Counter is a 100% free online character count calculator that's simple to use. Sometimes users prefer simplicity over all of the detailed writing information Word Counter provides, and this is exactly what this tool offers. It displays character count and word count which is often the only information a person needs to know about their writing. Best of all, you receive the needed information at a lightning fast speed.Character Counter is a 100% free online character count calculator that's simple to use. Sometimes users prefer simplicity over all of the detailed writing information Word Counter provides, and this is exactly what this tool offers. It displays character count and word count which is often the only information a person needs to know about their writing. Best of all, you receive the needed information at a lightning fast speed.Character Counter is a 100% free online character count calculator that's simple to use. Sometimes users prefer simplicity over all of theasdfasdfadsfasdfadsf");
    Status s = db->Put(*write_op, array_key[i], array_key[i]);
    assert(s.ok());
    string ret_val = "";
    // s = db->Get(*read_op, key, &ret_val);

    if (my_clock_get_time(&end_time) == -1) {
      std::cerr << "Failed to get experiment end time" << std::endl;
    }
    
    experiment_stats.num_inserts++;
    experiment_stats.insert_time += getclock_diff_ns(start_time, end_time);
  }

//   assert(experiment_stats.num_inserts == experiment_stats.num_to_be_inserted);
  cout << endl;

  // delete all pointers to array_key 
  for(int i = 0; i< n; i++)
  {
      delete array_key[i];
  }
  delete[] array_key;
  return 1;
}

int performPointLookups(DB *&db, int *data, bool show_progress) {
  if (show_progress) cout << "Point lookups\t";
  int tot_inserts = experiment_stats.num_inserts;
  progressbar bar(experiment_stats.num_to_lookup);
  bar.set_todo_char(" ");
  bar.set_done_char("█");
  for (int i = 0; i < experiment_stats.num_to_lookup; i++) {
    if (show_progress) bar.update();
    // pick a number between 1 and tot_inserts
    int query_index = (rand() % tot_inserts) + 1;
    std::string returnedValue = "";
    char sk[4];
    memset(sk, '\0', sizeof(char) * 4);
    // // EncodeVarint32(sk, query_index);
    // intToByte(query_index, sk);

    memcpy(sk, &query_index, sizeof(query_index));

    my_clock start_time, end_time;
    if (my_clock_get_time(&start_time) == -1) {
      std::cerr << "Failed to get experiment start time" << std::endl;
    }
    // perform the get operation
    Status s = db->Get(rocksdb::ReadOptions(), sk, &returnedValue);
    if (my_clock_get_time(&end_time) == -1) {
      std::cerr << "Failed to get experiment end time" << std::endl;
    }

    if (s.ok()) {
      // match key and value. Both should be same in this experiment

      experiment_stats.num_positive_pl++;

    } else {
      experiment_stats.num_empty_pl++;
    }
    experiment_stats.pl_time += getclock_diff_ns(start_time, end_time);
  }
  cout << endl;
  return 1;
}
void printEmulationOutput(const EmuEnv *_env) {
  int l = 16;
  std::cout << std::endl;
  std::cout << "-----LSM state-----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "T" << std::setfill(' ')
            << std::setw(l) << "L" << std::setfill(' ') << std::setw(l) << "P"
            << std::setfill(' ') << std::setw(l) << "B" << std::setfill(' ')
            << std::setw(l) << "E" << std::setfill(' ') << std::setw(l) << "M"
            << std::setfill(' ') << std::setw(l) << "f" << std::setfill(' ')
            << std::setw(l) << "file_size" << std::setfill(' ') << std::setw(l)
            << "compaction_pri" << std::setfill(' ') << std::setw(l) << "bpk"
            << std::setfill(' ') << std::setw(l);
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << _env->size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->derived_num_levels;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size_in_pages;
  std::cout << std::setfill(' ') << std::setw(l) << _env->entries_per_page;
  std::cout << std::setfill(' ') << std::setw(l) << _env->entry_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size;
  std::cout << std::setfill(' ') << std::setw(l)
            << _env->file_to_memtable_size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->file_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->compaction_pri;
  std::cout << std::setfill(' ') << std::setw(l) << _env->bits_per_key;
  std::cout << std::endl;

  std::cout << std::endl;
}

void printExperimentResults(DB *db, Statistics *db_stats, EmuEnv *_env) {
  // call this to get tree stats for levels
  string key = "rocksdb.stats";
  string str;
  db->GetProperty(key, &str);

  cout << "************** Experimental Statistics *********************"
       << endl;

  // use Tabulate helper to create a table
  tabulate::Table stats;

  // create header
  stats.add_row({"Metric", "Value"});
  Stats *com_stats = Stats::getInstance();
  // add data
  stats.add_row({"Size Ratio", to_string(_env->size_ratio)});
  stats.add_row(
      {"Ingestion time", to_string(experiment_stats.insert_time) + " ns"});
  stats.add_row({"Num inserts", to_string(experiment_stats.num_inserts)});
  stats.add_row(
      {"Point lookup time", to_string(experiment_stats.pl_time) + " ns"});
  stats.add_row({"Num lookups", to_string(experiment_stats.num_to_lookup)});
  stats.add_row(
      {"Num +ve lookups", to_string(experiment_stats.num_positive_pl)});
  stats.add_row(
      {"Num empty lookups", to_string(experiment_stats.num_empty_pl)});
  stats.add_row(
      {"Bytes Written", to_string(db_stats->getTickerCount(BYTES_WRITTEN))});
  stats.add_row({"#. Keys Written",
                 to_string(db_stats->getTickerCount(NUMBER_KEYS_WRITTEN))});
  stats.add_row({"#. Compactions", to_string(com_stats->compaction_count)});
  stats.add_row({"#. Levels in Tree", to_string(com_stats->levels_in_tree)});

  // table format
  stats.format()
      .font_style({tabulate::FontStyle::bold})
      .border_top("-")
      .border_bottom("-")
      .border_left("|")
      .border_right("|")
      .corner("+");

  // header format
  stats[0]
      .format()
      .padding_top(1)
      .padding_bottom(1)
      .font_align(tabulate::FontAlign::center)
      .font_style({tabulate::FontStyle::underline});
  //   .font_background_color(tabulate::Color::red);

  stats.column(1).format().font_color(tabulate::Color::yellow);
  stats.column(0).format().font_color(tabulate::Color::blue);

  //   stats.column(0).format().font_align(tabulate::FontAlign::center);
  stats.column(1).format().font_align(tabulate::FontAlign::center);

  // print table
  stats.print(std::cout);
  std::cout << endl;
  std::cout << "\n====\tRocksdb Stats printing to log file ====" << endl;
  //   com_stats->printStats();
}

void PrintStats(DB *db, const char *key, bool print_header = false) {
  if (print_header) {
    fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
  }
  std::string stats;
  if (!db->GetProperty(key, &stats)) {
    stats = "(failed)";
  }
  fprintf(stdout, "\n%s\n", stats.c_str());
}

int runExperiments(EmuEnv *_env, int *data) {
  DB *db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;
  // init RocksDB configurations and experiment settings
  configOptions(_env, &options, &table_options, &write_options, &read_options,
                &flush_options);
  Status s = DB::Open(options, _env->path, &db);
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  cout << "Ingestion_file_path: " << _env->ingestion_path << endl;

  if (_env->destroy) {
    // DestroyDB(kDBPath, options);
    cout << "hi " << endl;
    DestroyDB(_env->path, options);
  }

  if (_env->show_progress)
    cout << "********************* Starting Experiments "
            "*****************************"
         << endl;

  // perform ingestions
  performIngestions(db, data, &write_options, &read_options,
                    _env->show_progress);

  // perform point lookups for verification
  performPointLookups(db, data, _env->show_progress);

  Statistics *stats = options.statistics.get();
  // print experiment results/stats
  printExperimentResults(db, stats, _env);

  PrintStats(db, "rocksdb.stats", true);

  //   cout<<db->GetAggregatedIntPropert

  // close db and log if error
  s = db->Close();
  if (!s.ok()) cerr << s.ToString() << endl;

  delete db;
  return 0;
}

int main(int argc, char *argv[]) {
  EmuEnv *_env = EmuEnv::getInstance();

  if (parse_arguments2(argc, argv, _env)) {
    exit(1);
  }

  int *data;
  long int size = 0;

  Stats *instance = Stats::getInstance();

  ifstream infile(_env->ingestion_path, ios::in | ios::binary);
  if (!infile) {
    cout << "Cannot open file!" << endl;
    return 0;
  }
  FILE *file = fopen(_env->ingestion_path.c_str(), "rb");
  if (file == NULL) return 0;
  fseek(file, 0, SEEK_END);
  size = ftell(file);
  fclose(file);

  cout << "size = " << size << endl;

  data = new int[size / sizeof(int)];
  infile.read((char *)data, size);
  // fclose(input_file);
  infile.close();

  int num = size / sizeof(int);

  cout << "Number of inserts = " << num << endl;

  experiment_stats.num_to_be_inserted = num;
  experiment_stats.num_to_lookup = 100;
  cout << "Show progress" << (bool)_env->show_progress << endl;

  int s = runExperiments(_env, data);
  printEmulationOutput(_env);

  return 0;
}

int parse_arguments2(int argc, char *argv[], EmuEnv *_env) {
  args::ArgumentParser parser("RocksDB_parser.", "");

  args::Group group1(parser, "This group is all exclusive:",
                     args::Group::Validators::DontCare);
  args::Group group4(parser, "Optional switches and parameters:",
                     args::Group::Validators::DontCare);
  args::ValueFlag<int> size_ratio_cmd(
      group1, "T", "The size ratio of two adjacent levels  [def: 2]",
      {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(
      group1, "P", "The number of pages that can fit into a buffer [def: 128]",
      {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(
      group1, "B", "The number of entries that fit into a page [def: 128]",
      {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(
      group1, "E", "The size of a key-value pair inserted into DB [def: 128 B]",
      {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(
      group1, "M",
      "The size of a buffer that is configured manually [def: 2 MB]",
      {'M', "memory_size"});
  args::ValueFlag<int> file_to_memtable_size_ratio_cmd(
      group1, "file_to_memtable_size_ratio",
      "The size of a file over the size of configured buffer size [def: 1]",
      {'f', "file_to_memtable_size_ratio"});
  args::ValueFlag<long> file_size_cmd(
      group1, "file_size",
      "The size of a file that is configured manually [def: 2 MB]",
      {'F', "file_size"});
  args::ValueFlag<int> compaction_pri_cmd(
      group1, "compaction_pri",
      "[Compaction priority: 1 for kMinOverlappingRatio, 2 for "
      "kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for "
      "kOldestSmallestSeqFirst; def: 2]",
      {'C', "compaction_pri"});
  args::ValueFlag<int> bits_per_key_cmd(
      group1, "bits_per_key",
      "The number of bits per key assigned to Bloom filter [def: 0]",
      {'b', "bits_per_key"});
  args::ValueFlag<int> experiment_runs_cmd(
      group1, "experiment_runs",
      "The number of experiments repeated each time [def: 1]", {'R', "run"});
  //  args::ValueFlag<long> num_inserts_cmd(group1, "inserts", "The number of
  //  unique inserts to issue in the experiment [def: 0]", {'i', "inserts"});
  args::Flag clear_sys_page_cache_cmd(
      group4, "clear_sys_page_cache",
      "Clear system page cache before experiments", {"cc", "clear_cache"});
  args::Flag destroy_cmd(group4, "destroy_db",
                         "Destroy and recreate the database",
                         {"dd", "destroy_db"});
  args::Flag direct_reads_cmd(group4, "use_direct_reads", "Use direct reads",
                              {"dr", "use_direct_reads"});
  args::ValueFlag<int> verbosity_cmd(
      group4, "verbosity", "The verbosity level of execution [0,1,2; def: 0]",
      {'V', "verbosity"});
  args::ValueFlag<std::string> path_cmd(
      group4, "path", "path for writing the DB and all the metadata files",
      {'p', "path"});

  args::ValueFlag<std::string> ingestionPath_cmd(
      group4, "ingestionPath", "file path for ingestion workload",
      {'i', "ingestionPath"});

  //   args::Flag show_progress_cmd(group4, "showProgress",
  //                                "Display Progress Bar for operations",
  //                                {"pg", "showProgress"});

  try {
    parser.ParseCLI(argc, argv);
  } catch (args::Help &) {
    std::cout << parser;
    exit(0);
    // return 0;
  } catch (args::ParseError &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  } catch (args::ValidationError &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  }

  _env->size_ratio =
      size_ratio_cmd ? args::get(size_ratio_cmd) : _env->size_ratio;
  _env->buffer_size_in_pages = buffer_size_in_pages_cmd
                                   ? args::get(buffer_size_in_pages_cmd)
                                   : _env->buffer_size_in_pages;
  _env->entries_per_page = entries_per_page_cmd
                               ? args::get(entries_per_page_cmd)
                               : _env->entries_per_page;
  _env->entry_size =
      entry_size_cmd ? args::get(entry_size_cmd) : _env->entry_size;
  _env->buffer_size = buffer_size_cmd
                          ? args::get(buffer_size_cmd)
                          : _env->buffer_size_in_pages *
                                _env->entries_per_page * _env->entry_size;
  _env->file_to_memtable_size_ratio =
      file_to_memtable_size_ratio_cmd
          ? args::get(file_to_memtable_size_ratio_cmd)
          : _env->file_to_memtable_size_ratio;
  _env->file_size = file_size_cmd
                        ? args::get(file_size_cmd)
                        : _env->file_to_memtable_size_ratio * _env->buffer_size;
  _env->verbosity = verbosity_cmd ? args::get(verbosity_cmd) : _env->verbosity;
  _env->compaction_pri =
      compaction_pri_cmd ? args::get(compaction_pri_cmd) : _env->compaction_pri;
  _env->bits_per_key =
      bits_per_key_cmd ? args::get(bits_per_key_cmd) : _env->bits_per_key;
  _env->experiment_runs = experiment_runs_cmd ? args::get(experiment_runs_cmd)
                                              : _env->experiment_runs;
  //  _env->num_inserts = num_inserts_cmd ? args::get(num_inserts_cmd) : 0;
  _env->clear_sys_page_cache =
      clear_sys_page_cache_cmd ? true : _env->clear_sys_page_cache;
  _env->destroy = destroy_cmd ? true : _env->destroy;
  _env->use_direct_reads = direct_reads_cmd ? true : _env->use_direct_reads;
  _env->path = path_cmd ? args::get(path_cmd) : _env->path;
  _env->ingestion_path =
      ingestionPath_cmd ? args::get(ingestionPath_cmd) : workloadPath;
  // cout<<"show cmd = "<<args::get(show_progress_cmd)<<endl;
  // _env->show_progress = show_progress_cmd ? true : false;
  _env->show_progress = true;
  return 0;
}
