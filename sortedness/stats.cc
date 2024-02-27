

#include "stats.h"
#include "emu_environment.h"
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sys/time.h>

Stats *Stats::instance = 0;

Stats::Stats() {

  // current tree stats
  levels_in_tree = -1;
  files_in_tree = -1;

  // current file stats
  files_in_level.resize(0);

  // compaction stats
  compaction_count = 0;
  files_read_for_compaction = -1;
  bytes_read_for_compaction = -1;

  // latency stats
  long exp_runtime = -1; // !YBS-sep09-XX!

  files_moved_trivial = 0;
  trivial_if_accesses = 0;
  bytes_moved_trivial = 0;
}

void Stats::printStats() {
  EmuEnv *_env = EmuEnv::getInstance();

  int l = 12;

  std::cout << std::setfill(' ') << std::setw(l - 3) << "#p_ts_in_tree"
            << std::setfill(' ') << std::setw(l) << "#kv_in_tree"
            << std::setfill(' ') << std::setw(l) << "#I_done"
            << std::setfill(' ') << std::setw(l) << "L_in_tree"
            << std::setfill(' ') << std::setw(l) << "#U_done"
            << std::setfill(' ') << std::setw(l) << "#PD_done"
            << std::setfill(' ') << std::setw(l) << "#cmpt" << std::setfill(' ')
            << std::setw(l) << "#cmpt_easy" << std::setfill(' ') << std::setw(l)
            << "fls_rd_cmpt" << std::setfill(' ') << std::setw(l)
            << "fls_wr_cmpt" << std::setfill(' ') << std::setw(l)
            << "bts_rd_cmpt" << std::setfill(' ') << std::setw(l)
            << "bts_wr_cmpt"
            << "\n";

  std::cout << std::setfill(' ') << std::setw(l) << levels_in_tree;

  std::cout << std::setfill(' ') << std::setw(l) << compaction_count;

  std::cout << std::setfill(' ') << std::setw(l) << files_read_for_compaction;

  std::cout << std::setfill(' ') << std::setw(l) << bytes_read_for_compaction;

  std::cout << std::endl;

  std::cout << "files in tree = " << files_in_tree << std::endl;

  std::cout << std::setfill(' ') << std::setw(l - 3) << "\%space_amp"
            << std::setfill(' ') << std::setw(l) << "dpth" << std::setfill(' ')
            << std::setw(l + 7) << "exp_runtime (ms)"
            << "\n"; // !YBS-sep09-XX!
  std::cout << std::endl;
}

Stats *Stats::getInstance() {
  if (instance == 0)
    instance = new Stats();

  return instance;
}
