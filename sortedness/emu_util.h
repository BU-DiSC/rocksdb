/*
 *  Created on: Oct 9, 2019
 *  Author: Guanting Chen
 */

#ifndef EMU_UTIL_H_
#define EMU_UTIL_H_

#include <iostream>
#include <fstream>
#include <thread>
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/convenience.h"
#include "db/db_impl/db_impl.h"
#include "util/cast_util.h"

using namespace rocksdb;

// Close DB in a way of detecting errors
// followed by deleting the database object when examined to determine if there were any errors. 
// Regardless of errors, it will release all resources and is irreversible.
// Flush the memtable before close 
Status CloseDB(DB *&db, const FlushOptions &flush_op);

// Reopen DB with configured options and a consistent dbptr
// use DB::Close()
Status ReopenDB(DB *&db, const Options &op, const FlushOptions &flush_op);

bool CompactionMayAllComplete(DB *db);
bool FlushMemTableMayAllComplete(DB *db);

// Print progress bar during workload execution
// n : total number of queries
// count : number of queries finished
// mini_count : keep track of current progress of percentage
inline void showProgress(const uint64_t &n, const uint64_t &count, uint64_t &mini_count) {
  if(count % (n/100) == 0){
  	if (count == n || n == 0) {
	    std::cout << ">OK!\n";
	    return;
  	} 
    if(count % (n/10) == 0) {
      std::cout << ">" << ++mini_count * 10 << "%<";
      fflush(stdout);
    }
  	std::cout << "=";
    fflush(stdout);
  }
}

// Hardcode command to clear system cache 
// May need password to get root access.
inline void clearPageCache() {
  system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
	// sync();
	// std::ofstream ofs("/proc/sys/vm/drop_caches");
	// ofs << "3" << std::endl;
}

// Sleep program for millionseconds
inline void sleep_for_ms(uint32_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}


#endif /*EMU_UTIL_H_*/