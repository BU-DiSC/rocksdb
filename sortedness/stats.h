

#ifndef STATS_H_
#define STATS_H_


#include <iostream>
#include <vector> 

using namespace std;



class Stats
{
private:
  Stats(); 
  static Stats *instance;

public:
  static Stats* getInstance();

  // current tree stats
  int levels_in_tree;
  long files_in_tree;

  // current file stats
  std::vector<int> files_in_level;

  // compaction stats
  long compaction_count;
  long files_read_for_compaction;
  long bytes_read_for_compaction;

  void printStats();


};

#endif /*STATS_H_*/


