#pragma once

#include <cinttypes>
#include <cmath>

namespace ROCKSDB_NAMESPACE {
inline uint64_t GetLevelDPT(uint64_t dpt, int target_level, int total_levels,
                double size_ratio) {
  double base = dpt * (size_ratio - 1) / (pow(size_ratio, (total_levels - 1)) - 1);
  return base * pow(size_ratio, target_level);
}
}  // namespace ROCKSDB_NAMESPACE