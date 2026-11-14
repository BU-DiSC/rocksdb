#pragma once

#include <cinttypes>
#include <cmath>

namespace ROCKSDB_NAMESPACE {
inline uint64_t GetPTTL(uint64_t dpt, int target_level, int total_levels,
                double size_ratio) {
  double base = ((size_ratio-1)/(pow(size_ratio, total_levels-1)-1));
  return static_cast<uint64_t>(size_ratio * dpt * base);
}
inline uint64_t GetILIT(uint64_t dpt, uint64_t ldt, int target_level, int total_levels,
                double size_ratio) {
  double base = (pow(size_ratio,target_level)-1/(pow(size_ratio, total_levels-1)-1));
  return static_cast<uint64_t>(dpt * base);
}
}  // namespace ROCKSDB_NAMESPACE
