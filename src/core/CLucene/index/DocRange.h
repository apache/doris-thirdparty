#pragma once

#include <stdint.h>

#include <vector>

#include "CLucene/CLConfig.h"

enum class DocRangeType {
  kMany = 0,
  kRange = 1,

  kNone
};

class DocRange {
 public:
  DocRange() : doc_many(PFOR_BLOCK_SIZE + 3), freq_many(PFOR_BLOCK_SIZE + 3) {}

 public:
  DocRangeType type_ = DocRangeType::kNone;

  uint32_t doc_many_size_ = 0;
  uint32_t freq_many_size_ = 0;
  std::vector<uint32_t> doc_many;
  std::vector<uint32_t> freq_many;

  std::pair<uint32_t, uint32_t> doc_range;
};