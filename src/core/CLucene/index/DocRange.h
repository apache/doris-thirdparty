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
  DocRange() = default;
  ~DocRange() = default;

 public:
  DocRangeType type_ = DocRangeType::kNone;

  uint32_t doc_many_size_ = 0;
  uint32_t freq_many_size_ = 0;
  uint32_t norm_many_size_ = 0;

  std::vector<uint32_t>* doc_many = nullptr;
  std::vector<uint32_t>* freq_many = nullptr;
  std::vector<uint32_t>* norm_many = nullptr;

  std::pair<uint32_t, uint32_t> doc_range;
};