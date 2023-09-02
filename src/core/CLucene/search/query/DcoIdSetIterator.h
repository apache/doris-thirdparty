#pragma once

#include "CLucene/index/DocRange.h"

class DocIdSetIterator {
public:
  DocIdSetIterator() = default;
  virtual ~DocIdSetIterator() = default;

  virtual int32_t docID() = 0;
  virtual int32_t nextDoc() = 0;
  virtual int32_t advance(int32_t target) = 0;

  virtual int32_t docFreq() const = 0;
  virtual bool readRange(DocRange* docRange) const = 0;
};