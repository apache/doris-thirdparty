#pragma once

#include "CLucene/index/Terms.h"

#include <limits.h>
#include <cstdint>

CL_NS_USE(index)

class TermIterator {
public:
  TermIterator() = default;
  TermIterator(TermDocs* termDocs) 
    : termDocs_(termDocs) {
  }

  inline bool isEmpty() const {
    return termDocs_ == nullptr;
  }

  inline int32_t docID() const {
    int32_t docId = termDocs_->doc();
    return docId >= INT_MAX ? INT_MAX : docId;
  }

  inline int32_t freq() const {
    return termDocs_->freq();
  }

  inline int32_t norm() const {
      return termDocs_->norm();
  }

  inline int32_t nextDoc() const {
    if (termDocs_->next()) {
      return termDocs_->doc();
    }
    return INT_MAX;
  }

  inline int32_t advance(int32_t target) const {
    if (termDocs_->skipTo(target)) {
      return termDocs_->doc();
    }
    return INT_MAX;
  }

  inline int32_t docFreq() const {
    return termDocs_->docFreq();
  }

  inline int32_t docNorm() const {
      return termDocs_->docNorm();
  }

  inline bool readRange(DocRange* docRange) const {
    return termDocs_->readRange(docRange);
  }
  
protected:
  TermDocs* termDocs_ = nullptr;
};