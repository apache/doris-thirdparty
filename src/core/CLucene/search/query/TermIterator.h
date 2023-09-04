#pragma once

#include "CLucene/search/query/DcoIdSetIterator.h"
#include "CLucene/index/Terms.h"

#include <limits.h>

CL_NS_USE(index)

class TermIterator : public DocIdSetIterator {
public:
  TermIterator() = default;
  TermIterator(TermDocs* termDocs) : termDocs_(termDocs) {
  }

  virtual ~TermIterator() = default;

  bool isEmpty() {
    return termDocs_ == nullptr;
  }

  int32_t docID() override {
    uint32_t docId = termDocs_->doc();
    return docId >= INT_MAX ? INT_MAX : docId;
  }

  int32_t nextDoc() override {
    if (termDocs_->next()) {
      return termDocs_->doc();
    }
    return INT_MAX;
  }

  int32_t advance(int32_t target) override {
    if (termDocs_->skipTo(target)) {
      return termDocs_->doc();
    }
    return INT_MAX;
  }

  int32_t docFreq() const override {
    return termDocs_->docFreq();
  }

  bool readRange(DocRange* docRange) const override {
    return termDocs_->readRange(docRange);
  }
  
private:
  TermDocs* termDocs_ = nullptr;
};