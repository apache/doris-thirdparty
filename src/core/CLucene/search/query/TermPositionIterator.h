#pragma once

#include "CLucene/search/query/TermIterator.h"
#include "CLucene/index/Terms.h"

#include <limits.h>

CL_NS_USE(index)

class TermPositionIterator : public TermIterator {
public:
  TermPositionIterator() = default;
  TermPositionIterator(TermPositions* termPositions) 
    : TermIterator(termPositions), termPositions_(termPositions) {
  }

  inline int32_t nextPosition() const {
    return termPositions_->nextPosition();
  }

private:
  TermPositions* termPositions_ = nullptr;
};