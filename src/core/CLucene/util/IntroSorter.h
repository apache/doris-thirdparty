#pragma once
#include "CLucene/SharedHeader.h"
#include "CLucene/util/Sorter.h"

#include <vector>

CL_NS_DEF(util)

/**
 * {@link Sorter} implementation based on a variant of the quicksort algorithm
 * called <a href="http://en.wikipedia.org/wiki/Introsort">introsort</a>: when
 * the recursion level exceeds the log of the length of the array to sort, it
 * falls back to heapsort. This prevents quicksort from running into its
 * worst-case quadratic runtime. Small arrays are sorted with
 * insertion sort.
 * @lucene.internal
 */
class IntroSorter : public Sorter
{
  /** Create a new {@link IntroSorter}. */
public:
  IntroSorter();

  void sort(int from, int to) override final;

  virtual void quicksort(int from, int to, int maxDepth);

  // Don't rely on the slow default impl of setPivot/comparePivot since
  // quicksort relies on these methods to be fast for good performance

protected:
  void setPivot(int i) = 0;

  int comparePivot(int j) = 0;

  int compare(int i, int j) override;

protected:
  std::shared_ptr<IntroSorter> shared_from_this()
  {
    return std::static_pointer_cast<IntroSorter>(Sorter::shared_from_this());
  }
};

CL_NS_END