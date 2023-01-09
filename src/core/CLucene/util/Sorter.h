#pragma once
#include "CLucene/_ApiHeader.h"
#include <memory>

#include <stdexcept>

CL_NS_DEF(util)

/** Base class for sorting algorithms implementations.
 * @lucene.internal */
class Sorter : public std::enable_shared_from_this<Sorter> {
public:
    static constexpr int BINARY_SORT_THRESHOLD = 20;

    /** Sole constructor, used for inheritance. */
protected:
    Sorter();

    /** Compare entries found in slots <code>i</code> and <code>j</code>.
   *  The contract for the returned value is the same as
   *  {@link Comparator#compare(Object, Object)}. */
    virtual int compare(int i, int j) = 0;

    /** Swap values at slots <code>i</code> and <code>j</code>. */
    virtual void swap(int i, int j) = 0;

private:
    int pivotIndex = 0;

    /** Save the value at slot <code>i</code> so that it can later be used as a
   * pivot, see {@link #comparePivot(int)}. */
protected:
    virtual void setPivot(int i);

    /** Compare the pivot with the slot at <code>j</code>, similarly to
   *  {@link #compare(int, int) compare(i, j)}. */
    virtual int comparePivot(int j);

    /** Sort the slice which starts at <code>from</code> (inclusive) and ends at
   *  <code>to</code> (exclusive). */
public:
    virtual void sort(int from, int to) = 0;

    virtual void checkRange(int from, int to);

    virtual void mergeInPlace(int from, int mid, int to);

    virtual int lower(int from, int to, int val);

    virtual int upper(int from, int to, int val);

    // faster than lower when val is at the end of [from:to[
    virtual int lower2(int from, int to, int val);

    // faster than upper when val is at the beginning of [from:to[
    virtual int upper2(int from, int to, int val);

    void reverse(int from, int to);

    void rotate(int lo, int mid, int hi);

    virtual void doRotate(int lo, int mid, int hi);

    /**
   * A binary sort implementation. This performs {@code O(n*log(n))} comparisons
   * and {@code O(n^2)} swaps. It is typically used by more sophisticated
   * implementations as a fall-back when the numbers of items to sort has become
   * less than {@value #BINARY_SORT_THRESHOLD}.
   */
    virtual void binarySort(int from, int to);

    virtual void binarySort(int from, int to, int i);

    /**
   * Use heap sort to sort items between {@code from} inclusive and {@code to}
   * exclusive. This runs in {@code O(n*log(n))} and is used as a fall-back by
   * {@link IntroSorter}.
   */
    virtual void heapSort(int from, int to);

    virtual void heapify(int from, int to);

    virtual void siftDown(int i, int from, int to);

    static int heapParent(int from, int i);

    static int heapChild(int from, int i);
};
CL_NS_END