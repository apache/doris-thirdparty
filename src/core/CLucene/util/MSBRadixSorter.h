#pragma once
#include "BytesRefBuilder.h"
#include "CLucene/SharedHeader.h"
#include "CLucene/util/Sorter.h"
#include "IntroSorter.h"

#include <vector>

CL_NS_DEF(util)

/** Radix sorter for variable-length strings. This class sorts based on the most
 *  significant byte first and falls back to {@link IntroSorter} when the size
 *  of the buckets to sort becomes small. It is <b>NOT</b> stable.
 *  Worst-case memory usage is about {@code 2.3 KB}.
 *  @lucene.internal */
class MSBRadixSorter : public Sorter{
    // after that many levels of recursion we fall back to introsort anyway
    // this is used as a protection against the fact that radix sort performs
    // worse when there are long common prefixes (probably because of cache
    // locality)
private:
    static constexpr int LEVEL_THRESHOLD = 8;
    // size of histograms: 256 + 1 to indicate that the string is finished
    static constexpr int HISTOGRAM_SIZE = 257;
    // buckets below this size will be sorted with introsort
    static constexpr int LENGTH_THRESHOLD = 100;

    // we store one histogram per recursion level
    std::vector<std::vector<int>> histograms =
            std::vector<std::vector<int>>(LEVEL_THRESHOLD);
    std::vector<int> endOffsets = std::vector<int>(HISTOGRAM_SIZE);
    std::vector<int> commonPrefix;

    const int maxLength;

    /**
   * Sole constructor.
   * @param maxLength the maximum length of keys, pass {@link Integer#MAX_VALUE}
   * if unknown.
   */
protected:
    explicit MSBRadixSorter(int maxLength);

    /** Return the k-th byte of the entry at index {@code i}, or {@code -1} if
   * its length is less than or equal to {@code k}. This may only be called
   * with a value of {@code i} between {@code 0} included and
   * {@code maxLength} excluded. */
    virtual int byteAt(int i, int k) = 0;

private:
    class IntroSorterAnonymousInnerClass : public IntroSorter {
    private:
        std::shared_ptr<MSBRadixSorter> outerInstance;

        int k = 0;

    public:
        IntroSorterAnonymousInnerClass(
                shared_ptr<MSBRadixSorter>&& outerInstance, int k);

    protected:
        void swap(int i, int j) override;

        int compare(int i, int j) override;

        void setPivot(int i) override;

        int comparePivot(int j) override;

    private:
        std::shared_ptr<BytesRefBuilder> pivot;

    protected:
        std::shared_ptr<IntroSorterAnonymousInnerClass> shared_from_this() {
            return std::static_pointer_cast<IntroSorterAnonymousInnerClass>(
                    IntroSorter::shared_from_this());
        }
    };

protected:
    int compare(int i, int j) final;

public:
    void sort(int from, int to) override;

private:
    void sort(int from, int to, int k, int l);

    void introSort(int from, int to, int k);

    /**
   * @param k the character number to compare
   * @param l the level of recursion
   */
    void radixSort(int from, int to, int k, int l);

    // only used from assert
    static bool assertHistogram(int commonPrefixLength, std::vector<int> &histogram);

    /** Return a number for the k-th character between 0 and {@link
   * #HISTOGRAM_SIZE}. */
    int getBucket(int i, int k);

    /** Build a histogram of the number of values per {@link #getBucket(int, int)
   * bucket} and return a common prefix length for all visited values.
   *  @see #buildHistogram */
    int computeCommonPrefixLengthAndBuildHistogram(int from, int to, int k,
                                                   std::vector<int> &histogram);

    /** Build an histogram of the k-th characters of values occurring between
   *  offsets {@code from} and {@code to}, using {@link #getBucket}. */
    void buildHistogram(int from, int to, int k, std::vector<int> &histogram);

    /** Accumulate values of the histogram so that it does not store counts but
   *  start offsets. {@code endOffsets} will store the end offsets. */
    static void sumHistogram(std::vector<int> &histogram,
                             std::vector<int> &endOffsets);

    /**
   * Reorder based on start/end offsets for each bucket. When this method
   * returns, startOffsets and endOffsets are equal.
   * @param startOffsets start offsets per bucket
   * @param endOffsets end offsets per bucket
   */
    void reorder(int from, int to, std::vector<int> &start,
                 std::vector<int> &end, int k);

protected:
    std::shared_ptr<MSBRadixSorter> shared_from_this() {
        return std::static_pointer_cast<MSBRadixSorter>(Sorter::shared_from_this());
    }
};

CL_NS_END