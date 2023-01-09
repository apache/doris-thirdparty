#pragma once
#include <deque>
#include <limits>
#include <memory>
#include <unordered_set>

// C++ NOTE: Forward class declarations:
#include "CLucene/util/BytesRef.h"
#include "CLucene/util/MSBRadixSorter.h"
#include "test.h"

using namespace std;
using namespace lucene::util;
class TestMSBRadixSorter : public enable_shared_from_this<TestMSBRadixSorter> {
public:
    TestMSBRadixSorter(CuTest *tc):tc(tc){}
    ~TestMSBRadixSorter() = default;

private:
    CuTest *tc;

public:
    void test(std::vector<BytesRef> &refs, std::vector<BytesRef> &refs2, int len);

private:
    class MSBRadixSorterAnonymousInnerClass : public MSBRadixSorter {
    private:
        std::shared_ptr<TestMSBRadixSorter> outerInstance;

        std::vector<BytesRef>* refs;
        int finalMaxLength = 0;

    public:
        MSBRadixSorterAnonymousInnerClass(
                std::shared_ptr<TestMSBRadixSorter> outerInstance, int maxLength,
                std::vector<BytesRef> *refs,
                int finalMaxLength);

    protected:
        int byteAt(int i, int k) override;

        void swap(int i, int j) override;

    protected:
        std::shared_ptr<MSBRadixSorterAnonymousInnerClass> shared_from_this() {
            return std::static_pointer_cast<MSBRadixSorterAnonymousInnerClass>(
                    MSBRadixSorter::shared_from_this());
        }
    };

public:
    //virtual void testEmpty();

    virtual void testOneValue();

    virtual void testNValues();

/*private:
    void testRandom(int commonPrefixLen, int maxLen);

public:
    virtual void testRandom();

    virtual void testRandomWithLotsOfDuplicates();

    virtual void testRandomWithSharedPrefix();

    virtual void testRandomWithSharedPrefixAndLotsOfDuplicates();

    virtual void testRandom2();*/
};
