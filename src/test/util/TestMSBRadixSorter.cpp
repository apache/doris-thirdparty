#include "TestMSBRadixSorter.h"
#include "CLucene/util/NumericUtils.h"
#include "test.h"

#include <algorithm>
#include <random>
void TestMSBRadixSorter::test(std::vector<BytesRef> &refs,
                              std::vector<BytesRef> &refs2,
                              int len) {
    std::vector<BytesRef> expected;

    for (auto ref : refs) {
        //printf("expected %x %x\n",ref.bytes.at(2), ref.bytes.at(3));
        expected.push_back(ref);
    }
    //std::sort(expected.begin(), expected.end());

    int maxLength = 0;
    for (int i = 0; i < len; ++i) {
        auto ref = refs[i];
        maxLength = max(maxLength, ref.length);
    }

    int finalMaxLength = maxLength;
    make_shared<MSBRadixSorterAnonymousInnerClass>(shared_from_this(), maxLength,
                                                   &refs2, finalMaxLength)->sort(0, len);
    std::vector<std::shared_ptr<BytesRef>> actual;
    for (const auto& ref : refs2) {
        //printf("actual %x %x\n",ref.bytes.at(2), ref.bytes.at(3));
        actual.push_back(std::make_shared<BytesRef>(ref));
    }
    CuAssertEquals(tc, actual.size(), expected.size());

    for (auto i=0; i< actual.size();i++) {
        CuAssertEquals(tc, actual[i]->bytes.size(), expected[i].bytes.size());
        //printf("%x  %x\n",actual[i]->bytes.at(3), expected[i].bytes.at(3));
        CuAssertEquals(tc,actual[i]->bytes.at(3), expected[i].bytes.at(3));
    }


    //assertArrayEquals(expected, actual);
}

TestMSBRadixSorter::MSBRadixSorterAnonymousInnerClass::
        MSBRadixSorterAnonymousInnerClass(
                shared_ptr<TestMSBRadixSorter> outerInstance, int maxLength,
                vector<BytesRef> *refs,
                int finalMaxLength)
    : MSBRadixSorter(maxLength)
{
    this->outerInstance = outerInstance;
    this->refs = refs;
    this->finalMaxLength = finalMaxLength;
}

int TestMSBRadixSorter::MSBRadixSorterAnonymousInnerClass::byteAt(int i, int k)
{
    auto ref = (*refs)[i];
    if (ref.length <= k) {
        return -1;
    }
    return ref.bytes.at(ref.offset + k) & 0xff;
}

void TestMSBRadixSorter::MSBRadixSorterAnonymousInnerClass::swap(int i, int j)
{
    //printf("swap %d %d\n",i,j);
    /*if (i >= refs.size()||j >= refs.size()){
        printf("bang\n");
        return;
    }*/
    auto tmp = (*refs)[i];
    (*refs)[i] = (*refs)[j];
    (*refs)[j] = tmp;
}

void TestMSBRadixSorter::testOneValue()
{
    std::vector<uint8_t> scratch(4);
    NumericUtils::intToSortableBytes(1, scratch, 0);

    BytesRef x1(scratch);

    std::vector<BytesRef> y;
    y.emplace_back(x1);
    test(y, y, 1);
}

void TestMSBRadixSorter::testNValues()
{
    const int n = 1000;
    auto y = std::vector<BytesRef>();
    auto z = std::vector<BytesRef>();

    for (int docID = 0; docID < n; docID++) {
        std::vector<uint8_t> scratch(4);
        NumericUtils::intToSortableBytes(docID, scratch, 0);
        y.emplace_back(scratch);
    }
    for (int docID = n-1; docID >= 0; docID--) {
        std::vector<uint8_t> scratch(4);
        NumericUtils::intToSortableBytes(docID, scratch, 0);
        z.emplace_back(scratch);
    }

    test(y, z, n);
}

void testSorter(CuTest *tc) {
    auto test = std::make_shared<TestMSBRadixSorter>(tc);
    test->testOneValue();
    test->testNValues();
}

CuSuite *testMSBRadixSorter() {
    CuSuite *suite = CuSuiteNew(_T("CLucene MSBRadixSorter Test"));

    SUITE_ADD_TEST(suite, testSorter);

    return suite;
}