#include "CLucene/store/store_v2/ByteArrayDataInput.h"
#include "CuTest.h"
#include "test.h"

void TestByteArrayDataInput(CuTest* tc) {
    using namespace store_v2;

    // Basic functionality test: test reading single bytes
    std::vector<uint8_t> sampleData = {1, 2, 3, 4, 5};
    ByteArrayDataInput byteArrayInput(&sampleData);
    CuAssertIntEquals(tc, _T("First byte mismatch"), 1, byteArrayInput.readByte());
    CuAssertIntEquals(tc, _T("Second byte mismatch"), 2, byteArrayInput.readByte());

    // Test reading multiple bytes into a buffer
    uint8_t buffer[3];
    byteArrayInput.readBytes(buffer, 3);
    CuAssertIntEquals(tc, _T("Buffer first element mismatch"), 3, buffer[0]);
    CuAssertIntEquals(tc, _T("Buffer second element mismatch"), 4, buffer[1]);
    CuAssertIntEquals(tc, _T("Buffer third element mismatch"), 5, buffer[2]);

    // Test EOF detection
    CuAssertTrue(tc, byteArrayInput.eof());

    // Reset input and retest reading
    byteArrayInput.reset(&sampleData);
    CuAssertIntEquals(tc, _T("Reset first byte mismatch"), 1, byteArrayInput.readByte());

    // Test unsupported operations to ensure exceptions are thrown
    bool exceptionThrown = false;
    try {
        byteArrayInput.length();
    } catch (...) {
        exceptionThrown = true;
    }
    CuAssertTrue(tc, exceptionThrown);

    exceptionThrown = false;
    try {
        byteArrayInput.seek(0);
    } catch (...) {
        exceptionThrown = true;
    }
    CuAssertTrue(tc, exceptionThrown);

    // Boundary condition tests
    // 0 bytes
    std::vector<uint8_t> emptyData;
    byteArrayInput.reset(&emptyData);
    CuAssertTrue(tc, byteArrayInput.eof());

    // 1 byte
    std::vector<uint8_t> oneByteData = {42};
    byteArrayInput.reset(&oneByteData);
    CuAssertTrue(tc, !byteArrayInput.eof());
    CuAssertIntEquals(tc, _T("Single byte mismatch"), 42, byteArrayInput.readByte());
    CuAssertTrue(tc, byteArrayInput.eof());

    // capacity bytes (assuming capacity is 5)
    int capacity = 5;
    std::vector<uint8_t> capacityData = {10, 20, 30, 40, 50};
    byteArrayInput.reset(&capacityData);
    for (int i = 0; i < capacity; i++) {
        CuAssertIntEquals(tc, _T("Capacity byte mismatch"), capacityData[i],
                          byteArrayInput.readByte());
    }
    CuAssertTrue(tc, byteArrayInput.eof());

    // capacity - 1 bytes
    std::vector<uint8_t> capacityMinusOneData = {1, 2, 3, 4};
    byteArrayInput.reset(&capacityMinusOneData);
    for (int i = 0; i < capacity - 1; i++) {
        CuAssertIntEquals(tc, _T("Capacity - 1 byte mismatch"), capacityMinusOneData[i],
                          byteArrayInput.readByte());
    }
    CuAssertTrue(tc, byteArrayInput.eof());

    // capacity + 1 bytes
    std::vector<uint8_t> capacityPlusOneData = {5, 10, 15, 20, 25, 30};
    byteArrayInput.reset(&capacityPlusOneData);
    for (int i = 0; i < capacity + 1; i++) {
        CuAssertIntEquals(tc, _T("Capacity + 1 byte mismatch"), capacityPlusOneData[i],
                          byteArrayInput.readByte());
    }
    CuAssertTrue(tc, byteArrayInput.eof());

    std::cout << "TestByteArrayDataInput passed" << std::endl;
}

CuSuite* testByteArrayDataInputSuite() {
    CuSuite* suite = CuSuiteNew(_T("ByteArrayDataInput Test Suite"));
    SUITE_ADD_TEST(suite, TestByteArrayDataInput);
    return suite;
}
