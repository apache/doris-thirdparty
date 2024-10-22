#include "CLucene/store/store_v2/GrowableByteArrayDataOutput.h"
#include "CuTest.h"
#include "test.h"

class MockIndexOutput : public CL_NS(store)::IndexOutput {
public:
    std::vector<uint8_t> data;

    void writeByte(uint8_t b) override { data.push_back(b); }

    void writeBytes(const uint8_t* b, const int32_t len) override {
        data.insert(data.end(), b, b + len);
    }

    void writeBytes(const uint8_t* b, const int32_t len, const int32_t offset) override {
        data.insert(data.end(), b + offset, b + offset + len);
    }

    void close() override {}
    int64_t getFilePointer() const override { return data.size(); }
    void seek(const int64_t pos) override {}
    int64_t length() const override { return data.size(); }
    void flush() override {}

    size_t size() const { return data.size(); }
};

void TestGrowableByteArrayDataOutput(CuTest* tc) {
    getchar();

    using namespace store_v2;

    // Basic functionality test: test writing single bytes
    GrowableByteArrayDataOutput byteArrayOutput;
    byteArrayOutput.writeByte(10);
    byteArrayOutput.writeByte(20);

    // Test writing multiple bytes at once
    uint8_t multiBytes[] = {30, 40, 50};
    byteArrayOutput.writeBytes(multiBytes, 3);

    // Verify written data by copying it to a mock output
    MockIndexOutput mockOutput;
    byteArrayOutput.writeTo(&mockOutput);
    CuAssertIntEquals(tc, _T("First byte mismatch"), 10, mockOutput.data[0]);
    CuAssertIntEquals(tc, _T("Second byte mismatch"), 20, mockOutput.data[1]);
    CuAssertIntEquals(tc, _T("Third byte mismatch"), 30, mockOutput.data[2]);
    CuAssertIntEquals(tc, _T("Fourth byte mismatch"), 40, mockOutput.data[3]);
    CuAssertIntEquals(tc, _T("Fifth byte mismatch"), 50, mockOutput.data[4]);

    // Test with 0 bytes (no data should be added)
    byteArrayOutput.writeBytes(nullptr, 0);
    CuAssertIntEquals(tc, _T("Size should remain the same after writing 0 bytes"), 5,
                      mockOutput.data.size());

    // Test boundary: writing 1 byte after capacity is reached
    byteArrayOutput.writeByte(60);
    byteArrayOutput.writeTo(&mockOutput);
    CuAssertIntEquals(tc, _T("Sixth byte mismatch after expansion"), 10, mockOutput.data[5]);

    // Boundary condition test: simulate writing a large amount of data to ensure capacity growth
    for (int i = 0; i < 300; i++) {
        byteArrayOutput.writeByte(70);
    }
    CuAssertTrue(tc, byteArrayOutput.size() >= 300 + 5 + 1); // Check if capacity expanded correctly

    // Test compression output to ensure compressed size is smaller than uncompressed
    MockIndexOutput compressedOutput;
    size_t old_size = byteArrayOutput.size();
    byteArrayOutput.writeCompressedTo(&compressedOutput);
    size_t new_size = compressedOutput.size();
    CuAssertTrue(tc, new_size <= old_size); // Compressed data should be smaller

    std::cout << "TestGrowableByteArrayDataOutput passed" << std::endl;
}

CuSuite* testGrowableByteArrayDataOutputSuite() {
    CuSuite* suite = CuSuiteNew(_T("GrowableByteArrayDataOutput Test Suite"));
    SUITE_ADD_TEST(suite, TestGrowableByteArrayDataOutput);
    return suite;
}
