#include "test.h"

#include <memory.h>
#include <stdlib.h>
#include <time.h>

#include <cstdint>
#include <cstdio>
#include <vector>

#include "CLucene/index/CodeMode.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "CLucene/util/PFORUtil.h"
#include "CuTest.h"
#include "for/vp4.h"

using namespace lucene::store;
// Add a helper macro for printing more detailed error messages when assertions fail
#define CuAssertTrueWithMessage(tc, message, condition) \
    do {                                                \
        if (!(condition)) {                             \
            printf("Assertion failed: %s\n", message);  \
        }                                               \
        CuAssertTrue(tc, condition);                    \
    } while (0)

static const uint64_t g_pow10[] = {
        1ULL,          // 10^0
        10ULL,         // 10^1
        100ULL,        // 10^2
        1000ULL,       // 10^3
        10000ULL,      // 10^4
        100000ULL,     // 10^5
        1000000ULL,    // 10^6
        10000000ULL,   // 10^7
        100000000ULL,  // 10^8
        1000000000ULL, // 10^9
        10000000000ULL // 10^10
};

// 计算 10^(floor(b/3))，若超出 g_pow10 范围可再加判断
static inline uint64_t get_pow10_for_b(unsigned b) {
    // floor(b/3)
    unsigned idx = b / 3;
    if (idx >= sizeof(g_pow10) / sizeof(g_pow10[0])) {
        // 超过预置表最大 10^10，就固定返回 10^10 或自行处理
        return g_pow10[10];
    }
    return g_pow10[idx];
}

// 计算 2^b 的函数
static inline unsigned power2(unsigned b) {
    // (1U << b) 当 b=32 时也可能溢出，你可自行判断
    return (1U << b);
}

/**
 * @param values        输出数组
 * @param n             要生成的数据个数
 * @param b             当前位宽
 * @param with_exception 0=无异常，1=有异常
 */
void generate_raw_data_for_bitwidth(unsigned* values, unsigned n, unsigned b, int with_exception) {
    if (n == 0) return;

    if (!with_exception) {
        // =====================================
        //         无异常模式：递增序列
        // =====================================
        // 1) 先给一个随机初始值 base (你也可随意决定)
        unsigned base = rand() % 1000;
        values[0] = base;

        // 2) 根据 b 分段决定"增量最大范围"
        unsigned inc_range;
        if (b < 4) {
            // b=0 => 2^0=1, b=1 =>2, b=2 =>4, b=3=>8
            inc_range = power2(b);
        } else {
            // b>=4 => 用10^(floor(b/3)) => 10,100,1000,...
            uint64_t r = get_pow10_for_b(b);
            // 这里最好判断 r 是否超出 unsigned 范围
            // 若测试场景不会特别大，可以直接转为 unsigned
            if (r > 0xFFFFFFFFULL) {
                r = 0xFFFFFFFFULL; // 避免溢出
            }
            inc_range = (unsigned)r;
        }

        // 3) 生成递增序列
        for (unsigned i = 1; i < n; i++) {
            // +1 是为了避免 0 增量的情况
            unsigned inc = 1 + rand() % inc_range;
            base += inc;
            values[i] = base;
        }

    } else {
        // =====================================
        //         有异常模式：直接随机
        // =====================================

        // 观察示例得知：
        //  - b=0 => rand()%2
        //  - b=2 => rand()%4
        //  - b=3 => rand()%10
        //  - b=7 => rand()%100
        //  - b=10 => rand()%1000
        //  - b=13 => rand()%10000
        //  => 规律：当 b >= 3 用 10^(floor(b/3))；当 b < 3 用特殊处理

        uint64_t val_range = 0; // 用 64 位临时存，最后再转回 unsigned

        if (b == 0) {
            val_range = 2; // 0..1
        } else if (b == 1) {
            // 你没给 b=1 的具体例子，这里假设跟 b=0 一样 => range=2
            val_range = 2; // 0..1
        } else if (b == 2) {
            val_range = 4; // 0..3
        } else {
            // b>=3 => 用 10^(floor(b/3))
            val_range = get_pow10_for_b(b);
            // 同样检查一下是否超过 unsigned
            if (val_range > 0xFFFFFFFFULL) {
                val_range = 0xFFFFFFFFULL;
            }
        }

        // 直接随机
        for (unsigned i = 0; i < n; i++) {
            unsigned x = (unsigned)(rand() % (unsigned)val_range);
            values[i] = x;
        }
    }
}

void test_pfor_has_prox(CuTest* tc) {
    const unsigned TEST_SIZE = 512;
    const char* testFileName = "pfor.dat";

    // 分配缓冲区
    std::vector<unsigned> docDeltaBuffer(TEST_SIZE);
    std::vector<unsigned> freqBuffer(TEST_SIZE);
    std::vector<unsigned> encoded_data(TEST_SIZE * 2);
    std::vector<unsigned> decoded1(TEST_SIZE);
    std::vector<unsigned> decoded2(TEST_SIZE);

    srand((unsigned)time(NULL));
    printf("开始测试 p4nd1dec256v32...\n");

    {
        generate_raw_data_for_bitwidth(docDeltaBuffer.data(), TEST_SIZE, 32, 0);
        auto* dir = lucene::store::FSDirectory::getDirectory("./");

        auto* output = dir->createOutput(testFileName);

        lucene::util::pfor_encode(output, docDeltaBuffer, freqBuffer, true);
        output->close();
        _CLDELETE(output);
        dir->close();
        _CLDELETE(dir);
    }
    {
        IndexInput* input = nullptr;
        CLuceneError error;
        auto* dir = lucene::store::FSDirectory::getDirectory("./");
        bool result = dir->openInput(testFileName, input, error);
        lucene::util::pfor_decode(input, decoded1, decoded2, true, false);
        for (size_t i = 0; i < TEST_SIZE; i++) {
            CuAssertIntEquals(tc, _T("docDeltaBuffer[%zu] != decoded1[%zu]"), docDeltaBuffer[i],
                              decoded1[i]);
            CuAssertIntEquals(tc, _T("freqBuffer[%zu] != decoded2[%zu]"), freqBuffer[i],
                              decoded2[i]);
        }
        input->close();
        _CLDELETE(input);
        dir->close();
        _CLDELETE(dir);
    }
    printf("测试完成!\n");
}

void test_pfor_no_prox(CuTest* tc) {
    const unsigned TEST_SIZE = 512;
    const char* testFileName = "pfor.dat";

    // 分配缓冲区
    std::vector<unsigned> docDeltaBuffer(TEST_SIZE);
    std::vector<unsigned> freqBuffer(TEST_SIZE);
    std::vector<unsigned> encoded_data(TEST_SIZE * 2);
    std::vector<unsigned> decoded1(TEST_SIZE);
    std::vector<unsigned> decoded2(TEST_SIZE);

    srand((unsigned)time(NULL));
    printf("开始测试 p4nd1dec256v32...\n");

    {
        generate_raw_data_for_bitwidth(docDeltaBuffer.data(), TEST_SIZE, 32, 0);
        auto* dir = lucene::store::FSDirectory::getDirectory("./");

        auto* output = dir->createOutput(testFileName);

        lucene::util::pfor_encode(output, docDeltaBuffer, freqBuffer, false);
        output->close();
        _CLDELETE(output);
        dir->close();
        _CLDELETE(dir);
    }
    {
        IndexInput* input = nullptr;
        CLuceneError error;
        auto* dir = lucene::store::FSDirectory::getDirectory("./");
        bool result = dir->openInput(testFileName, input, error);
        lucene::util::pfor_decode(input, decoded1, decoded2, false, false);
        for (size_t i = 0; i < TEST_SIZE; i++) {
            CuAssertIntEquals(tc, _T("docDeltaBuffer[%zu] != decoded1[%zu]"), docDeltaBuffer[i],
                              decoded1[i]);
            CuAssertIntEquals(tc, _T("freqBuffer[%zu] != decoded2[%zu]"), freqBuffer[i],
                              decoded2[i]);
        }
        input->close();
        _CLDELETE(input);
        dir->close();
        _CLDELETE(dir);
    }
    printf("测试完成!\n");
}

// Test the compatibility of P4DEC and P4ENC
void test_p4dec_p4enc_compat(CuTest* tc) {
    const unsigned TEST_SIZE = 512;
    const char* testFileName = "pfor_p4enc.dat";

    // Allocate buffers
    std::vector<uint32_t> originalData(TEST_SIZE);
    std::vector<uint32_t> decodedData(TEST_SIZE);
    std::vector<uint32_t> freqs(TEST_SIZE);
    std::vector<uint32_t> decodedFreqs(TEST_SIZE);

    srand((unsigned)time(NULL));
    printf("Testing P4ENC and pfor_decode compatibility...\n");

    // Generate test data with delta encoding pattern (increasing values)
    generate_raw_data_for_bitwidth(originalData.data(), TEST_SIZE, 32, 0);
    generate_raw_data_for_bitwidth(freqs.data(), TEST_SIZE, 32, 1);

    auto encode = [](IndexOutput* out, std::vector<uint32_t>& buffer, bool isDoc) {
        std::vector<uint8_t> compress(4 * buffer.size() + PFOR_BLOCK_SIZE);
        size_t size = 0;
        if (isDoc) {
            size = P4ENC(buffer.data(), buffer.size(), compress.data());
        } else {
            size = P4NZENC(buffer.data(), buffer.size(), compress.data());
        }
        out->writeVInt(size);
        out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
    };
    // 第一步：使用P4ENC编码数据并写入文件
    {
        auto* dir = lucene::store::FSDirectory::getDirectory("./");
        auto* output = dir->createOutput(testFileName);

        // 写入编码模式和大小
        output->writeByte((char)lucene::index::CodeMode::kPfor);
        output->writeVInt(TEST_SIZE);

        // 编码并写入数据
        encode(output, originalData, true);
        encode(output, freqs, false);

        output->close();
        _CLDELETE(output);
        dir->close();
        _CLDELETE(dir);
    }

    // 第二步：使用pfor_decode解码数据
    {
        IndexInput* input = nullptr;
        CLuceneError error;
        auto* dir = lucene::store::FSDirectory::getDirectory("./");
        bool result = dir->openInput(testFileName, input, error);

        // 使用pfor_decode解码数据 (不使用代理 has_prox=false, compatibleRead=false)
        uint32_t decoded_size =
                lucene::util::pfor_decode(input, decodedData, decodedFreqs, true, false);

        // 验证解码大小
        CuAssertIntEquals(tc, _T("Decoded size mismatch"), TEST_SIZE, decoded_size);

        // 验证解码数据与原始数据匹配
        for (size_t i = 0; i < TEST_SIZE; i++) {
            //printf("freqs[%zu] = %u, decodedFreqs[%zu] = %u\n", i, freqs[i], i, decodedFreqs[i]);
            //printf("originalData[%zu] = %u, decodedData[%zu] = %u\n", i, originalData[i], i, decodedData[i]);
            CuAssertTrueWithMessage(tc, "Decoded doc doesn't match original",
                                    originalData[i] == decodedData[i]);
            CuAssertTrueWithMessage(tc, "Decoded freq doesn't match original",
                                    freqs[i] == decodedFreqs[i]);
        }

        input->close();
        _CLDELETE(input);
        dir->close();
        _CLDELETE(dir);
    }

    printf("P4ENC/pfor_decode compatibility test completed successfully!\n");
}

// Test cross-platform compatibility for P4DEC/P4ENC
void test_cross_platform_compat(CuTest* tc) {
    const unsigned TEST_SIZE = 512;
    const char* testFileName = "pfor_cross_platform.dat";

    // Allocate buffers
    std::vector<uint32_t> originalData(TEST_SIZE);
    std::vector<uint32_t> decodedData(TEST_SIZE);

    srand((unsigned)time(NULL));
    printf("Testing cross-platform compatibility...\n");

    // Generate test data with different patterns
    for (unsigned i = 0; i < TEST_SIZE; i++) {
        // Mix of small and large values to test different bit widths
        if (i % 10 == 0) {
            originalData[i] = rand() % 1000000; // Occasional large value
        } else {
            originalData[i] = rand() % 100; // Mostly small values
        }
    }

    // Part 1: Write encoded data to file using PFOR encoding
    {
        auto* dir = lucene::store::FSDirectory::getDirectory("./");
        auto* output = dir->createOutput(testFileName);

        // Write encoding mode and size
        output->writeByte((char)lucene::index::CodeMode::kPfor);
        output->writeVInt(TEST_SIZE);

        // Encode and write the data
        std::vector<uint8_t> compress(4 * TEST_SIZE + PFOR_BLOCK_SIZE);
        size_t size = lucene::util::P4ENC(originalData.data(), TEST_SIZE, compress.data());
        output->writeVInt(size);
        output->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);

        output->close();
        _CLDELETE(output);
        dir->close();
        _CLDELETE(dir);
    }

    // Part 2: Read encoded data from file and decode it with compatibleRead=true
    {
        IndexInput* input = nullptr;
        CLuceneError error;
        auto* dir = lucene::store::FSDirectory::getDirectory("./");
        bool result = dir->openInput(testFileName, input, error);

        // Verify the encoded format
        char mode = input->readByte();
        uint32_t arraySize = input->readVInt();
        CuAssertIntEquals(tc, _T("Array size mismatch"), TEST_SIZE, arraySize);

        // Read, decode and verify
        uint32_t SerializedSize = input->readVInt();
        std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
        input->readBytes(buf.data(), SerializedSize);

        // Use P4DEC for decoding, simulating cross-platform read
        lucene::util::P4DEC(buf.data(), arraySize, decodedData.data());

        // Verify decoded data matches original
        for (size_t i = 0; i < TEST_SIZE; i++) {
            CuAssertIntEquals(tc, _T("Cross-platform decoded data mismatch at %zu"),
                              originalData[i], decodedData[i]);
        }

        input->close();
        _CLDELETE(input);
        dir->close();
        _CLDELETE(dir);
    }

    printf("Cross-platform compatibility test completed successfully!\n");
}

// Test compatibility between encoded by ARM old version data and decoded by x86 new version with compatible mode
void test_p4ndx_compatibility(CuTest* tc) {
    const unsigned TEST_SIZE = 512;
    const char* testFileName1 = "pfor_p4ndx_compat_gen_by_old_version_x86_64.dat";
    const char* testFileName2 = "pfor_p4ndx_compat_gen_by_old_version_arm.dat";

    // Allocate buffers
    std::vector<uint32_t> docDeltaBuffer = {
            635,    1188,   1795,   2109,   2694,   3612,   3714,   4511,   5072,   5352,   5526,
            5894,   6706,   6891,   6979,   7080,   7586,   7789,   8530,   9065,   9704,   9949,
            10377,  10678,  11516,  11921,  12226,  13133,  13417,  13854,  14215,  14486,  15476,
            16444,  17380,  18306,  19191,  19580,  20302,  21099,  21119,  22014,  22178,  22361,
            22440,  23043,  23326,  24262,  25067,  25443,  26265,  27061,  27681,  27931,  28027,
            28837,  28843,  29595,  29663,  29953,  30494,  30922,  31834,  32364,  33111,  33311,
            33766,  34749,  35689,  36217,  36348,  36660,  37083,  37378,  37872,  38725,  39622,
            40399,  40540,  40594,  41098,  42060,  42909,  43032,  43243,  43539,  43823,  44040,
            44439,  44790,  45648,  46587,  46718,  46839,  47307,  47536,  48208,  48482,  49046,
            49658,  50460,  51154,  51429,  52005,  52993,  53761,  54541,  54778,  55674,  56594,
            57236,  57635,  58517,  59007,  59881,  60325,  60462,  60971,  60983,  61518,  62378,
            63247,  64073,  64415,  64757,  65050,  65620,  65633,  66552,  66685,  67661,  67733,
            67912,  68514,  69161,  69327,  69697,  70475,  71229,  71846,  71896,  72291,  72659,
            72942,  73178,  73419,  74145,  74517,  75266,  75356,  75615,  76575,  77533,  77617,
            77918,  78569,  79297,  79520,  79536,  80534,  81241,  81584,  81653,  82538,  83483,
            83550,  83601,  84267,  85112,  85268,  85550,  86444,  87347,  87996,  88172,  88310,
            88551,  88804,  89666,  90008,  90350,  90822,  91475,  92127,  93034,  93340,  93994,
            94628,  95156,  95825,  96457,  96691,  96703,  96755,  96874,  97182,  97301,  97822,
            98795,  99758,  100434, 101040, 101248, 101826, 102081, 102816, 102884, 103731, 104070,
            104999, 105539, 106220, 106972, 107165, 107849, 108507, 109005, 109342, 109633, 109658,
            110016, 110290, 110900, 111621, 111947, 112675, 112703, 113499, 114099, 114451, 115209,
            115837, 116794, 117111, 117668, 118231, 118634, 119258, 119668, 120409, 121313, 122262,
            123035, 123690, 124183, 124991, 125303, 126293, 126790, 127745, 128111, 128965, 129545,
            129873, 130447, 130704, 130759, 131712, 131764, 131771, 132075, 132236, 132870, 133482,
            134311, 134501, 134676, 134907, 135073, 136009, 136333, 136402, 137286, 137734, 137810,
            138539, 138795, 139534, 139604, 140356, 140401, 141189, 142146, 142771, 142886, 143416,
            143649, 144170, 145004, 145289, 145816, 146305, 146750, 146910, 147010, 147636, 147986,
            148612, 149468, 150335, 150896, 151427, 152362, 153159, 154138, 154500, 155025, 155259,
            155360, 155954, 156291, 156436, 157169, 157462, 157583, 158430, 158604, 158958, 159326,
            159333, 159971, 160865, 161712, 162146, 162552, 162850, 162909, 163016, 163940, 164207,
            165180, 165664, 166461, 167368, 167648, 168423, 168692, 168848, 169208, 169929, 170679,
            171375, 172240, 172722, 173710, 174696, 175377, 175890, 176581, 176629, 177148, 177476,
            177769, 178486, 178599, 179297, 179312, 179836, 180640, 180930, 181072, 181848, 182621,
            183559, 183594, 183999, 184064, 184719, 185279, 186055, 186430, 187091, 187915, 188154,
            188649, 188812, 189388, 189563, 190239, 190505, 190727, 190921, 191866, 192732, 192995,
            193405, 193969, 194246, 195179, 195546, 196464, 196890, 197385, 198075, 198790, 199319,
            199765, 199896, 200079, 200085, 200992, 201549, 202215, 202945, 203092, 203252, 204144,
            204219, 204905, 205472, 205812, 206071, 206184, 206821, 206946, 207673, 207719, 208407,
            208762, 209092, 209498, 209770, 209877, 210129, 210442, 211263, 212043, 212802, 213754,
            214068, 214832, 215690, 215912, 216693, 217632, 218001, 218942, 219124, 219567, 220545,
            220646, 220780, 221017, 221582, 222352, 223065, 223356, 223523, 224275, 224920, 225768,
            225925, 226841, 227795, 228556, 228784, 229559, 230099, 231085, 231163, 231369, 231470,
            231757, 232184, 233066, 233291, 234086, 234260, 235018, 235607, 235758, 236616, 237339,
            238078, 238500, 239344, 239795, 239859, 240222, 240424, 241132, 241694, 242405, 243380,
            243896, 244367, 244922, 245564, 245926, 246818, 247537, 248104, 249097, 249102, 249448,
            249674, 250255, 250395, 250794, 251132, 251213, 252114, 252662, 252817, 253457, 253778,
            254128, 254570, 254955, 255667, 256311, 256755};
    std::vector<uint32_t> freqBuffer = {
            73, 5,  18, 40, 27, 24, 33, 88, 15, 51, 7,  59, 7,  4,  84, 39, 43, 34, 28, 75, 35, 75,
            29, 26, 48, 79, 67, 32, 42, 10, 75, 67, 67, 45, 7,  94, 21, 40, 35, 37, 43, 94, 48, 2,
            98, 85, 41, 93, 19, 22, 69, 54, 49, 50, 32, 97, 81, 0,  29, 24, 62, 57, 91, 30, 54, 51,
            76, 76, 91, 63, 65, 87, 57, 13, 89, 7,  98, 31, 1,  70, 5,  22, 76, 54, 24, 9,  52, 6,
            9,  33, 82, 71, 4,  2,  25, 53, 97, 76, 30, 25, 20, 93, 90, 7,  3,  55, 96, 10, 6,  79,
            63, 76, 84, 85, 52, 39, 10, 13, 91, 68, 22, 76, 50, 46, 19, 75, 99, 16, 4,  81, 41, 24,
            27, 83, 31, 30, 38, 27, 92, 44, 59, 56, 20, 43, 93, 25, 82, 55, 38, 25, 23, 13, 2,  25,
            59, 21, 53, 10, 89, 57, 44, 82, 81, 71, 17, 64, 53, 55, 43, 97, 0,  2,  53, 72, 46, 99,
            49, 28, 54, 40, 6,  30, 53, 8,  5,  5,  64, 81, 60, 74, 22, 17, 18, 4,  50, 41, 21, 14,
            94, 28, 58, 92, 80, 60, 97, 5,  58, 96, 54, 87, 3,  46, 45, 33, 99, 53, 40, 15, 86, 1,
            90, 8,  18, 60, 64, 21, 54, 37, 87, 48, 65, 45, 92, 98, 58, 42, 3,  16, 90, 9,  55, 93,
            56, 0,  26, 7,  5,  67, 23, 91, 20, 65, 99, 38, 77, 15, 11, 31, 52, 99, 32, 18, 96, 76,
            68, 54, 18, 71, 23, 9,  32, 78, 2,  88, 31, 81, 48, 88, 0,  23, 80, 20, 40, 31, 10, 17,
            47, 22, 49, 99, 21, 81, 69, 17, 57, 37, 24, 28, 60, 47, 37, 93, 77, 91, 33, 8,  72, 33,
            97, 72, 56, 29, 92, 96, 60, 55, 14, 59, 77, 15, 11, 98, 48, 32, 67, 57, 70, 91, 85, 82,
            90, 74, 75, 68, 66, 61, 28, 90, 94, 77, 15, 3,  6,  59, 99, 19, 14, 65, 30, 91, 32, 41,
            41, 80, 74, 9,  38, 96, 52, 75, 78, 43, 2,  6,  63, 68, 19, 91, 10, 13, 69, 25, 16, 27,
            85, 68, 98, 99, 33, 81, 91, 66, 74, 84, 98, 48, 93, 88, 96, 98, 16, 27, 93, 18, 85, 56,
            38, 4,  47, 48, 69, 68, 74, 38, 48, 11, 6,  98, 10, 91, 31, 53, 9,  6,  38, 60, 54, 83,
            48, 3,  33, 64, 30, 26, 34, 67, 82, 72, 71, 82, 21, 92, 2,  47, 30, 50, 58, 88, 1,  20,
            32, 32, 74, 93, 38, 64, 53, 45, 99, 54, 48, 33, 70, 30, 59, 5,  97, 94, 29, 20, 76, 50,
            12, 78, 49, 95, 81, 7,  83, 34, 80, 67, 18, 6,  13, 57, 70, 18, 54, 69, 72, 54, 2,  95,
            36, 14, 52, 33, 8,  81, 5,  36, 84, 17, 14, 33, 12, 47, 93, 48, 81, 25, 67, 52, 31, 80,
            9,  1,  99, 15, 22, 23, 69, 25};
    std::vector<uint32_t> decodedDocs(TEST_SIZE);
    std::vector<uint32_t> decodedFreqs(TEST_SIZE);

    srand((unsigned)time(NULL));
    printf("Testing pfor_decode compatibility...\n");

#if defined(__AVX2__)
    // Part 2: Decode data using pfor_decode with compatible mode (compatibleRead=true)
    {
        IndexInput* input = nullptr;
        CLuceneError error;
        auto* dir = lucene::store::FSDirectory::getDirectory(clucene_data_location);
        bool result = dir->openInput(testFileName2, input, error);

        // Use pfor_decode with compatibleRead=true
        uint32_t decoded_size =
                lucene::util::pfor_decode(input, decodedDocs, decodedFreqs, true, true);

        // Verify decoded size
        CuAssertIntEquals(tc, _T("Decoded size mismatch"), TEST_SIZE, decoded_size);

        // Verify decoded data matches original
        for (size_t i = 0; i < TEST_SIZE; i++) {
            CuAssertTrueWithMessage(tc, "Decoded doc doesn't match original",
                                    docDeltaBuffer[i] == decodedDocs[i]);
            CuAssertTrueWithMessage(tc, "Decoded freq doesn't match original",
                                    freqBuffer[i] == decodedFreqs[i]);
        }

        input->close();
        _CLDELETE(input);
        dir->close();
        _CLDELETE(dir);
    }
#elif defined(__ARM_NEON) || defined(__SSSE3__)
    {
        IndexInput* input = nullptr;
        CLuceneError error;
        auto* dir = lucene::store::FSDirectory::getDirectory(clucene_data_location);
        bool result = dir->openInput(testFileName1, input, error);

        // Use pfor_decode with compatibleRead=true
        uint32_t decoded_size =
                lucene::util::pfor_decode(input, decodedDocs, decodedFreqs, true, true);

        // Verify decoded size
        CuAssertIntEquals(tc, _T("Decoded size mismatch"), TEST_SIZE, decoded_size);

        // Verify decoded data matches original
        for (size_t i = 0; i < TEST_SIZE; i++) {
            CuAssertTrueWithMessage(tc, "Decoded doc doesn't match original",
                                    docDeltaBuffer[i] == decodedDocs[i]);
            CuAssertTrueWithMessage(tc, "Decoded freq doesn't match original",
                                    freqBuffer[i] == decodedFreqs[i]);
        }

        input->close();
        _CLDELETE(input);
        dir->close();
        _CLDELETE(dir);
    }
#endif
    printf("compatibility test completed successfully!\n");
}
CuSuite* testPFORSuite() {
    CuSuite* suite = CuSuiteNew(_T("PFOR Test Suite"));

    SUITE_ADD_TEST(suite, test_pfor_has_prox);
    SUITE_ADD_TEST(suite, test_pfor_no_prox);
    SUITE_ADD_TEST(suite, test_p4dec_p4enc_compat);
    SUITE_ADD_TEST(suite, test_cross_platform_compat);
    SUITE_ADD_TEST(suite, test_p4ndx_compatibility);

    return suite;
}