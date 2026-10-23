#include "test.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "CLucene/store/RAMDirectory.h"
#include "CLucene/util/PFORUtil.h"
#include "CLucene/util/PForCodec.h"
#include "CuTest.h"

using namespace lucene::store;
using namespace lucene::util;

#define CuAssertTrueWithMessage(tc, message, condition) \
    do {                                                \
        if (!(condition)) {                             \
            printf("Assertion failed: %s\n", message);  \
        }                                               \
        CuAssertTrue(tc, condition);                    \
    } while (0)

// ---------------------------------------------------------------------------------
// Tests of the block codec behind PforUtil
// ---------------------------------------------------------------------------------
namespace pfor = lucene::util::pfor;

using CodecEncodeFn = size_t (*)(const uint32_t*, size_t, uint8_t*);
using CodecDecodeFn = size_t (*)(const uint8_t*, const uint8_t*, size_t, uint32_t*);

struct CodecVariant {
    const char* name;
    CodecEncodeFn encode;
    CodecDecodeFn decode;
};

// The position is the variant id stored in pfor_codec_golden.dat.
static const CodecVariant kCodecVariants[] = {
        {"zigzag128", pfor::encodeZigzag128, pfor::decodeZigzag128},
        {"zigzag256", pfor::encodeZigzag256, pfor::decodeZigzag256},
        {"delta1_128", pfor::encodeDelta1_128, pfor::decodeDelta1_128},
        {"delta1_256", pfor::encodeDelta1_256, pfor::decodeDelta1_256},
};

// The arrays encoded in pfor_codec_golden.dat. Each id exercises other block types: all
// zero, constant, small gaps, sparse outliers, mixed bit widths, 24+ bit outliers, ...
static std::vector<uint32_t> goldenArray(unsigned id, size_t n) {
    static const unsigned kWidths[] = {1, 3, 7, 13, 21, 31};
    std::vector<uint32_t> a(n);
    uint32_t s = 12345 + id;
    uint32_t cur = 1000;
    for (size_t i = 0; i < n; ++i) {
        s = s * 1664525u + 1013904223u;
        switch (id) {
        case 0:
            a[i] = 0;
            break;
        case 1:
            a[i] = 7;
            break;
        case 2:
            cur += 1 + ((s >> 24) & 7);
            a[i] = cur;
            break;
        case 3:
            a[i] = 1 + ((s >> 24) % 5);
            break;
        case 4:
            cur += ((s >> 8) & 15) == 0 ? (s >> 8) : 1 + ((s >> 24) & 7);
            a[i] = cur;
            break;
        case 5:
            a[i] = s;
            break;
        case 6:
            a[i] = ((s >> 20) & 63) == 0 ? s : 3;
            break;
        case 7:
            a[i] = s >> (32 - kWidths[(i / 100) % 6]);
            break;
        case 8:
            a[i] = i % 150;
            break;
        case 9:
            cur += (s >> 29) == 0 ? s : 1 + (s >> 30);
            a[i] = cur;
            break;
        default: {
            // many medium and two huge zigzag deltas per 128 values
            const size_t m = i % 128;
            const uint32_t z = (m == 12 || m == 60) ? s : (m > 12 && m < 60) ? (s >> 20) & 4095 : 0;
            cur += (z >> 1) ^ (0u - (z & 1));
            a[i] = cur;
            break;
        }
        }
    }
    return a;
}

// Streams written by the previous implementation of the format must decode to the same values.
void test_pfor_codec_golden(CuTest* tc) {
    std::string path = std::string(clucene_data_location) + "/pfor_codec_golden.dat";
    FILE* f = fopen(path.c_str(), "rb");
    CuAssertTrue(tc, f != nullptr);
    std::vector<uint8_t> file;
    uint8_t chunk[4096];
    for (size_t got; (got = fread(chunk, 1, sizeof(chunk), f)) > 0;) {
        file.insert(file.end(), chunk, chunk + got);
    }
    fclose(f);
    const size_t fileBytes = file.size();
    file.resize(fileBytes + pfor::kDecodePadding); // the decoders may look past the stream

    CuAssertTrue(tc, fileBytes >= 8 && memcmp(file.data(), "PFG1", 4) == 0);
    uint32_t cases;
    memcpy(&cases, file.data() + 4, 4);
    size_t pos = 8;
    for (uint32_t c = 0; c < cases; ++c) {
        CuAssertTrue(tc, pos + 10 <= fileBytes);
        uint8_t variant = file[pos];
        uint8_t id = file[pos + 1];
        uint32_t n, len;
        memcpy(&n, file.data() + pos + 2, 4);
        memcpy(&len, file.data() + pos + 6, 4);
        pos += 10;
        CuAssertTrue(tc, variant < 4 && pos + len <= fileBytes);

        const CodecVariant& v = kCodecVariants[variant];
        std::vector<uint32_t> expected = goldenArray(id, n);
        std::vector<uint32_t> decoded(n);
        size_t used = v.decode(file.data() + pos, file.data() + pos + len, n, decoded.data());
        CuAssertTrueWithMessage(tc, v.name, used == len);
        CuAssertTrueWithMessage(tc, v.name, expected == decoded);
        pos += len;
    }
    CuAssertTrue(tc, pos == fileBytes);
}

void test_pfor_codec_roundtrip(CuTest* tc) {
    std::vector<size_t> sizes;
    for (size_t n = 1; n <= 40; ++n) sizes.push_back(n);
    for (size_t n : {100, 127, 128, 129, 130, 255, 256, 257, 258, 259, 300, 383, 384, 385, 511, 512,
                     513, 1000, 1025}) {
        sizes.push_back(n);
    }
    for (const CodecVariant& v : kCodecVariants) {
        for (unsigned id = 0; id <= 10; ++id) {
            for (size_t n : sizes) {
                std::vector<uint32_t> a = goldenArray(id, n);
                std::vector<uint8_t> enc(pfor::encodedSizeBound(n));
                size_t len = v.encode(a.data(), n, enc.data());
                CuAssertTrueWithMessage(tc, v.name, len <= enc.size());

                // decode from a buffer of exactly the encoded size
                enc.resize(len);
                enc.resize(len + pfor::kDecodePadding);
                std::vector<uint32_t> decoded(n);
                size_t used = v.decode(enc.data(), enc.data() + len, n, decoded.data());
                CuAssertTrueWithMessage(tc, v.name, used == len);
                CuAssertTrueWithMessage(tc, v.name, a == decoded);
            }
        }
    }
}

// Streams that end early or have invalid fields are reported instead of read past.
void test_pfor_codec_corrupt(CuTest* tc) {
    for (const CodecVariant& v : kCodecVariants) {
        for (unsigned id : {3u, 4u, 6u, 7u, 9u, 10u}) {
            const size_t n = 700;
            std::vector<uint32_t> a = goldenArray(id, n);
            std::vector<uint8_t> enc(pfor::encodedSizeBound(n));
            enc.resize(v.encode(a.data(), n, enc.data()));

            for (size_t cut : {size_t(0), size_t(1), enc.size() / 3, enc.size() - 1}) {
                std::vector<uint8_t> truncated(enc.begin(), enc.begin() + cut);
                truncated.resize(cut + pfor::kDecodePadding);
                std::vector<uint32_t> decoded(n);
                bool corrupt = false;
                try {
                    v.decode(truncated.data(), truncated.data() + cut, n, decoded.data());
                } catch (CLuceneError& e) {
                    corrupt = e.number() == CL_ERR_CorruptIndex;
                }
                CuAssertTrueWithMessage(tc, v.name, corrupt);
            }
        }
    }

    // a bit width above 32 in a block header
    uint8_t bad[3 + pfor::kDecodePadding] = {0x05, 0x3f, 0x00};
    uint32_t out[8];
    bool corrupt = false;
    try {
        pfor::decodeDelta1_128(bad, bad + 3, 4, out);
    } catch (CLuceneError& e) {
        corrupt = e.number() == CL_ERR_CorruptIndex;
    }
    CuAssertTrue(tc, corrupt);
}

// P4ENC / P4DEC and P4NZENC / P4NZDEC use the format of the platform they run on.
void test_p4_wrappers(CuTest* tc) {
    for (unsigned id : {2u, 3u, 4u, 6u, 7u}) {
        for (size_t n : {size_t(1), size_t(5), size_t(128), size_t(300), size_t(512)}) {
            std::vector<uint32_t> a = goldenArray(id, n);
            std::vector<uint8_t> enc(pfor::encodedSizeBound(n) + PFOR_BLOCK_SIZE);
            std::vector<uint32_t> out(n + 256);
            size_t len = P4ENC(a.data(), n, enc.data());
            CuAssertTrue(tc, len <= enc.size() - PFOR_BLOCK_SIZE);
            P4DEC(enc.data(), n, out.data());
            CuAssertTrueWithMessage(tc, "P4ENC/P4DEC", std::equal(a.begin(), a.end(), out.begin()));

            len = P4NZENC(a.data(), n, enc.data());
            CuAssertTrue(tc, len <= enc.size() - PFOR_BLOCK_SIZE);
            P4NZDEC(enc.data(), n, out.data());
            CuAssertTrueWithMessage(tc, "P4NZENC/P4NZDEC",
                                    std::equal(a.begin(), a.end(), out.begin()));
        }
    }
}

// encodePos writes blocks of 128 positions and a remainder, decodePos reads one block per call.
void test_encodePos_decodePos(CuTest* tc) {
    for (size_t n : {size_t(1), size_t(40), size_t(128), size_t(129), size_t(300), size_t(1000)}) {
        std::vector<uint32_t> positions = goldenArray(7, n);
        std::vector<uint32_t> expected = positions;

        RAMDirectory dir;
        IndexOutput* out = dir.createOutput("pos");
        PforUtil::encodePos(out, positions);
        out->close();
        _CLDELETE(out);

        IndexInput* in = nullptr;
        CLuceneError error;
        CuAssertTrue(tc, dir.openInput("pos", in, error));
        std::vector<uint32_t> decoded;
        std::vector<uint32_t> block(PforUtil::blockSize + 16);
        while (decoded.size() < n) {
            uint32_t size = PforUtil::decodePos(in, block);
            CuAssertTrue(tc, size > 0 && size <= PforUtil::blockSize);
            decoded.insert(decoded.end(), block.begin(), block.begin() + size);
        }
        CuAssertTrueWithMessage(tc, "encodePos/decodePos", decoded == expected);
        in->close();
        _CLDELETE(in);
        dir.close();
    }
}

CuSuite* testPFORSuite() {
    CuSuite* suite = CuSuiteNew(_T("PFOR Test Suite"));

    SUITE_ADD_TEST(suite, test_pfor_codec_golden);
    SUITE_ADD_TEST(suite, test_pfor_codec_roundtrip);
    SUITE_ADD_TEST(suite, test_pfor_codec_corrupt);
    SUITE_ADD_TEST(suite, test_p4_wrappers);
    SUITE_ADD_TEST(suite, test_encodePos_decodePos);

    return suite;
}
