#include "test.h"

#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "CLucene/store/RAMDirectory.h"
#include "CuTest.h"
#include <codecvt>
#include <ctime>
#include <locale>
#include <string>
#include <vector>
#include <utility>
#include <iostream>
#include "CLucene/index/_TermInfosWriter.h"
#include "CLucene/index/_TermInfosReader.h"
#include "CLucene/index/_FieldInfos.h"
#include "CLucene/index/_TermInfo.h"
#include "CLucene/index/Term.h"

using namespace lucene::store;

// Add a helper macro for printing more detailed error messages when assertions fail
#define CuAssertTrueWithMessage(tc, message, condition) \
    do {                                                \
        if (!(condition)) {                             \
            printf("Assertion failed: %s\n", message);  \
        }                                               \
        CuAssertTrue(tc, condition);                    \
    } while (0)

static void TestUTF8WriteAndReadChars(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();

    const char* testFileName = "test_utf8_chars.dat";

    IndexOutput* output = dir->createOutput(testFileName);

    std::wstring testString;
    testString.push_back(L'A');             // 1 byte
    testString.push_back(L'你');            // 3 bytes
    testString.push_back(L'好');            // 3 bytes
    testString.push_back((wchar_t)0x1F600); // 4 bytes
    output->writeVInt(testString.length());
    output->writeSChars<TCHAR>(testString.c_str(), testString.length());

    output->close();
    _CLDELETE(output);

    IndexInput* input = nullptr;
    CLuceneError error;
    auto result = dir->openInput(testFileName, input, error);
    CuAssertTrue(tc, result);

    TCHAR* readBackStr = input->readString();
    std::wstring readBackString(readBackStr);
    _CLDELETE_LARRAY(readBackStr);
    printf("\n=== Unicode Character basic test ===\n");

    CuAssertIntEquals(tc, _T("UTF-8 is not equal to the original string"), (int)testString.size(),
                      (int)readBackString.size());

    for (size_t i = 0; i < testString.size(); i++) {
        printf("Character #%zu: 0x%04X -> Readback: 0x%04X, %s\n", i, (unsigned int)testString[i],
               (unsigned int)readBackString[i],
               (testString[i] == readBackString[i] ? "Success" : "Failed"));
        char errorMsg[256];
        sprintf(errorMsg, "Character mismatch - Position: %zu, Original: 0x%04X, Readback: 0x%04X",
                i, (unsigned int)testString[i], (unsigned int)readBackString[i]);
        CuAssertTrueWithMessage(tc, errorMsg, testString[i] == readBackString[i]);
    }

    input->close();
    _CLDELETE(input);
    _CLDELETE(dir);
}

static void WriteCharsLegacy(IndexOutput* output, const TCHAR* s, int32_t length) {
    const int32_t end = length;
    for (int32_t i = 0; i < end; ++i) {
        const int32_t code = (int32_t)s[i];
        if (code >= 0x01 && code <= 0x7F) {
            output->writeByte((uint8_t)code);
        } else if (((code >= 0x80) && (code <= 0x7FF)) || code == 0) {
            output->writeByte((uint8_t)(0xC0 | (code >> 6)));
            output->writeByte((uint8_t)(0x80 | (code & 0x3F)));
        } else {
            output->writeByte((uint8_t)(0xE0 | (((uint32_t)code) >> 12)));
            output->writeByte((uint8_t)(0x80 | ((code >> 6) & 0x3F)));
            output->writeByte((uint8_t)(0x80 | (code & 0x3F)));
        }
    }
}

static void ReadCharsLegacy(IndexInput* input, TCHAR* buffer, int32_t start, int32_t len) {
    const int32_t end = start + len;
    TCHAR b;
    for (int32_t i = start; i < end; ++i) {
        b = input->readByte();
        if ((b & 0x80) == 0) {
            b = (b & 0x7F);
        } else if ((b & 0xE0) != 0xE0) {
            b = (((b & 0x1F) << 6) | (input->readByte() & 0x3F));
        } else {
            b = ((b & 0x0F) << 12) | ((input->readByte() & 0x3F) << 6);
            b |= (input->readByte() & 0x3F);
        }
        buffer[i] = b;
    }
}

// Test encoding and decoding of various Unicode character ranges
static void TestUnicodeRanges(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();
    const char* testFileName = "test_unicode_ranges.dat";

    // Create a test string containing characters from various Unicode ranges
    std::vector<std::pair<wchar_t, const char*>> unicodeTestChars = {
            // ASCII range (U+0000 - U+007F) - 1 byte UTF-8
            //            {0x0000, "NULL character (U+0000)"},
            {0x0001, "Start of Heading (U+0001)"},
            {0x007F, "Delete (U+007F)"},
            {L'A', "Latin letter A (U+0041)"},
            {L'z', "Latin letter z (U+007A)"},

            // Latin Extended (U+0080 - U+07FF) - 2 bytes UTF-8
            {0x00A9, "Copyright symbol © (U+00A9)"},
            {0x00AE, "Registered trademark ® (U+00AE)"},
            {0x00F1, "Spanish letter ñ (U+00F1)"},
            {0x0394, "Greek letter Δ (U+0394)"},
            {0x03A9, "Greek letter Ω (U+03A9)"},
            {0x03C0, "Greek letter π (U+03C0)"},
            {0x0440, "Cyrillic letter р (U+0440)"},
            {0x0521, "Armenian letter Ց (U+0521)"},
            {0x05D0, "Hebrew letter א (U+05D0)"},
            {0x0648, "Arabic letter و (U+0648)"},

            // Other Basic Multilingual Plane characters (U+0800 - U+FFFF) - 3 bytes UTF-8
            {0x0915, "Devanagari letter क (U+0915)"},
            {0x0E01, "Thai letter ก (U+0E01)"},
            {0x1100, "Hangul letter ᄀ (U+1100)"},
            {L'你', "Chinese character 你 (U+4F60)"},
            {L'好', "Chinese character 好 (U+597D)"},
            {0x6C34, "Chinese character 水 (U+6C34)"},
            {0x7389, "Chinese character 玉 (U+7389)"},
            {0x9999, "Chinese character 香 (U+9999)"},
            {0xFF01, "Fullwidth exclamation mark ！ (U+FF01)"},
            {0xFFFF, "BMP maximum value (U+FFFF)"},

            // Supplementary planes (U+10000 - U+10FFFF) - 4 bytes UTF-8
            {0x1F600, "Grinning face emoji 😀 (U+1F600)"},
            {0x1F64F, "Prayer hands emoji 🙏 (U+1F64F)"},
            {0x1F914, "Thinking face emoji 🤔 (U+1F914)"},
            {0x1F4A9, "Pile of poo emoji 💩 (U+1F4A9)"},
            {0x1F680, "Rocket emoji 🚀 (U+1F680)"},
            {0x10348, "Gothic letter 𐍈 (U+10348)"},
            {0x10400, "Deseret letter 𐐀 (U+10400)"},
            {0x10FFFF, "Unicode maximum value (U+10FFFF)"},

            {0x2200, "For All ∀ (U+2200)"},
            {0x2211, "Summation ∑ (U+2211)"},
            {0x221E, "Infinity ∞ (U+221E)"},
            {0x2248, "Almost Equal To ≈ (U+2248)"},

            {0x20AC, "Euro € (U+20AC)"},
            {0x20BD, "Russian Ruble ₽ (U+20BD)"},
            {0x20B9, "Indian Rupee ₹ (U+20B9)"},
            {0x20A9, "Won Sign ₩ (U+20A9)"},

            {0x2190, "Left Arrow ← (U+2190)"},
            {0x2192, "Right Arrow → (U+2192)"},
            {0x2191, "Up Arrow ↑ (U+2191)"},
            {0x2193, "Down Arrow ↓ (U+2193)"},

            {0x2550, "Box Drawing ═ (U+2550)"},
            {0x2551, "Box Drawing ║ (U+2551)"},
            {0x2554, "Box Drawing ╔ (U+2554)"},
            {0x2557, "Box Drawing ╗ (U+2557)"},

            {0x2122, "Trade Mark ™ (U+2122)"},
            {0x2105, "Care Of ℅ (U+2105)"},
            {0x2113, "Script Small L ℓ (U+2113)"},
            {0x2116, "Numero Sign № (U+2116)"},

            {0x2600, "Black Sun with Rays ☀ (U+2600)"},
            {0x2602, "Umbrella ☂ (U+2602)"},
            {0x2614, "Umbrella with Rain Drops ☔ (U+2614)"},
            {0x2665, "Black Heart Suit ♥ (U+2665)"},

            {0x1D400, "Mathematical Bold Capital A 𝐀 (U+1D400)"},
            {0x1D538, "Mathematical Double-Struck Capital A 𝔸 (U+1D538)"},
            {0x1F300, "Cyclone 🌀 (U+1F300)"},
            {0x1F431, "Cat Face 🐱 (U+1F431)"},
            {0x1F52B, "Pistol 🔫 (U+1F52B)"},
            {0x1F697, "Automobile 🚗 (U+1F697)"},

            {0x20000, "CJK Unified Ideograph 𠀀 (U+20000)"},
            {0x2A700, "CJK Unified Ideograph 𪜀 (U+2A700)"},

            {0x2300, "Diameter Sign ⌀ (U+2300)"},
            {0x231B, "Hourglass ⌛ (U+231B)"},
            {0x2328, "Keyboard ⌨ (U+2328)"},
            {0x23F0, "Alarm Clock ⏰ (U+23F0)"}};

    // Build test string
    std::wstring testString;
    for (const auto& pair : unicodeTestChars) {
        testString.push_back(pair.first);
    }
    try {
        // Write test string
        IndexOutput* output = dir->createOutput(testFileName);
        output->writeSChars<TCHAR>(testString.c_str(), testString.length());
        output->close();
        _CLDELETE(output);
    } catch (const std::exception& e) {
        CuAssertTrueWithMessage(tc, e.what(), false);
    }
    try {
        // Read and verify
        IndexInput* input = nullptr;
        CLuceneError error;
        auto result = dir->openInput(testFileName, input, error);
        CuAssertTrue(tc, result);

        TCHAR* readBackStr = _CL_NEWARRAY(TCHAR, testString.length() + 1);
        input->readChars(readBackStr, 0, testString.length());
        readBackStr[testString.length()] = 0;
        std::wstring readBackString(readBackStr);
        _CLDELETE_LARRAY(readBackStr);

        // Verify length
        CuAssertIntEquals(tc, _T("Unicode string length mismatch"), (int)testString.size(),
                          (int)readBackString.size());

        // Verify each character
        printf("\n=== Unicode Character Encoding Test ===\n");
        for (size_t i = 0; i < testString.size(); i++) {
            wchar_t original = testString[i];
            wchar_t readBack = readBackString[i];

            printf("Character #%zu: 0x%04X (%s) -> Readback: 0x%04X, %s\n", i,
                   (unsigned int)original, unicodeTestChars[i].second, (unsigned int)readBack,
                   (original == readBack ? "Success" : "Failed"));

            char errorMsg[256];
            sprintf(errorMsg,
                    "Unicode character mismatch - Position: %zu, Character: %s, Original: 0x%04X, "
                    "Readback: 0x%04X",
                    i, unicodeTestChars[i].second, (unsigned int)original, (unsigned int)readBack);
            CuAssertTrueWithMessage(tc, errorMsg, original == readBack);
        }

        input->close();
        _CLDELETE(input);
        _CLDELETE(dir);
    } catch (const std::exception& e) {
        CuAssertTrueWithMessage(tc, e.what(), false);
    }
}

// Test edge cases and special characters
static void TestEdgeCases(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();
    const char* testFileName = "test_edge_cases.dat";

    // Create a test string containing edge cases
    std::wstring testString;

    // 1. Empty string test
    IndexOutput* emptyOutput = dir->createOutput("empty_string.dat");
    emptyOutput->writeVInt(0);
    emptyOutput->writeSChars<TCHAR>(L"", 0);
    emptyOutput->close();
    _CLDELETE(emptyOutput);

    IndexInput* emptyInput = nullptr;
    CLuceneError emptyError;
    dir->openInput("empty_string.dat", emptyInput, emptyError);
    TCHAR* emptyStr = emptyInput->readString();
    std::wstring emptyReadBack(emptyStr);
    _CLDELETE_LARRAY(emptyStr);
    CuAssertIntEquals(tc, _T("Empty string length should be 0"), 0, (int)emptyReadBack.size());
    emptyInput->close();
    _CLDELETE(emptyInput);

    // 2. Long string test (containing various characters)
    std::wstring longString;
    // Add 1000 mixed characters
    for (int i = 0; i < 250; i++) {
        longString.push_back(L'A' + (i % 26));  // ASCII
        longString.push_back(0x00A0 + i % 100); // Latin extended
        longString.push_back(0x4E00 + i % 100); // Chinese
        longString.push_back(0x1F600 + i % 50); // Emoji
    }

    IndexOutput* longOutput = dir->createOutput("long_string.dat");
    longOutput->writeVInt(longString.length());
    longOutput->writeSChars<TCHAR>(longString.c_str(), longString.length());
    longOutput->close();
    _CLDELETE(longOutput);

    IndexInput* longInput = nullptr;
    CLuceneError longError;
    dir->openInput("long_string.dat", longInput, longError);
    TCHAR* longStr = longInput->readString();
    std::wstring longReadBack(longStr);
    _CLDELETE_LARRAY(longStr);

    CuAssertIntEquals(tc, _T("Long string length mismatch"), (int)longString.size(),
                      (int)longReadBack.size());

    // Only check some characters to avoid too much output
    printf("\n=== Long String Test (showing first 10 characters) ===\n");
    for (size_t i = 0; i < 10 && i < longString.size(); i++) {
        printf("Character #%zu: 0x%04X -> Readback: 0x%04X, %s\n", i, (unsigned int)longString[i],
               (unsigned int)longReadBack[i],
               (longString[i] == longReadBack[i] ? "Success" : "Failed"));

        char errorMsg[256];
        sprintf(errorMsg,
                "Long string character mismatch - Position: %zu, Original: 0x%04X, Readback: "
                "0x%04X",
                i, (unsigned int)longString[i], (unsigned int)longReadBack[i]);
        CuAssertTrueWithMessage(tc, errorMsg, longString[i] == longReadBack[i]);
    }

    // Random sample check
    for (size_t i = 0; i < longString.size(); i += 100) {
        char errorMsg[256];
        sprintf(errorMsg,
                "Long string sample check failed - Position: %zu, Original: 0x%04X, Readback: "
                "0x%04X",
                i, (unsigned int)longString[i], (unsigned int)longReadBack[i]);
        CuAssertTrueWithMessage(tc, errorMsg, longString[i] == longReadBack[i]);
    }

    longInput->close();
    _CLDELETE(longInput);

    _CLDELETE(dir);
}

// Test UTF-8 encoding boundary cases
static void TestUTF8EncodingBoundaries(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();
    const char* testFileName = "test_utf8_boundaries.dat";

    // Test UTF-8 encoding boundary cases
    struct BoundaryTest {
        wchar_t codePoint;
        const char* description;
        int expectedBytes;
    };

    std::vector<BoundaryTest> boundaryTests = {// 1-byte boundaries
                                               //{0x0000, "NULL (U+0000) - 1-byte lower bound", 1},
                                               {0x007F, "DELETE (U+007F) - 1-byte upper bound", 1},

                                               // 2-byte boundaries
                                               {0x0080, "PAD (U+0080) - 2-byte lower bound", 2},
                                               {0x07FF, "2-byte upper bound (U+07FF)", 2},

                                               // 3-byte boundaries
                                               {0x0800, "3-byte lower bound (U+0800)", 3},
                                               {0xFFFF, "BMP upper bound (U+FFFF)", 3},

                                               // 4-byte boundaries (supplementary planes)
                                               {0x10000, "SMP lower bound (U+10000)", 4},
                                               {0x10FFFF, "Unicode upper bound (U+10FFFF)", 4}};

    // Build test string
    std::wstring testString;
    for (const auto& test : boundaryTests) {
        testString.push_back(test.codePoint);
    }

    // Write test string
    IndexOutput* output = dir->createOutput(testFileName);

    // Manually write each character and record byte count
    output->writeVInt(testString.length());

    std::vector<int> actualBytes;
    int64_t startPos, endPos;

    for (size_t i = 0; i < testString.size(); i++) {
        startPos = output->getFilePointer();
        output->writeChars(&testString[i], 1);
        endPos = output->getFilePointer();
        actualBytes.push_back((int)(endPos - startPos));
    }

    output->close();
    _CLDELETE(output);

    // Read and verify
    IndexInput* input = nullptr;
    CLuceneError error;
    dir->openInput(testFileName, input, error);

    const int32_t len = input->readVInt();
    TCHAR* buffer = _CL_NEWARRAY(TCHAR, len + 1);
    input->readChars(buffer, 0, len);
    buffer[len] = 0;
    std::wstring readBackString(buffer);
    _CLDELETE_LARRAY(buffer);

    // Verify length
    CuAssertIntEquals(tc, _T("Boundary test string length mismatch"), (int)testString.size(),
                      (int)readBackString.size());

    // Verify each character and their byte counts
    printf("\n=== UTF-8 Encoding Boundary Tests ===\n");
    for (size_t i = 0; i < testString.size(); i++) {
        wchar_t original = testString[i];
        wchar_t readBack = readBackString[i];

        printf("Character #%zu: U+%04X (%s)\n", i, (unsigned int)original,
               boundaryTests[i].description);
        printf("  - Expected bytes: %d, Actual bytes: %d, %s\n", boundaryTests[i].expectedBytes,
               actualBytes[i],
               (boundaryTests[i].expectedBytes == actualBytes[i] ? "Success" : "Failed"));
        printf("  - Readback: U+%04X, %s\n", (unsigned int)readBack,
               (original == readBack ? "Success" : "Failed"));

        char errorMsg[256];
        sprintf(errorMsg,
                "Boundary character mismatch - Position: %zu, Description: %s, Original: 0x%04X, "
                "Readback: 0x%04X",
                i, boundaryTests[i].description, (unsigned int)original, (unsigned int)readBack);
        CuAssertTrueWithMessage(tc, errorMsg, original == readBack);
        CuAssertIntEquals(tc, _T("UTF-8 encoding byte count mismatch"),
                          boundaryTests[i].expectedBytes, actualBytes[i]);
    }

    input->close();
    _CLDELETE(input);
    _CLDELETE(dir);
}

// Test special UTF-8 sequences
static void TestSpecialUTF8Sequences(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();

    // Test some special UTF-8 sequences
    struct SpecialSequenceTest {
        std::wstring str;
        const char* description;
    };

    std::vector<SpecialSequenceTest> specialTests = {
            {L"", "Empty string"},
            {L"Hello", "Pure ASCII string"},
            {L"你好世界", "Pure Chinese string"},
            {L"Hello, 世界!", "Mixed ASCII and Chinese"},
            {L"🌍🌎🌏", "Pure Emoji (4-byte characters)"},
            {L"Earth: 🌍 🌎 🌏", "Mixed Chinese and Emoji"},
            {L"A\u0000B", "String containing NULL character"},
            {L"سلام دنیا", "Arabic/Persian text"},
            {L"こんにちは世界", "Japanese and Chinese"},
            {L"Hello\nWorld", "String with newline"},
            {L"Tab\tCharacter", "String with tab character"},
            {std::wstring(1000, L'A'), "1000 identical characters"},
            {L"😀😃😄😁😆😅😂🤣", "Consecutive Emoji"}};

    for (size_t testIndex = 0; testIndex < specialTests.size(); testIndex++) {
        const auto& test = specialTests[testIndex];
        std::string testFileName = "special_test_" + std::to_string(testIndex) + ".dat";

        // Write test string
        IndexOutput* output = dir->createOutput(testFileName.c_str());
        output->writeVInt(test.str.length());
        output->writeSChars<TCHAR>(test.str.c_str(), test.str.length());
        output->close();
        _CLDELETE(output);

        // Read and verify
        IndexInput* input = nullptr;
        CLuceneError error;
        dir->openInput(testFileName.c_str(), input, error);

        TCHAR* readBackStr = input->readString();
        std::wstring readBackString(readBackStr);
        _CLDELETE_LARRAY(readBackStr);

        printf("\n=== Special UTF-8 Sequence Test #%zu: %s ===\n", testIndex, test.description);
        printf("  - Original length: %zu, Readback length: %zu, %s\n", test.str.length(),
               readBackString.length(),
               (test.str.length() == readBackString.length() ? "Success" : "Failed"));

        CuAssertIntEquals(tc, _T("Special sequence length mismatch"), (int)test.str.length(),
                          (int)readBackString.length());

        // For shorter strings, print each character for comparison
        if (test.str.length() <= 20) {
            for (size_t i = 0; i < test.str.length(); i++) {
                printf("  Character #%zu: U+%04X -> Readback: U+%04X, %s\n", i,
                       (unsigned int)test.str[i], (unsigned int)readBackString[i],
                       (test.str[i] == readBackString[i] ? "Success" : "Failed"));

                char errorMsg[256];
                sprintf(errorMsg,
                        "Special sequence character mismatch - Test: %s, Position: %zu, Original: "
                        "0x%04X, Readback: 0x%04X",
                        test.description, i, (unsigned int)test.str[i],
                        (unsigned int)readBackString[i]);
                CuAssertTrueWithMessage(tc, errorMsg, test.str[i] == readBackString[i]);
            }
        } else {
            // For longer strings, just check equality
            char errorMsg[256];
            sprintf(errorMsg, "Long special sequence mismatch - Test: %s, Length: %zu",
                    test.description, test.str.length());
            CuAssertTrueWithMessage(tc, errorMsg, test.str == readBackString);
        }

        input->close();
        _CLDELETE(input);
    }

    _CLDELETE(dir);
}

// Test UTF-8 encoding performance
static void TestUTF8Performance(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();
    const char* testFileName = "test_utf8_performance.dat";

    // Create a large test string with various types of characters
    std::wstring testString;
    const int testSize = 100000; // 100,000 characters

    // Add different types of characters
    for (int i = 0; i < testSize / 4; i++) {
        testString.push_back(L'A' + (i % 26));     // ASCII characters
        testString.push_back(0x00A0 + (i % 128));  // Latin extended
        testString.push_back(0x4E00 + (i % 1000)); // Chinese characters
        testString.push_back(0x1F600 + (i % 50));  // Emoji (4-byte characters)
    }

    printf("\n=== UTF-8 Encoding Performance Test (String length: %zu) ===\n", testString.size());

    // Measure write time
    clock_t writeStart = clock();

    IndexOutput* output = dir->createOutput(testFileName);
    output->writeVInt(testString.length());
    output->writeSChars<TCHAR>(testString.c_str(), testString.length());
    output->close();
    _CLDELETE(output);

    clock_t writeEnd = clock();
    double writeTime = ((double)(writeEnd - writeStart)) / CLOCKS_PER_SEC;
    printf("Write time: %.6f seconds\n", writeTime);

    // Measure read time
    clock_t readStart = clock();

    IndexInput* input = nullptr;
    CLuceneError error;
    dir->openInput(testFileName, input, error);

    TCHAR* readBackStr = input->readString();
    std::wstring readBackString(readBackStr);
    _CLDELETE_LARRAY(readBackStr);

    clock_t readEnd = clock();
    double readTime = ((double)(readEnd - readStart)) / CLOCKS_PER_SEC;
    printf("Read time: %.6f seconds\n", readTime);

    // Verify results
    char lengthErrorMsg[256];
    sprintf(lengthErrorMsg,
            "Performance test string length mismatch - Original: %zu, Readback: %zu",
            testString.size(), readBackString.size());
    CuAssertIntEquals(tc, _T("Performance test string length mismatch"), (int)testString.size(),
                      (int)readBackString.size());
    printf("  %s\n", lengthErrorMsg); // Print error message directly instead of using macro

    char contentErrorMsg[256];
    sprintf(contentErrorMsg, "Performance test string content mismatch");
    CuAssertTrueWithMessage(tc, contentErrorMsg, testString == readBackString);

    // Calculate characters processed per second
    double writeCharsPerSec = testString.size() / writeTime;
    double readCharsPerSec = readBackString.size() / readTime;

    printf("Write speed: %.2f characters/second\n", writeCharsPerSec);
    printf("Read speed: %.2f characters/second\n", readCharsPerSec);

    input->close();
    _CLDELETE(input);
    _CLDELETE(dir);
}

static void TestUTF8Compatibility(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();
    const char* testFileName = "test_compatibility.dat";

    // Test characters covering different ranges
    const std::vector<wchar_t> testChars = {
            // Basic 1-byte
            L'A',
            // Common 3-byte Chinese characters
            L'你', L'好',
            // 4-byte characters from different planes
            0x1F600, // 😀 Grinning Face (Emoji)
            0xF600,  // 丽
            0x1F64F, // 🙏 Folded Hands
            0xF64F,  // 碌
            0x20021, // 𠀡 CJK Unified Ideographs Extension B
            0x0021,  //  ! Basic Latin
            0x2A6D6, // 𪛖 CJK Unified Ideographs Extension C
            0xA6D6,  // ꛖ Hangul Jamo Extended-B
            0x10123, // 𐄣 Ancient Greek Numbers
            0x0123,  //  Cuneiform
            0x10348, // 𐍈 Gothic Letter Hwair
            0x0348,  //  Ȉ
            // Boundary cases
            //0x10000, // Minimum 4-byte character
            0x10FFFF // Maximum valid Unicode code point
    };

    // Build test string with mixed character types
    std::wstring testString;
    for (auto ch : testChars) {
        testString.push_back(ch);
    }
    printf("testString.size() = %zu\n", testString.size());
    // Test case 1: New code write, old code readn
    try {
        IndexOutput* output = dir->createOutput("new_write_old_read.dat");
        output->writeVInt(testString.length());
        output->writeChars(testString.c_str(), testString.length());
        output->close();
        _CLDELETE(output);

        IndexInput* input = nullptr;
        CLuceneError error;
        dir->openInput("new_write_old_read.dat", input, error);

        const int32_t len = input->readVInt();
        TCHAR* buffer = _CL_NEWARRAY(TCHAR, len + 1);
        ReadCharsLegacy(input, buffer, 0, len);
        buffer[len] = 0;

        for (int32_t i = 0; i < len; i++) {
            printf("  Character #%d: U+%04X -> Readback: U+%04X, %s\n", i,
                   (unsigned int)testString[i], (unsigned int)buffer[i],
                   (testString[i] == buffer[i] ? "Success" : "Failed"));
        }
        CuAssertTrue(tc, buffer[3] != testString[3]); // old code cannot parse 4-byte character
        _CLDELETE_LARRAY(buffer);
        _CLDELETE(input);
    } catch (CLuceneError& e) {
        CuAssertTrueWithMessage(tc, e.what(), false);
    }

    // Test case 2: Old code write, new code read
    {
        // Old implementation write (only handles 3 bytes)
        IndexOutput* output = dir->createOutput("old_write_new_read.dat");
        output->writeVInt(testString.length());
        output->writeSCharsOrigin<TCHAR>(testString.c_str(), testString.length());
        output->close();
        _CLDELETE(output);

        // Read using both methods for comparison
        std::wstring oldResult; // Old method read result
        std::wstring newResult; // New method read result

        // Old method read
        {
            IndexInput* input = nullptr;
            CLuceneError error;
            dir->openInput("old_write_new_read.dat", input, error);

            const int32_t len = input->readVInt();
            TCHAR* buffer = _CL_NEWARRAY(TCHAR, len + 1);
            ReadCharsLegacy(input, buffer, 0, len);
            for (int32_t i = 0; i < len; i++) {
                printf("testString[%d] = %d, buffer[%d] = %d\n", i, testString[i], i, buffer[i]);
            }
            buffer[len] = 0;
            oldResult = buffer;
            _CLDELETE_LARRAY(buffer);
            _CLDELETE(input);
        }
        printf("oldResult.size() = %zu\n", oldResult.size());

        // New method read
        {
            IndexInput* input = nullptr;
            CLuceneError error;
            dir->openInput("old_write_new_read.dat", input, error);

            const int32_t len = input->readVInt();
            TCHAR* buffer = _CL_NEWARRAY(TCHAR, len + 1);
            input->readChars(buffer, 0, len);
            buffer[len] = 0;
            newResult = buffer;
            _CLDELETE_LARRAY(buffer);
            _CLDELETE(input);
        }
        printf("newResult.size() = %zu\n", newResult.size());
        printf("oldResult.size() = %zu, newResult.size() = %zu\n", oldResult.size(),
               newResult.size());
        // Compare results
        char lengthErrorMsg[256];
        sprintf(lengthErrorMsg,
                "Compatibility test length mismatch - Old method: %zu, New method: %zu",
                oldResult.size(), newResult.size());
        CuAssertTrueWithMessage(tc, lengthErrorMsg, oldResult.size() == newResult.size());

        // Verify each character
        for (size_t i = 0; i < oldResult.size(); i++) {
            wchar_t oldChar = oldResult[i];
            wchar_t newChar = newResult[i];

            printf("Character #%zu: Old method: U+%04X, New method: U+%04X, %s, Original: U+%04X\n",
                   i, (unsigned int)oldChar, (unsigned int)newChar,
                   (oldChar == newChar ? "Match" : "Mismatch"), (unsigned int)testString[i]);

            char errorMsg[256];
            sprintf(errorMsg,
                    "Character mismatch - Position: %zu, Old method: U+%04X, New method: U+%04X", i,
                    (unsigned int)oldChar, (unsigned int)newChar);

            CuAssertTrueWithMessage(tc, errorMsg, oldChar == newChar);
        }

        printf("\nCompatibility test: %s\n",
               (oldResult == newResult ? "PASSED - Methods are compatible"
                                       : "FAILED - Methods are not compatible"));
    }

    _CLDELETE(dir);
}

void print_wstring(const std::wstring& wstr) {
    std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
    std::string str = converter.to_bytes(wstr);
    std::cout << str << std::endl;
}
// Test STermInfosWriter with various Unicode characters (write and read)
static void TestSTermInfosWriterUnicode(CuTest* tc) {
    printf("\n=== Testing STermInfosWriter<TCHAR> with Unicode (Write and Read) ===\n");

    // Create a RAM directory for testing
    Directory* dir = _CLNEW RAMDirectory();

    // Create field infos
    FieldInfos* fieldInfos = _CLNEW FieldInfos();
    fieldInfos->add(_T("content"), false);

    const char* segmentName = "test_unicode";

    // Define Unicode test cases
    struct UnicodeTermTest {
        std::wstring str;
        const char* description;
    };

    std::vector<UnicodeTermTest> testTerms = {
            {L"A", "Basic Latin A (U+0041)"},
            {L"z", "Basic Latin z (U+007A)"},
            {L"©", "Copyright Sign (U+00A9)"},
            {L"é", "Latin Small E with acute (U+00E9)"},
            {L"œ", "Latin Small Ligature OE (U+0153)"},
            {L"Ω", "Greek Capital Omega (U+03A9)"},
            {L"π", "Greek Small Pi (U+03C0)"},
            {L"Я", "Cyrillic Capital Letter Ya (U+042F)"},
            {L"я", "Cyrillic Small Letter Ya (U+044F)"},
            {L"א", "Hebrew Letter Alef (U+05D0)"},
            {L"ا", "Arabic Letter Alef (U+0627)"},
            {L"अ", "Devanagari Letter A (U+0905)"},
            {L"一", "CJK Unified Ideograph (U+4E00)"},
            {L"中", "CJK Unified Ideograph (U+4E2D)"},
            {L"文", "CJK Unified Ideograph (U+6587)"},
            {L"青", "CJK Unified Ideograph (U+9752)"},
            {L"あ", "Hiragana Letter A (U+3042)"},
            {L"ア", "Katakana Letter A (U+30A2)"},
            {L"가", "Hangul Syllable GA (U+AC00)"},

            {L"∀", "For All (U+2200)"},
            {L"∑", "Summation (U+2211)"},
            {L"∞", "Infinity (U+221E)"},
            {L"≈", "Almost Equal To (U+2248)"},
            {L"€", "Euro (U+20AC)"},
            {L"₽", "Russian Ruble (U+20BD)"},
            {L"₹", "Indian Rupee (U+20B9)"},
            {L"₩", "Won Sign (U+20A9)"},
            {L"←", "Left Arrow (U+2190)"},
            {L"→", "Right Arrow (U+2192)"},
            {L"═", "Box Drawing (U+2550)"},
            {L"║", "Box Drawing (U+2551)"},
            {L"☀", "Black Sun with Rays (U+2600)"},
            {L"♥", "Black Heart Suit (U+2665)"},

            {L"Hello世界", "Mixed English and Chinese"},
            {L"Café☕", "Latin with accent and emoji"},
            {L"Москва🏙", "Cyrillic with emoji"},
            {L"こんにちは🌸", "Japanese with emoji"},
            {L"안녕하세요🇰🇷", "Korean with flag emoji"},
            {L"αβγδεζηθ", "Greek alphabet sequence"},
            {L"∀x∈ℝ∃y≥x", "Mathematical expression"},
            {L"♠♥♦♣", "Card suits"},
            {L"←↑→↓", "Arrow directions"},
            {L"╔═══╗║   ║╚═══╝", "Box drawing frame"},

            {L"苹果🍎Apple", "Mixed Chinese, emoji and English"},
            {L"数学∫f(x)dx=F(x)+C", "Mathematical formula with Chinese"},
            {L"こんにちは世界", "Japanese and Chinese"},
            {L"тест тест 测试 테스트", "Mixed Cyrillic, Chinese and Korean"},
            {L"😀😁😂🤣😃😄😅😆", "Multiple emoji sequence"}};

    // === WRITE PHASE ===
    printf("\n--- Write Phase ---\n");

    // Create a TermInfosWriter
    STermInfosWriter<TCHAR>* writer =
            _CLNEW STermInfosWriter<TCHAR>(dir, segmentName, fieldInfos, 128);
    writer->setEnableCorrectTermWrite(true);

    // Add terms to the writer
    TermInfo* ti = _CLNEW TermInfo();
    int64_t freqPointer = 0;
    int64_t proxPointer = 1000;

    // Store terms for verification
    std::vector<Term*> terms;
    std::vector<TermInfo*> termInfos;

    // Add all test terms to the writer
    for (size_t i = 0; i < testTerms.size(); i++) {
        // Create term
        Term* term = _CLNEW Term(_T("content"), testTerms[i].str.c_str());
        terms.push_back(term);

        // Create and store term info
        TermInfo* currentTi = _CLNEW TermInfo();
        currentTi->docFreq = i + 1;
        currentTi->freqPointer = freqPointer;
        currentTi->proxPointer = proxPointer;
        currentTi->skipOffset = (i % 10 == 0) ? 10 : 0;
        termInfos.push_back(currentTi);

        // Add term to writer
        TermInfo tempTi;
        tempTi.docFreq = currentTi->docFreq;
        tempTi.freqPointer = currentTi->freqPointer;
        tempTi.proxPointer = currentTi->proxPointer;
        tempTi.skipOffset = currentTi->skipOffset;
        writer->add(term->field(), term->text(), term->textLength(), &tempTi);

        printf("Added Term #%zu: \"", i);
        print_wstring(testTerms[i].str);
        printf("\" - %s\n", testTerms[i].description);

        // Increment pointers for next term
        freqPointer += 100 + i * 10;
        proxPointer += 200 + i * 20;
    }

    // Close the writer
    writer->close();
    _CLDELETE(writer);
    _CLDELETE(ti);

    // === READ PHASE ===
    printf("\n--- Read Phase ---\n");

    // Create a TermInfosReader
    TermInfosReader* reader = _CLNEW TermInfosReader(dir, segmentName, fieldInfos);

    // Verify each term can be read back correctly
    for (size_t i = 0; i < terms.size(); i++) {
        Term* originalTerm = terms[i];
        TermInfo* originalInfo = termInfos[i];

        // Read term info
        auto readInfo = reader->get(originalTerm, nullptr);

        printf("Term #%zu: \"", i);
        print_wstring(testTerms[i].str);
        printf("\" - %s\n", readInfo ? "Found" : "NOT FOUND");

        if (readInfo) {
            // Verify values match
            CuAssertTrue(tc, originalInfo->docFreq == readInfo->docFreq);
            CuAssertTrue(tc, originalInfo->freqPointer == readInfo->freqPointer);
            CuAssertTrue(tc, originalInfo->proxPointer == readInfo->proxPointer);
        }

        _CLDELETE(readInfo);
    }

    // Verify term enumeration
    SegmentTermEnum* termEnum = reader->terms();
    size_t count = 0;

    printf("\n--- Term Enumeration ---\n");
    while (termEnum->next()) {
        Term* term = termEnum->term(false);
        printf("Enum #%zu: ", count);
        print_wstring(term->text());
        CuAssertTrueWithMessage(tc, "Term text mismatch",
                                _tcscmp(term->text(), testTerms[count].str.c_str()) == 0);
        count++;
    }

    printf("Total enumerated terms: %zu (expected: %zu)\n", count, terms.size());
    CuAssertTrue(tc, (int)terms.size() == (int)count);

    _CLDELETE(termEnum);
    _CLDELETE(reader);

    // Clean up
    for (size_t i = 0; i < terms.size(); i++) {
        _CLDELETE(terms[i]);
        _CLDELETE(termInfos[i]);
    }

    _CLDELETE(fieldInfos);
    _CLDELETE(dir);

    printf("STermInfosWriter/Reader Unicode test completed successfully\n");
}

// Test STermInfosWriter with Unicode characters when correctTermWrite is disabled
static void TestSTermInfosWriterUnicodeDisabled(CuTest* tc) {
    printf("\n=== Testing STermInfosWriter<TCHAR> with Unicode (Disabled Correct Term Write) "
           "===\n");

    // Create a RAM directory for testing
    Directory* dir = _CLNEW RAMDirectory();

    // Create field infos
    FieldInfos* fieldInfos = _CLNEW FieldInfos();
    fieldInfos->add(_T("content"), false);

    const char* segmentName = "test_unicode_disabled";

    // Define Unicode test cases - one normal char and one > 0xFFFF
    struct UnicodeTermTest {
        std::wstring str;
        const char* description;
        bool shouldCorruptOnRead;
    };

    std::vector<UnicodeTermTest> testTerms = {
            {L"中", "CJK Unified Ideograph (U+4E2D)", false}, // Regular Unicode character
            {L"𠜎", "CJK Unified Ideograph Extension B (U+2070E)",
             true} // Character beyond BMP (> 0xFFFF)
    };

    // === WRITE PHASE ===
    printf("\n--- Write Phase ---\n");

    // Create a TermInfosWriter with correctTermWrite disabled
    STermInfosWriter<TCHAR>* writer =
            _CLNEW STermInfosWriter<TCHAR>(dir, segmentName, fieldInfos, 128);
    writer->setEnableCorrectTermWrite(false); // Disable correct term write

    // Add terms to the writer
    int64_t freqPointer = 0;
    int64_t proxPointer = 1000;

    // Store terms for verification
    std::vector<Term*> terms;
    std::vector<TermInfo*> termInfos;
    std::vector<std::wstring> originalStrings;

    // Add all test terms to the writer
    for (size_t i = 0; i < testTerms.size(); i++) {
        // Store original string for later comparison
        originalStrings.push_back(testTerms[i].str);

        // Create term
        Term* term = _CLNEW Term(_T("content"), testTerms[i].str.c_str());
        terms.push_back(term);

        // Create and store term info
        TermInfo* currentTi = _CLNEW TermInfo();
        currentTi->docFreq = i + 1;
        currentTi->freqPointer = freqPointer;
        currentTi->proxPointer = proxPointer;
        currentTi->skipOffset = 0;
        termInfos.push_back(currentTi);

        // Add term to writer
        TermInfo tempTi;
        tempTi.docFreq = currentTi->docFreq;
        tempTi.freqPointer = currentTi->freqPointer;
        tempTi.proxPointer = currentTi->proxPointer;
        tempTi.skipOffset = currentTi->skipOffset;
        writer->add(term->field(), term->text(), term->textLength(), &tempTi);

        printf("Added Term #%zu: \"", i);
        print_wstring(testTerms[i].str);
        printf("\" - %s\n", testTerms[i].description);

        // Increment pointers for next term
        freqPointer += 100 + i * 10;
        proxPointer += 200 + i * 20;
    }

    // Close the writer
    writer->close();
    _CLDELETE(writer);

    // === READ PHASE ===
    printf("\n--- Read Phase ---\n");

    // Create a TermInfosReader
    TermInfosReader* reader = _CLNEW TermInfosReader(dir, segmentName, fieldInfos);

    // Verify each term's read behavior
    for (size_t i = 0; i < terms.size(); i++) {
        Term* originalTerm = terms[i];
        TermInfo* originalInfo = termInfos[i];

        // Read term info
        auto readInfo = reader->get(originalTerm, nullptr);

        printf("Term #%zu: \"", i);
        print_wstring(testTerms[i].str);
        printf("\" - %s\n", readInfo ? "Found" : "NOT FOUND");

        if (readInfo) {
            // Verify values match
            CuAssertTrue(tc, originalInfo->docFreq == readInfo->docFreq);
            CuAssertTrue(tc, originalInfo->freqPointer == readInfo->freqPointer);
            CuAssertTrue(tc, originalInfo->proxPointer == readInfo->proxPointer);
        }

        _CLDELETE(readInfo);
    }

    // Verify term enumeration
    SegmentTermEnum* termEnum = reader->terms();
    size_t count = 0;

    printf("\n--- Term Enumeration ---\n");
    while (termEnum->next()) {
        Term* term = termEnum->term(false);
        printf("Enum #%zu: ", count);
        print_wstring(term->text());

        // For regular Unicode characters, they should be preserved correctly
        if (!testTerms[count].shouldCorruptOnRead) {
            CuAssertTrueWithMessage(tc, "Regular Unicode term text should match",
                                    _tcscmp(term->text(), originalStrings[count].c_str()) == 0);
        } else {
            // For characters beyond BMP (>0xFFFF), they should NOT match due to disabled correctTermWrite
            printf(" - Expected corruption for high Unicode character\n");
            CuAssertTrueWithMessage(tc, "High Unicode term should be corrupted",
                                    _tcscmp(term->text(), originalStrings[count].c_str()) != 0);
        }
        count++;
    }

    printf("Total enumerated terms: %zu (expected: %zu)\n", count, terms.size());
    CuAssertTrue(tc, (int)terms.size() == (int)count);

    _CLDELETE(termEnum);
    _CLDELETE(reader);

    // Clean up
    for (size_t i = 0; i < terms.size(); i++) {
        _CLDELETE(terms[i]);
        _CLDELETE(termInfos[i]);
    }

    _CLDELETE(fieldInfos);
    _CLDELETE(dir);

    printf("STermInfosWriter/Reader Unicode Disabled test completed\n");
}

CuSuite* testUTF8CharsSuite() {
    CuSuite* suite = CuSuiteNew(_T("UTF-8 Character Test Suite"));

    SUITE_ADD_TEST(suite, TestUTF8WriteAndReadChars);
    SUITE_ADD_TEST(suite, TestUnicodeRanges);
    SUITE_ADD_TEST(suite, TestEdgeCases);
    SUITE_ADD_TEST(suite, TestUTF8EncodingBoundaries);
    SUITE_ADD_TEST(suite, TestSpecialUTF8Sequences);
    SUITE_ADD_TEST(suite, TestUTF8Performance);
    SUITE_ADD_TEST(suite, TestUTF8Compatibility);
    SUITE_ADD_TEST(suite, TestSTermInfosWriterUnicode);
    SUITE_ADD_TEST(suite, TestSTermInfosWriterUnicodeDisabled);

    return suite;
}