#include "test.h"

#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "CLucene/store/RAMDirectory.h"
#include "CuTest.h"
#include <ctime>
#include <string>
#include <vector>
#include <utility>
#include <iostream>

using namespace lucene::store;

// Add a helper macro for printing more detailed error messages when assertions fail
#define CuAssertTrueWithMessage(tc, message, condition) \
    do { \
        if (!(condition)) { \
            printf("Assertion failed: %s\n", message); \
        } \
        CuAssertTrue(tc, condition); \
    } while(0)

static void TestUTF8WriteAndReadChars(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();

    const char* testFileName = "test_utf8_chars.dat";

    IndexOutput* output = dir->createOutput(testFileName);

    std::wstring testString;
    testString.push_back(L'A');             // 1 byte
    testString.push_back(L'你');            // 3 bytes
    testString.push_back(L'好');            // 3 bytes
    testString.push_back((wchar_t)0x1F600); // 4 bytes

    output->writeString(testString);

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
            
            // 数学符号
            {0x2200, "For All ∀ (U+2200)"},
            {0x2211, "Summation ∑ (U+2211)"},
            {0x221E, "Infinity ∞ (U+221E)"},
            {0x2248, "Almost Equal To ≈ (U+2248)"},
            
            // 货币符号
            {0x20AC, "Euro € (U+20AC)"},
            {0x20BD, "Russian Ruble ₽ (U+20BD)"},
            {0x20B9, "Indian Rupee ₹ (U+20B9)"},
            {0x20A9, "Won Sign ₩ (U+20A9)"},
            
            // 箭头符号
            {0x2190, "Left Arrow ← (U+2190)"},
            {0x2192, "Right Arrow → (U+2192)"},
            {0x2191, "Up Arrow ↑ (U+2191)"},
            {0x2193, "Down Arrow ↓ (U+2193)"},
            
            // 框线符号
            {0x2550, "Box Drawing ═ (U+2550)"},
            {0x2551, "Box Drawing ║ (U+2551)"},
            {0x2554, "Box Drawing ╔ (U+2554)"},
            {0x2557, "Box Drawing ╗ (U+2557)"},
            
            // 字母符号
            {0x2122, "Trade Mark ™ (U+2122)"},
            {0x2105, "Care Of ℅ (U+2105)"},
            {0x2113, "Script Small L ℓ (U+2113)"},
            {0x2116, "Numero Sign № (U+2116)"},
            
            // 装饰符号
            {0x2600, "Black Sun with Rays ☀ (U+2600)"},
            {0x2602, "Umbrella ☂ (U+2602)"},
            {0x2614, "Umbrella with Rain Drops ☔ (U+2614)"},
            {0x2665, "Black Heart Suit ♥ (U+2665)"},
            
            // 更多补充平面字符
            {0x1D400, "Mathematical Bold Capital A 𝐀 (U+1D400)"},
            {0x1D538, "Mathematical Double-Struck Capital A 𝔸 (U+1D538)"},
            {0x1F300, "Cyclone 🌀 (U+1F300)"},
            {0x1F431, "Cat Face 🐱 (U+1F431)"},
            {0x1F52B, "Pistol 🔫 (U+1F52B)"},
            {0x1F697, "Automobile 🚗 (U+1F697)"},
            
            // 额外CJK字符
            {0x20000, "CJK Unified Ideograph 𠀀 (U+20000)"},
            {0x2A700, "CJK Unified Ideograph 𪜀 (U+2A700)"},
            
            // 其他技术符号
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

            printf("Character #%zu: 0x%04X (%s) -> Readback: 0x%04X, %s\n", i, (unsigned int)original,
                   unicodeTestChars[i].second, (unsigned int)readBack,
                   (original == readBack ? "Success" : "Failed"));

            char errorMsg[256];
            sprintf(errorMsg, "Unicode character mismatch - Position: %zu, Character: %s, Original: 0x%04X, Readback: 0x%04X",
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
    emptyOutput->writeString(L"");
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
    longOutput->writeString(longString);
    longOutput->close();
    _CLDELETE(longOutput);
    
    IndexInput* longInput = nullptr;
    CLuceneError longError;
    dir->openInput("long_string.dat", longInput, longError);
    TCHAR* longStr = longInput->readString();
    std::wstring longReadBack(longStr);
    _CLDELETE_LARRAY(longStr);
    
    CuAssertIntEquals(tc, _T("Long string length mismatch"), (int)longString.size(), (int)longReadBack.size());
    
    // Only check some characters to avoid too much output
    printf("\n=== Long String Test (showing first 10 characters) ===\n");
    for (size_t i = 0; i < 10 && i < longString.size(); i++) {
        printf("Character #%zu: 0x%04X -> Readback: 0x%04X, %s\n", 
               i, 
               (unsigned int)longString[i],
               (unsigned int)longReadBack[i], 
               (longString[i] == longReadBack[i] ? "Success" : "Failed"));
        
        char errorMsg[256];
        sprintf(errorMsg, "Long string character mismatch - Position: %zu, Original: 0x%04X, Readback: 0x%04X", 
                i, (unsigned int)longString[i], (unsigned int)longReadBack[i]);
        CuAssertTrueWithMessage(tc, errorMsg, longString[i] == longReadBack[i]);
    }
    
    // Random sample check
    for (size_t i = 0; i < longString.size(); i += 100) {
        char errorMsg[256];
        sprintf(errorMsg, "Long string sample check failed - Position: %zu, Original: 0x%04X, Readback: 0x%04X", 
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

    std::vector<BoundaryTest> boundaryTests = {
        // 1-byte boundaries
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
        {0x10FFFF, "Unicode upper bound (U+10FFFF)", 4}
    };

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
    CuAssertIntEquals(tc, _T("Boundary test string length mismatch"), (int)testString.size(), (int)readBackString.size());

    // Verify each character and their byte counts
    printf("\n=== UTF-8 Encoding Boundary Tests ===\n");
    for (size_t i = 0; i < testString.size(); i++) {
        wchar_t original = testString[i];
        wchar_t readBack = readBackString[i];
        
        printf("Character #%zu: U+%04X (%s)\n", 
               i, 
               (unsigned int)original, 
               boundaryTests[i].description);
        printf("  - Expected bytes: %d, Actual bytes: %d, %s\n", 
               boundaryTests[i].expectedBytes, 
               actualBytes[i],
               (boundaryTests[i].expectedBytes == actualBytes[i] ? "Success" : "Failed"));
        printf("  - Readback: U+%04X, %s\n", 
               (unsigned int)readBack, 
               (original == readBack ? "Success" : "Failed"));
        
        char errorMsg[256];
        sprintf(errorMsg, "Boundary character mismatch - Position: %zu, Description: %s, Original: 0x%04X, Readback: 0x%04X", 
                i, boundaryTests[i].description, (unsigned int)original, (unsigned int)readBack);
        CuAssertTrueWithMessage(tc, errorMsg, original == readBack);
        CuAssertIntEquals(tc, _T("UTF-8 encoding byte count mismatch"), boundaryTests[i].expectedBytes, actualBytes[i]);
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
        {L"😀😃😄😁😆😅😂🤣", "Consecutive Emoji"}
    };
    
    for (size_t testIndex = 0; testIndex < specialTests.size(); testIndex++) {
        const auto& test = specialTests[testIndex];
        std::string testFileName = "special_test_" + std::to_string(testIndex) + ".dat";
        
        // Write test string
        IndexOutput* output = dir->createOutput(testFileName.c_str());
        output->writeString(test.str);
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
        printf("  - Original length: %zu, Readback length: %zu, %s\n", 
               test.str.length(), 
               readBackString.length(),
               (test.str.length() == readBackString.length() ? "Success" : "Failed"));
        
        CuAssertIntEquals(tc, _T("Special sequence length mismatch"), (int)test.str.length(), (int)readBackString.length());
        
        // For shorter strings, print each character for comparison
        if (test.str.length() <= 20) {
            for (size_t i = 0; i < test.str.length(); i++) {
                printf("  Character #%zu: U+%04X -> Readback: U+%04X, %s\n", 
                       i, 
                       (unsigned int)test.str[i],
                       (unsigned int)readBackString[i], 
                       (test.str[i] == readBackString[i] ? "Success" : "Failed"));
                
                char errorMsg[256];
                sprintf(errorMsg, "Special sequence character mismatch - Test: %s, Position: %zu, Original: 0x%04X, Readback: 0x%04X", 
                        test.description, i, (unsigned int)test.str[i], (unsigned int)readBackString[i]);
                CuAssertTrueWithMessage(tc, errorMsg, test.str[i] == readBackString[i]);
            }
        } else {
            // For longer strings, just check equality
            char errorMsg[256];
            sprintf(errorMsg, "Long special sequence mismatch - Test: %s, Length: %zu", test.description, test.str.length());
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
        testString.push_back(L'A' + (i % 26));  // ASCII characters
        testString.push_back(0x00A0 + (i % 128));  // Latin extended
        testString.push_back(0x4E00 + (i % 1000));  // Chinese characters
        testString.push_back(0x1F600 + (i % 50));  // Emoji (4-byte characters)
    }
    
    printf("\n=== UTF-8 Encoding Performance Test (String length: %zu) ===\n", testString.size());
    
    // Measure write time
    clock_t writeStart = clock();
    
    IndexOutput* output = dir->createOutput(testFileName);
    output->writeString(testString);
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
    sprintf(lengthErrorMsg, "Performance test string length mismatch - Original: %zu, Readback: %zu", 
            testString.size(), readBackString.size());
    CuAssertIntEquals(tc, _T("Performance test string length mismatch"), (int)testString.size(), (int)readBackString.size());
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
        0xF600, // 丽
        0x1F64F, // 🙏 Folded Hands
        0xF64F, // 碌
        0x20021, // 𠀡 CJK Unified Ideographs Extension B
        0x0021, //  ! Basic Latin
        0x2A6D6, // 𪛖 CJK Unified Ideographs Extension C
        0xA6D6, // ꛖ Hangul Jamo Extended-B
        0x10123, // 𐄣 Ancient Greek Numbers
        0x0123, //  Cuneiform
        0x10348, // 𐍈 Gothic Letter Hwair
        0x0348, //  Ȉ
        // Boundary cases
        //0x10000, // Minimum 4-byte character
        0x10FFFF  // Maximum valid Unicode code point
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
            printf("  Character #%d: U+%04X -> Readback: U+%04X, %s\n", 
                   i, 
                   (unsigned int)testString[i],
                   (unsigned int)buffer[i], 
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
        WriteCharsLegacy(output, testString.c_str(), testString.length());
        output->close();
        _CLDELETE(output);

        // Read using both methods for comparison
        std::wstring oldResult;  // Old method read result
        std::wstring newResult;  // New method read result

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
        printf("oldResult.size() = %zu, newResult.size() = %zu\n", oldResult.size(), newResult.size());
        // Compare results
        char lengthErrorMsg[256];
        sprintf(lengthErrorMsg, "Compatibility test length mismatch - Old method: %zu, New method: %zu", 
                oldResult.size(), newResult.size());
        CuAssertTrueWithMessage(tc, lengthErrorMsg, oldResult.size() == newResult.size());
        
        // Verify each character
        for (size_t i = 0; i < oldResult.size(); i++) {
            wchar_t oldChar = oldResult[i];
            wchar_t newChar = newResult[i];
            
            printf("Character #%zu: Old method: U+%04X, New method: U+%04X, %s, Original: U+%04X\n", 
                   i, 
                   (unsigned int)oldChar,
                   (unsigned int)newChar, 
                   (oldChar == newChar ? "Match" : "Mismatch"),
                   (unsigned int)testString[i]);
            
            char errorMsg[256];
            sprintf(errorMsg, "Character mismatch - Position: %zu, Old method: U+%04X, New method: U+%04X", 
                    i, (unsigned int)oldChar, (unsigned int)newChar);
            
            CuAssertTrueWithMessage(tc, errorMsg, oldChar == newChar);
        }
        
        printf("\nCompatibility test: %s\n", 
               (oldResult == newResult ? "PASSED - Methods are compatible" : "FAILED - Methods are not compatible"));
    }

    _CLDELETE(dir);
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

    return suite;
}