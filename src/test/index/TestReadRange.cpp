/*------------------------------------------------------------------------------
 * Test for readRange and skipToBlock interfaces
 * Compares block-based reading with traditional next() approach
 * Verifies correctness and performance
 *------------------------------------------------------------------------------*/
#include <CLucene.h>
#include <CLucene/index/DocRange.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/MultiReader.h>
#include <CLucene/util/stringUtil.h>

#include <algorithm>
#include <chrono>
#include <ctime>
#include <random>
#include <string>
#include <vector>

#include "CLucene/analysis/Analyzers.h"
#include "CLucene/index/IndexVersion.h"
#include "CLucene/index/Term.h"
#include "CLucene/store/RAMDirectory.h"
#include "test.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)
CL_NS_USE(util)

// Test configuration
static constexpr int32_t DEFAULT_DOC_COUNT = 10000;
static constexpr int32_t PERF_ITERATIONS = 10;

#define FINALLY(eptr, finallyBlock)       \
    {                                     \
        finallyBlock;                     \
        if (eptr) {                       \
            std::rethrow_exception(eptr); \
        }                                 \
    }

static int32_t getDaySeed() {
    std::time_t now = std::time(nullptr);
    std::tm* localTime = std::localtime(&now);
    localTime->tm_sec = 0;
    localTime->tm_min = 0;
    localTime->tm_hour = 0;
    return static_cast<int32_t>(std::mktime(localTime) / (60 * 60 * 24));
}

// Generate random text with multiple tokens for position testing
static std::string generateRandomText(std::mt19937& rng, int minTokens, int maxTokens) {
    static const std::vector<std::string> words = {
            "apple",    "banana",    "cherry",     "date",      "elderberry", "fig",      "grape",
            "honeydew", "kiwi",      "lemon",      "mango",     "nectarine",  "orange",   "papaya",
            "quince",   "raspberry", "strawberry", "tangerine", "watermelon", "blueberry"};

    std::uniform_int_distribution<int> tokenDist(minTokens, maxTokens);
    std::uniform_int_distribution<size_t> wordDist(0, words.size() - 1);

    int numTokens = tokenDist(rng);
    std::string result;
    for (int i = 0; i < numTokens; ++i) {
        if (i > 0) result += " ";
        result += words[wordDist(rng)];
    }
    return result;
}

// Write index with random data
static void writeTestIndex(const std::string& fieldName, RAMDirectory* dir,
                           IndexVersion indexVersion, const std::vector<std::string>& datas) {
    auto* analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>;
    analyzer->set_stopwords(nullptr);
    auto* indexwriter = _CLNEW lucene::index::IndexWriter(dir, analyzer, true);
    indexwriter->setRAMBufferSizeMB(512);
    indexwriter->setMaxBufferedDocs(-1);
    indexwriter->setMaxFieldLength(0x7FFFFFFFL);
    indexwriter->setMergeFactor(1000000000);
    indexwriter->setUseCompoundFile(false);

    auto* char_string_reader = _CLNEW lucene::util::SStringReader<char>;

    auto* doc = _CLNEW lucene::document::Document();
    int32_t field_config = lucene::document::Field::STORE_NO;
    field_config |= lucene::document::Field::INDEX_TOKENIZED;
    auto field_name_w = std::wstring(fieldName.begin(), fieldName.end());
    auto* field = _CLNEW lucene::document::Field(field_name_w.c_str(), field_config);
    field->setOmitTermFreqAndPositions(false);
    field->setIndexVersion(indexVersion);
    doc->add(*field);

    for (const auto& data : datas) {
        char_string_reader->init(data.data(), data.size(), false);
        auto* stream = analyzer->reusableTokenStream(field->name(), char_string_reader);
        field->setValue(stream);
        indexwriter->addDocument(doc);
    }

    indexwriter->close();

    _CLLDELETE(indexwriter);
    _CLLDELETE(doc);
    _CLLDELETE(analyzer);
    _CLLDELETE(char_string_reader);
}

// Result structure for comparison
struct TermDocsResult {
    std::vector<int32_t> docs;
    std::vector<int32_t> freqs;
    std::vector<int32_t> norms;
};

struct TermPositionsResult {
    std::vector<int32_t> docs;
    std::vector<int32_t> freqs;
    std::vector<std::vector<int32_t>> positions; // positions per doc
};

// Read using traditional next() method
static TermDocsResult readWithNext(TermDocs* termDocs) {
    TermDocsResult result;
    while (termDocs->next()) {
        result.docs.push_back(termDocs->doc());
        result.freqs.push_back(termDocs->freq());
        result.norms.push_back(termDocs->norm());
    }
    return result;
}

// Read using readBlock() method
static TermDocsResult readWithBlock(TermDocs* termDocs) {
    TermDocsResult result;
    DocRange docRange;
    while (termDocs->readBlock(&docRange)) {
        for (uint32_t i = 0; i < docRange.doc_many_size_; ++i) {
            result.docs.push_back((*docRange.doc_many)[i]);
            if (docRange.freq_many && i < docRange.freq_many_size_) {
                result.freqs.push_back((*docRange.freq_many)[i]);
            } else {
                result.freqs.push_back(1);
            }
            if (docRange.norm_many && i < docRange.norm_many_size_) {
                result.norms.push_back((*docRange.norm_many)[i]);
            } else {
                result.norms.push_back(0);
            }
        }
    }
    return result;
}

// Read positions using traditional next()/nextPosition() method
static TermPositionsResult readPositionsWithNext(TermPositions* termPos) {
    TermPositionsResult result;
    while (termPos->next()) {
        result.docs.push_back(termPos->doc());
        int32_t freq = termPos->freq();
        result.freqs.push_back(freq);

        std::vector<int32_t> positions;
        for (int32_t i = 0; i < freq; ++i) {
            positions.push_back(termPos->nextPosition());
        }
        result.positions.push_back(std::move(positions));
    }
    return result;
}

// Read positions using readBlock() and nextDeltaPosition() method
static TermPositionsResult readPositionsWithBlock(TermPositions* termPos) {
    TermPositionsResult result;
    DocRange docRange;

    while (termPos->readBlock(&docRange)) {
        for (uint32_t i = 0; i < docRange.doc_many_size_; ++i) {
            result.docs.push_back((*docRange.doc_many)[i]);
            int32_t freq = 1;
            if (docRange.freq_many && i < docRange.freq_many_size_) {
                freq = (*docRange.freq_many)[i];
            }
            result.freqs.push_back(freq);

            std::vector<int32_t> positions;
            int32_t position = 0;
            for (int32_t j = 0; j < freq; ++j) {
                position += termPos->nextDeltaPosition();
                positions.push_back(position);
            }
            result.positions.push_back(std::move(positions));
        }
    }
    return result;
}

// Compare TermDocs results
static bool compareTermDocsResults(const TermDocsResult& a, const TermDocsResult& b,
                                   bool checkNorms = false) {
    if (a.docs.size() != b.docs.size()) {
        std::cerr << "Doc count mismatch: " << a.docs.size() << " vs " << b.docs.size()
                  << std::endl;
        return false;
    }

    for (size_t i = 0; i < a.docs.size(); ++i) {
        if (a.docs[i] != b.docs[i]) {
            std::cerr << "Doc mismatch at " << i << ": " << a.docs[i] << " vs " << b.docs[i]
                      << std::endl;
            return false;
        }
        if (a.freqs[i] != b.freqs[i]) {
            std::cerr << "Freq mismatch at doc " << a.docs[i] << ": " << a.freqs[i] << " vs "
                      << b.freqs[i] << std::endl;
            return false;
        }
        if (checkNorms && a.norms[i] != b.norms[i]) {
            std::cerr << "Norm mismatch at doc " << a.docs[i] << ": " << a.norms[i] << " vs "
                      << b.norms[i] << std::endl;
            return false;
        }
    }
    return true;
}

// Compare TermPositions results
static bool compareTermPositionsResults(const TermPositionsResult& a,
                                        const TermPositionsResult& b) {
    if (a.docs.size() != b.docs.size()) {
        std::cerr << "Doc count mismatch: " << a.docs.size() << " vs " << b.docs.size()
                  << std::endl;
        return false;
    }

    for (size_t i = 0; i < a.docs.size(); ++i) {
        if (a.docs[i] != b.docs[i]) {
            std::cerr << "Doc mismatch at " << i << ": " << a.docs[i] << " vs " << b.docs[i]
                      << std::endl;
            return false;
        }
        if (a.freqs[i] != b.freqs[i]) {
            std::cerr << "Freq mismatch at doc " << a.docs[i] << ": " << a.freqs[i] << " vs "
                      << b.freqs[i] << std::endl;
            return false;
        }
        if (a.positions[i].size() != b.positions[i].size()) {
            std::cerr << "Position count mismatch at doc " << a.docs[i] << std::endl;
            return false;
        }
        for (size_t j = 0; j < a.positions[i].size(); ++j) {
            if (a.positions[i][j] != b.positions[i][j]) {
                std::cerr << "Position mismatch at doc " << a.docs[i] << " pos " << j << ": "
                          << a.positions[i][j] << " vs " << b.positions[i][j] << std::endl;
                return false;
            }
        }
    }
    return true;
}

//=============================================================================
// Test: Basic readRange correctness for TermDocs
//=============================================================================
void TestReadRangeBasic(CuTest* tc) {
    std::srand(getDaySeed());
    std::mt19937 rng(getDaySeed());

    std::string fieldName = "content";
    std::vector<std::string> datas;
    for (int32_t i = 0; i < DEFAULT_DOC_COUNT; ++i) {
        datas.push_back(generateRandomText(rng, 1, 5));
    }

    RAMDirectory dir;
    writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

    auto* reader = IndexReader::open(&dir);
    std::exception_ptr eptr;

    try {
        Term* term = nullptr;
        TermEnum* enumerator = reader->terms();

        int termCount = 0;
        while (enumerator->next() && termCount < 50) { // Test first 50 terms
            term = enumerator->term();
            if (term == nullptr) continue;

            // Read with next()
            TermDocs* termDocs1 = reader->termDocs();
            termDocs1->seek(term);
            TermDocsResult result1 = readWithNext(termDocs1);
            termDocs1->close();
            _CLDELETE(termDocs1);

            // Read with readRange()
            TermDocs* termDocs2 = reader->termDocs();
            termDocs2->seek(term);
            TermDocsResult result2 = readWithBlock(termDocs2);
            termDocs2->close();
            _CLDELETE(termDocs2);

            // Compare results
            bool match = compareTermDocsResults(result1, result2);
            if (!match) {
                char termStr[256];
                STRCPY_TtoA(termStr, term->text(), 255);
                std::cerr << "Mismatch for term: " << termStr << std::endl;
            }
            assertTrue(match);

            _CLDECDELETE(term);
            termCount++;
        }

        enumerator->close();
        _CLDELETE(enumerator);

    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        reader->close();
        _CLLDELETE(reader);
    })

    std::cout << "\nTestReadRangeBasic success" << std::endl;
}

//=============================================================================
// Test: readRange with positions (TermPositions)
//=============================================================================
void TestReadRangePositions(CuTest* tc) {
    std::srand(getDaySeed());
    std::mt19937 rng(getDaySeed());

    std::string fieldName = "content";
    std::vector<std::string> datas;
    for (int32_t i = 0; i < 1000; ++i) {
        datas.push_back(generateRandomText(rng, 2, 8)); // Multiple tokens per doc
    }

    RAMDirectory dir;
    writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

    auto* reader = IndexReader::open(&dir);
    std::exception_ptr eptr;

    try {
        Term* term = nullptr;
        TermEnum* enumerator = reader->terms();

        int termCount = 0;
        while (enumerator->next() && termCount < 20) {
            term = enumerator->term();
            if (term == nullptr) continue;

            // Read with next()/nextPosition()
            TermPositions* termPos1 = reader->termPositions();
            termPos1->seek(term);
            TermPositionsResult result1 = readPositionsWithNext(termPos1);
            termPos1->close();
            _CLDELETE(termPos1);

            // Read with readRange()/nextDeltaPosition()
            TermPositions* termPos2 = reader->termPositions();
            termPos2->seek(term);
            TermPositionsResult result2 = readPositionsWithBlock(termPos2);
            termPos2->close();
            _CLDELETE(termPos2);

            // Compare results
            bool match = compareTermPositionsResults(result1, result2);
            if (!match) {
                char termStr[256];
                STRCPY_TtoA(termStr, term->text(), 255);
                std::cerr << "Position mismatch for term: " << termStr << std::endl;
            }
            assertTrue(match);

            _CLDECDELETE(term);
            termCount++;
        }

        enumerator->close();
        _CLDELETE(enumerator);

    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        reader->close();
        _CLLDELETE(reader);
    })

    std::cout << "\nTestReadRangePositions success" << std::endl;
}

//=============================================================================
// Test: skipToBlock correctness
//=============================================================================
void TestSkipToBlock(CuTest* tc) {
    std::srand(getDaySeed());
    std::mt19937 rng(getDaySeed());

    std::string fieldName = "content";
    std::vector<std::string> datas;

    // Create index with a common term appearing in many docs
    std::string commonWord = "common";
    for (int32_t i = 0; i < DEFAULT_DOC_COUNT; ++i) {
        std::string text = generateRandomText(rng, 1, 3);
        if (i % 3 == 0) { // Add common word to every 3rd doc
            text += " " + commonWord;
        }
        datas.push_back(text);
    }

    RAMDirectory dir;
    writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

    auto* reader = IndexReader::open(&dir);
    std::exception_ptr eptr;

    try {
        std::wstring ws = StringUtil::string_to_wstring(commonWord);
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);
        Term* term = _CLNEW Term(fieldNameW.c_str(), ws.c_str());

        // Test skipTo with traditional method
        std::vector<int32_t> skipToTargets = {0, 100, 500, 1000, 5000, 9000};

        for (int32_t target : skipToTargets) {
            // Using skipTo
            TermDocs* termDocs1 = reader->termDocs();
            termDocs1->seek(term);
            std::vector<int32_t> docs1;
            if (termDocs1->skipTo(target)) {
                docs1.push_back(termDocs1->doc());
                while (termDocs1->next()) {
                    docs1.push_back(termDocs1->doc());
                }
            }
            termDocs1->close();
            _CLDELETE(termDocs1);

            // Using skipToBlock + readRange
            TermDocs* termDocs2 = reader->termDocs();
            termDocs2->seek(term);
            termDocs2->skipToBlock(target);
            std::vector<int32_t> docs2;
            DocRange docRange;
            while (termDocs2->readRange(&docRange)) {
                for (uint32_t i = 0; i < docRange.doc_many_size_; ++i) {
                    int32_t doc = (*docRange.doc_many)[i];
                    if (doc >= target) {
                        docs2.push_back(doc);
                    }
                }
            }
            termDocs2->close();
            _CLDELETE(termDocs2);

            // Compare: docs2 should contain at least all docs from docs1
            // (skipToBlock may return some docs before target within the same block)
            bool allFound = true;
            for (int32_t doc : docs1) {
                if (std::find(docs2.begin(), docs2.end(), doc) == docs2.end()) {
                    std::cerr << "Doc " << doc << " not found after skipToBlock(" << target << ")"
                              << std::endl;
                    allFound = false;
                }
            }
            assertTrue(allFound);
        }

        _CLDECDELETE(term);

    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        reader->close();
        _CLLDELETE(reader);
    })

    std::cout << "\nTestSkipToBlock success" << std::endl;
}

//=============================================================================
// Test: Performance comparison between next() and readRange()
//=============================================================================
void TestReadRangePerformance(CuTest* tc) {
    std::srand(getDaySeed());
    std::mt19937 rng(getDaySeed());

    std::string fieldName = "content";
    std::vector<std::string> datas;

    // Create larger dataset for performance testing
    int32_t perfDocCount = 50000;
    for (int32_t i = 0; i < perfDocCount; ++i) {
        datas.push_back(generateRandomText(rng, 1, 5));
    }

    RAMDirectory dir;
    writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

    auto* reader = IndexReader::open(&dir);
    std::exception_ptr eptr;

    try {
        // Collect all terms
        std::vector<Term*> terms;
        TermEnum* enumerator = reader->terms();
        while (enumerator->next()) {
            Term* term = enumerator->term();
            if (term != nullptr) {
                terms.push_back(term);
            }
        }
        enumerator->close();
        _CLDELETE(enumerator);

        // Benchmark next() method
        auto startNext = std::chrono::high_resolution_clock::now();
        int64_t totalDocsNext = 0;
        for (int iter = 0; iter < PERF_ITERATIONS; ++iter) {
            for (Term* term : terms) {
                TermDocs* termDocs = reader->termDocs();
                termDocs->seek(term);
                while (termDocs->next()) {
                    totalDocsNext++;
                }
                termDocs->close();
                _CLDELETE(termDocs);
            }
        }
        auto endNext = std::chrono::high_resolution_clock::now();
        auto durationNext =
                std::chrono::duration_cast<std::chrono::milliseconds>(endNext - startNext);

        // Benchmark readRange() method
        auto startRange = std::chrono::high_resolution_clock::now();
        int64_t totalDocsRange = 0;
        for (int iter = 0; iter < PERF_ITERATIONS; ++iter) {
            for (Term* term : terms) {
                TermDocs* termDocs = reader->termDocs();
                termDocs->seek(term);
                DocRange docRange;
                while (termDocs->readRange(&docRange)) {
                    totalDocsRange += docRange.doc_many_size_;
                }
                termDocs->close();
                _CLDELETE(termDocs);
            }
        }
        auto endRange = std::chrono::high_resolution_clock::now();
        auto durationRange =
                std::chrono::duration_cast<std::chrono::milliseconds>(endRange - startRange);

        // Verify same number of docs read
        assertEquals(totalDocsNext, totalDocsRange);

        std::cout << "\n=== Performance Results ===" << std::endl;
        std::cout << "Terms: " << terms.size() << ", Iterations: " << PERF_ITERATIONS << std::endl;
        std::cout << "next() method:      " << durationNext.count() << " ms" << std::endl;
        std::cout << "readRange() method: " << durationRange.count() << " ms" << std::endl;
        std::cout << "Speedup: " << (double)durationNext.count() / durationRange.count() << "x"
                  << std::endl;

        // readRange should not be significantly slower
        // Allow up to 2x slower as acceptable (it should actually be faster in most cases)
        assertTrue(durationRange.count() <= durationNext.count() * 2);

        // Cleanup terms
        for (Term* term : terms) {
            _CLDECDELETE(term);
        }

    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        reader->close();
        _CLLDELETE(reader);
    })

    std::cout << "TestReadRangePerformance success" << std::endl;
}

//=============================================================================
// Test: Large document count stress test
//=============================================================================
void TestReadRangeLargeDataset(CuTest* tc) {
    std::srand(getDaySeed());
    std::mt19937 rng(getDaySeed());

    std::string fieldName = "content";
    std::vector<std::string> datas;

    // Use a small vocabulary to ensure high term frequency
    std::vector<std::string> vocabulary = {"alpha", "beta", "gamma", "delta", "epsilon"};

    int32_t largeDocCount = 100000;
    for (int32_t i = 0; i < largeDocCount; ++i) {
        std::uniform_int_distribution<size_t> dist(0, vocabulary.size() - 1);
        std::string text = vocabulary[dist(rng)];
        // Add 1-3 more words
        int extraWords = (i % 3) + 1;
        for (int j = 0; j < extraWords; ++j) {
            text += " " + vocabulary[dist(rng)];
        }
        datas.push_back(text);
    }

    RAMDirectory dir;
    writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

    auto* reader = IndexReader::open(&dir);
    std::exception_ptr eptr;

    try {
        // Test each vocabulary term
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);

        for (const std::string& word : vocabulary) {
            std::wstring ws = StringUtil::string_to_wstring(word);
            Term* term = _CLNEW Term(fieldNameW.c_str(), ws.c_str());

            // Read with next()
            TermDocs* termDocs1 = reader->termDocs();
            termDocs1->seek(term);
            TermDocsResult result1 = readWithNext(termDocs1);
            termDocs1->close();
            _CLDELETE(termDocs1);

            // Read with readRange()
            TermDocs* termDocs2 = reader->termDocs();
            termDocs2->seek(term);
            TermDocsResult result2 = readWithBlock(termDocs2);
            termDocs2->close();
            _CLDELETE(termDocs2);

            // Compare
            bool match = compareTermDocsResults(result1, result2);
            if (!match) {
                std::cerr << "Mismatch for term: " << word << std::endl;
            }
            assertTrue(match);

            std::cout << "Term '" << word << "': " << result1.docs.size() << " docs - OK"
                      << std::endl;

            _CLDECDELETE(term);
        }

    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        reader->close();
        _CLLDELETE(reader);
    })

    std::cout << "\nTestReadRangeLargeDataset success" << std::endl;
}

//=============================================================================
// Test: Index version compatibility (V1 and V2)
//=============================================================================
void TestReadRangeVersions(CuTest* tc) {
    std::srand(getDaySeed());
    std::mt19937 rng(getDaySeed());

    std::string fieldName = "content";
    std::vector<std::string> datas;
    for (int32_t i = 0; i < 5000; ++i) {
        datas.push_back(generateRandomText(rng, 1, 4));
    }

    std::vector<IndexVersion> versions = {IndexVersion::kV1, IndexVersion::kV2};

    for (IndexVersion version : versions) {
        RAMDirectory dir;
        writeTestIndex(fieldName, &dir, version, datas);

        auto* reader = IndexReader::open(&dir);
        std::exception_ptr eptr;

        try {
            Term* term = nullptr;
            TermEnum* enumerator = reader->terms();

            int termCount = 0;
            while (enumerator->next() && termCount < 30) {
                term = enumerator->term();
                if (term == nullptr) continue;

                // Read with next()
                TermDocs* termDocs1 = reader->termDocs();
                termDocs1->seek(term);
                TermDocsResult result1 = readWithNext(termDocs1);
                termDocs1->close();
                _CLDELETE(termDocs1);

                // Read with readRange()
                TermDocs* termDocs2 = reader->termDocs();
                termDocs2->seek(term);
                TermDocsResult result2 = readWithBlock(termDocs2);
                termDocs2->close();
                _CLDELETE(termDocs2);

                bool match = compareTermDocsResults(result1, result2);
                assertTrue(match);

                _CLDECDELETE(term);
                termCount++;
            }

            enumerator->close();
            _CLDELETE(enumerator);

        } catch (...) {
            eptr = std::current_exception();
        }

        FINALLY(eptr, {
            reader->close();
            _CLLDELETE(reader);
        })

        std::cout << "IndexVersion " << static_cast<int>(version) << " - OK" << std::endl;
    }

    std::cout << "\nTestReadRangeVersions success" << std::endl;
}

//=============================================================================
// Test: Edge cases - empty results, single doc, etc.
//=============================================================================
void TestReadRangeEdgeCases(CuTest* tc) {
    std::srand(getDaySeed());

    std::string fieldName = "content";

    // Test 1: Single document
    {
        RAMDirectory dir;
        std::vector<std::string> datas = {"single"};
        writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

        auto* reader = IndexReader::open(&dir);
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);
        std::wstring ws = StringUtil::string_to_wstring("single");
        Term* term = _CLNEW Term(fieldNameW.c_str(), ws.c_str());

        TermDocs* termDocs1 = reader->termDocs();
        termDocs1->seek(term);
        TermDocsResult result1 = readWithNext(termDocs1);
        termDocs1->close();
        _CLDELETE(termDocs1);

        TermDocs* termDocs2 = reader->termDocs();
        termDocs2->seek(term);
        TermDocsResult result2 = readWithBlock(termDocs2);
        termDocs2->close();
        _CLDELETE(termDocs2);

        assertTrue(compareTermDocsResults(result1, result2));
        assertEquals(result1.docs.size(), 1);

        _CLDECDELETE(term);
        reader->close();
        _CLLDELETE(reader);

        std::cout << "Single doc test - OK" << std::endl;
    }

    // Test 2: Non-existent term
    {
        RAMDirectory dir;
        std::vector<std::string> datas = {"apple", "banana", "cherry"};
        writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

        auto* reader = IndexReader::open(&dir);
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);
        std::wstring ws = StringUtil::string_to_wstring("nonexistent");
        Term* term = _CLNEW Term(fieldNameW.c_str(), ws.c_str());

        TermDocs* termDocs = reader->termDocs();
        termDocs->seek(term);
        DocRange docRange;
        bool hasData = termDocs->readRange(&docRange);
        // Non-existent term should return false or empty range
        assertTrue(!hasData || docRange.doc_many_size_ == 0);
        termDocs->close();
        _CLDELETE(termDocs);

        _CLDECDELETE(term);
        reader->close();
        _CLLDELETE(reader);

        std::cout << "Non-existent term test - OK" << std::endl;
    }

    // Test 3: Term appearing in all documents
    {
        RAMDirectory dir;
        std::vector<std::string> datas;
        for (int i = 0; i < 1000; ++i) {
            datas.push_back("common");
        }
        writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

        auto* reader = IndexReader::open(&dir);
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);
        std::wstring ws = StringUtil::string_to_wstring("common");
        Term* term = _CLNEW Term(fieldNameW.c_str(), ws.c_str());

        TermDocs* termDocs1 = reader->termDocs();
        termDocs1->seek(term);
        TermDocsResult result1 = readWithNext(termDocs1);
        termDocs1->close();
        _CLDELETE(termDocs1);

        TermDocs* termDocs2 = reader->termDocs();
        termDocs2->seek(term);
        TermDocsResult result2 = readWithBlock(termDocs2);
        termDocs2->close();
        _CLDELETE(termDocs2);

        assertTrue(compareTermDocsResults(result1, result2));
        assertEquals(result1.docs.size(), 1000);

        _CLDECDELETE(term);
        reader->close();
        _CLLDELETE(reader);

        std::cout << "All docs term test - OK" << std::endl;
    }

    std::cout << "\nTestReadRangeEdgeCases success" << std::endl;
}

//=============================================================================
// Test: Block-based interfaces (readBlock, skipToBlock, getMaxBlockFreq,
//       getMaxBlockNorm, getLastDocInBlock) working together
//=============================================================================
void TestBlockBasedInterfaces(CuTest* tc) {
    std::srand(getDaySeed());
    std::mt19937 rng(getDaySeed());

    std::string fieldName = "content";
    std::vector<std::string> datas;

    // Create index with a common term appearing frequently
    std::string commonWord = "target";
    for (int32_t i = 0; i < DEFAULT_DOC_COUNT; ++i) {
        std::string text = generateRandomText(rng, 1, 5);
        if (i % 2 == 0) {
            text += " " + commonWord;
            // Add multiple occurrences for higher freq
            if (i % 4 == 0) {
                text += " " + commonWord + " " + commonWord;
            }
        }
        datas.push_back(text);
    }

    RAMDirectory dir;
    writeTestIndex(fieldName, &dir, IndexVersion::kV2, datas);

    auto* reader = IndexReader::open(&dir);
    std::exception_ptr eptr;

    try {
        std::wstring ws = StringUtil::string_to_wstring(commonWord);
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);
        Term* term = _CLNEW Term(fieldNameW.c_str(), ws.c_str());

        // Collect all docs using next() as baseline
        TermDocs* baselineDocs = reader->termDocs();
        baselineDocs->seek(term);
        std::vector<int32_t> baselineDocIds;
        std::vector<int32_t> baselineFreqs;
        while (baselineDocs->next()) {
            baselineDocIds.push_back(baselineDocs->doc());
            baselineFreqs.push_back(baselineDocs->freq());
        }
        baselineDocs->close();
        _CLDELETE(baselineDocs);

        // Test: skipToBlock + readBlock + getMaxBlockFreq/getMaxBlockNorm/getLastDocInBlock
        std::vector<int32_t> skipTargets = {0, 500, 2000, 5000, 8000};

        for (int32_t target : skipTargets) {
            TermDocs* termDocs = reader->termDocs();
            termDocs->seek(term);

            std::vector<int32_t> collectedDocs;
            std::vector<int32_t> collectedFreqs;

            // Skip to target block
            bool skipped = termDocs->skipToBlock(target);

            // Verify getLastDocInBlock returns valid value after skip
            int32_t lastDocInBlock = termDocs->getLastDocInBlock();
            if (skipped && lastDocInBlock >= 0) {
                assertTrue(lastDocInBlock >= 0);
            }

            // Read blocks and verify maxBlockFreq/maxBlockNorm
            DocRange docRange;
            while (termDocs->readBlock(&docRange)) {
                int32_t maxFreq = termDocs->getMaxBlockFreq();
                int32_t maxNorm = termDocs->getMaxBlockNorm();
                lastDocInBlock = termDocs->getLastDocInBlock();

                // Verify maxBlockFreq is >= max freq in current block
                int32_t actualMaxFreq = 0;
                for (uint32_t i = 0; i < docRange.doc_many_size_; ++i) {
                    int32_t doc = (*docRange.doc_many)[i];
                    int32_t freq = (docRange.freq_many && i < docRange.freq_many_size_)
                                           ? (*docRange.freq_many)[i]
                                           : 1;
                    if (doc >= target) {
                        collectedDocs.push_back(doc);
                        collectedFreqs.push_back(freq);
                    }
                    if (freq > actualMaxFreq) {
                        actualMaxFreq = freq;
                    }
                }

                // maxBlockFreq should be >= actual max freq in block (if valid)
                if (maxFreq > 0) {
                    assertTrue(maxFreq >= actualMaxFreq);
                }

                // lastDocInBlock should be >= last doc in current block
                if (lastDocInBlock >= 0 && docRange.doc_many_size_ > 0) {
                    int32_t lastDocInRange = (*docRange.doc_many)[docRange.doc_many_size_ - 1];
                    assertTrue(lastDocInBlock >= lastDocInRange);
                }
            }

            // Verify collected docs match baseline (docs >= target)
            std::vector<int32_t> expectedDocs;
            for (size_t i = 0; i < baselineDocIds.size(); ++i) {
                if (baselineDocIds[i] >= target) {
                    expectedDocs.push_back(baselineDocIds[i]);
                }
            }

            // All expected docs should be in collected docs
            for (int32_t expectedDoc : expectedDocs) {
                bool found = std::find(collectedDocs.begin(), collectedDocs.end(), expectedDoc) !=
                             collectedDocs.end();
                if (!found) {
                    std::cerr << "Missing doc " << expectedDoc << " after skipToBlock(" << target
                              << ")" << std::endl;
                }
                assertTrue(found);
            }

            termDocs->close();
            _CLDELETE(termDocs);
        }

        _CLDECDELETE(term);

    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        reader->close();
        _CLLDELETE(reader);
    })

    std::cout << "\nTestBlockBasedInterfaces success" << std::endl;
}

//=============================================================================
// Test: block APIs on MultiTermDocs through MultiReader
//=============================================================================
void TestMultiReaderBlockApis(CuTest* tc) {
    constexpr int32_t firstReaderDocumentCount = 1149;
    constexpr int32_t secondReaderDocumentCount = 1134;
    const std::string fieldName = "content";
    const std::string termText = "target";

    std::vector<std::string> firstDocuments;
    std::vector<std::string> secondDocuments;
    firstDocuments.reserve(firstReaderDocumentCount);
    secondDocuments.reserve(secondReaderDocumentCount);
    for (int32_t i = 0; i < firstReaderDocumentCount; ++i) {
        firstDocuments.emplace_back(termText + " " + termText);
    }
    for (int32_t i = 0; i < secondReaderDocumentCount; ++i) {
        secondDocuments.emplace_back(termText + " " + termText + " " + termText);
    }

    RAMDirectory firstDirectory;
    RAMDirectory secondDirectory;
    writeTestIndex(fieldName, &firstDirectory, IndexVersion::kV4, firstDocuments);
    writeTestIndex(fieldName, &secondDirectory, IndexVersion::kV4, secondDocuments);

    ValueArray<IndexReader*> readers(2);
    readers[0] = IndexReader::open(&firstDirectory);
    readers[1] = IndexReader::open(&secondDirectory);
    auto* reader = _CLNEW MultiReader(&readers, true);
    std::exception_ptr eptr;
    Term* term = nullptr;

    try {
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);
        std::wstring termTextW = StringUtil::string_to_wstring(termText);
        term = _CLNEW Term(fieldNameW.c_str(), termTextW.c_str());

        TermDocs* nextDocs = reader->termDocs();
        nextDocs->seek(term);
        TermDocsResult expected = readWithNext(nextDocs);
        nextDocs->close();
        _CLDELETE(nextDocs);

        TermDocs* blockDocs = reader->termDocs();
        blockDocs->seek(term);
        TermDocsResult actual;
        DocRange docRange;
        bool sawSecondReader = false;
        bool sawBlockMetadata = false;
        bool sawSecondReaderBlockBoundary = false;
        bool blockDocsAreOrdered = true;
        bool blockMetadataIsValid = true;
        int32_t previousDoc = -1;
        std::vector<uint32_t> blockSizes;
        while (blockDocs->readBlock(&docRange)) {
            blockDocsAreOrdered = blockDocsAreOrdered && docRange.doc_many_size_ > 0;
            blockSizes.push_back(docRange.doc_many_size_);

            int32_t actualMaxFreq = 0;
            for (uint32_t i = 0; i < docRange.doc_many_size_; ++i) {
                int32_t doc = (*docRange.doc_many)[i];
                int32_t freq = (*docRange.freq_many)[i];
                blockDocsAreOrdered = blockDocsAreOrdered && doc > previousDoc;
                previousDoc = doc;
                actual.docs.push_back(doc);
                actual.freqs.push_back(freq);
                actualMaxFreq = std::max(actualMaxFreq, freq);
                sawSecondReader = sawSecondReader || doc >= firstReaderDocumentCount;
            }

            int32_t maxBlockFreq = blockDocs->getMaxBlockFreq();
            int32_t maxBlockNorm = blockDocs->getMaxBlockNorm();
            int32_t lastDocInBlock = blockDocs->getLastDocInBlock();
            if (maxBlockFreq >= 0 && maxBlockNorm >= 0) {
                sawBlockMetadata = true;
                blockMetadataIsValid = blockMetadataIsValid && maxBlockFreq >= actualMaxFreq;
            }
            if (lastDocInBlock >= 0) {
                const int32_t lastDocInRange =
                        (*docRange.doc_many)[docRange.doc_many_size_ - 1];
                blockMetadataIsValid =
                        blockMetadataIsValid &&
                        lastDocInBlock >= lastDocInRange;
                sawSecondReaderBlockBoundary = sawSecondReaderBlockBoundary ||
                                               (*docRange.doc_many)[0] >= firstReaderDocumentCount;
            }
        }
        blockDocs->close();
        _CLDELETE(blockDocs);

        assertTrueMsg(_T("all composite block docs must match next()"),
                      compareTermDocsResults(expected, actual));
        assertTrueMsg(_T("composite block docs must remain ordered"), blockDocsAreOrdered);
        assertTrueMsg(_T("composite block read must reach the second reader"), sawSecondReader);
        assertTrueMsg(_T("composite block metadata must be available"), sawBlockMetadata);
        assertTrueMsg(_T("composite block metadata must cross the reader boundary"),
                      sawSecondReaderBlockBoundary);
        assertTrueMsg(_T("block metadata must cover the returned physical block"),
                      blockMetadataIsValid);

        const std::vector<uint32_t> expectedBlockSizes = {511, 512, 126, 511, 512, 111};
        assertTrueMsg(_T("composite reader must preserve short physical tail blocks"),
                      blockSizes == expectedBlockSizes);

        for (int32_t target : {1023, firstReaderDocumentCount - 1, firstReaderDocumentCount,
                               firstReaderDocumentCount + 37}) {
            TermDocs* skippedDocs = reader->termDocs();
            skippedDocs->seek(term);
            skippedDocs->skipToBlock(target);

            std::vector<int32_t> actualTail;
            DocRange skippedRange;
            while (skippedDocs->readBlock(&skippedRange)) {
                for (uint32_t i = 0; i < skippedRange.doc_many_size_; ++i) {
                    int32_t doc = (*skippedRange.doc_many)[i];
                    if (target >= firstReaderDocumentCount) {
                        assertTrue(doc >= firstReaderDocumentCount);
                    }
                    if (doc >= target) {
                        actualTail.push_back(doc);
                    }
                }
            }
            skippedDocs->close();
            _CLDELETE(skippedDocs);

            std::vector<int32_t> expectedTail;
            for (int32_t doc : expected.docs) {
                if (doc >= target) {
                    expectedTail.push_back(doc);
                }
            }
            assertTrue(actualTail == expectedTail);
        }
    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        _CLDECDELETE(term);
        reader->close();
        _CLDELETE(reader);
    })

    std::cout << "\nTestMultiReaderBlockApis success" << std::endl;
}

//=============================================================================
// Test: changing child readers invalidates block state even when the new child
// has fewer postings than a skip block.
//=============================================================================
void TestMultiReaderBlockSkipAcrossShortReaders(CuTest* tc) {
    constexpr int32_t firstReaderDocumentCount = 128;
    constexpr int32_t secondReaderDocumentCount = 11;
    const std::string fieldName = "content";
    const std::string termText = "target";

    std::vector<std::string> firstDocuments(firstReaderDocumentCount, termText);
    std::vector<std::string> secondDocuments(secondReaderDocumentCount, termText);
    RAMDirectory firstDirectory;
    RAMDirectory secondDirectory;
    writeTestIndex(fieldName, &firstDirectory, IndexVersion::kV4, firstDocuments);
    writeTestIndex(fieldName, &secondDirectory, IndexVersion::kV4, secondDocuments);

    ValueArray<IndexReader*> readers(2);
    readers[0] = IndexReader::open(&firstDirectory);
    readers[1] = IndexReader::open(&secondDirectory);
    auto* reader = _CLNEW MultiReader(&readers, true);
    std::exception_ptr eptr;
    Term* term = nullptr;

    try {
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);
        std::wstring termTextW = StringUtil::string_to_wstring(termText);
        term = _CLNEW Term(fieldNameW.c_str(), termTextW.c_str());

        TermDocs* termDocs = reader->termDocs();
        termDocs->seek(term);

        DocRange firstRange;
        assertTrue(termDocs->readBlock(&firstRange));
        assertEquals(firstReaderDocumentCount, firstRange.doc_many_size_);
        assertTrue(!termDocs->skipToBlock(firstReaderDocumentCount - 1));

        // The second child has no skip list, so only MultiTermDocs can report
        // the state transition that invalidates a WAND block cache.
        assertTrue(termDocs->skipToBlock(firstReaderDocumentCount));

        DocRange secondRange;
        assertTrue(termDocs->readBlock(&secondRange));
        assertEquals(secondReaderDocumentCount, secondRange.doc_many_size_);
        for (uint32_t i = 0; i < secondRange.doc_many_size_; ++i) {
            assertEquals(firstReaderDocumentCount + static_cast<int32_t>(i),
                         (*secondRange.doc_many)[i]);
        }
        assertTrue(!termDocs->readBlock(&secondRange));

        termDocs->close();
        _CLDELETE(termDocs);
    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        _CLDECDELETE(term);
        reader->close();
        _CLDELETE(reader);
    })

    std::cout << "\nTestMultiReaderBlockSkipAcrossShortReaders success" << std::endl;
}

//=============================================================================
// Test: after skipping a block in a large child, entering a short child still
// reports the reader transition to a block-WAND caller.
//=============================================================================
void TestMultiReaderBlockSkipIntoShortReader(CuTest* tc) {
    constexpr int32_t firstReaderDocumentCount = 1149;
    constexpr int32_t secondReaderDocumentCount = 11;
    const std::string fieldName = "content";
    const std::string termText = "target";

    std::vector<std::string> firstDocuments(firstReaderDocumentCount, termText);
    std::vector<std::string> secondDocuments(secondReaderDocumentCount, termText);
    RAMDirectory firstDirectory;
    RAMDirectory secondDirectory;
    writeTestIndex(fieldName, &firstDirectory, IndexVersion::kV4, firstDocuments);
    writeTestIndex(fieldName, &secondDirectory, IndexVersion::kV4, secondDocuments);

    ValueArray<IndexReader*> readers(2);
    readers[0] = IndexReader::open(&firstDirectory);
    readers[1] = IndexReader::open(&secondDirectory);
    auto* reader = _CLNEW MultiReader(&readers, true);
    std::exception_ptr eptr;
    Term* term = nullptr;

    try {
        std::wstring fieldNameW = StringUtil::string_to_wstring(fieldName);
        std::wstring termTextW = StringUtil::string_to_wstring(termText);
        term = _CLNEW Term(fieldNameW.c_str(), termTextW.c_str());

        TermDocs* termDocs = reader->termDocs();
        termDocs->seek(term);

        // This skip enters the first reader's final short physical block.
        assertTrue(termDocs->skipToBlock(1023));
        // This one changes readers; the new reader has df < skipInterval.
        assertTrue(termDocs->skipToBlock(firstReaderDocumentCount));

        DocRange shortReaderRange;
        assertTrue(termDocs->readBlock(&shortReaderRange));
        assertEquals(secondReaderDocumentCount, shortReaderRange.doc_many_size_);
        for (uint32_t i = 0; i < shortReaderRange.doc_many_size_; ++i) {
            assertEquals(firstReaderDocumentCount + static_cast<int32_t>(i),
                         (*shortReaderRange.doc_many)[i]);
        }
        assertTrue(!termDocs->readBlock(&shortReaderRange));

        termDocs->close();
        _CLDELETE(termDocs);
    } catch (...) {
        eptr = std::current_exception();
    }

    FINALLY(eptr, {
        _CLDECDELETE(term);
        reader->close();
        _CLDELETE(reader);
    })

    std::cout << "\nTestMultiReaderBlockSkipIntoShortReader success" << std::endl;
}

//=============================================================================
// Suite registration
//=============================================================================
CuSuite* testReadRange() {
    CuSuite* suite = CuSuiteNew(_T("CLucene ReadRange Test"));

    SUITE_ADD_TEST(suite, TestReadRangeBasic);
    SUITE_ADD_TEST(suite, TestReadRangePositions);
    SUITE_ADD_TEST(suite, TestSkipToBlock);
    SUITE_ADD_TEST(suite, TestReadRangePerformance);
    SUITE_ADD_TEST(suite, TestReadRangeLargeDataset);
    SUITE_ADD_TEST(suite, TestReadRangeVersions);
    SUITE_ADD_TEST(suite, TestReadRangeEdgeCases);
    SUITE_ADD_TEST(suite, TestBlockBasedInterfaces);
    SUITE_ADD_TEST(suite, TestMultiReaderBlockApis);
    SUITE_ADD_TEST(suite, TestMultiReaderBlockSkipAcrossShortReaders);
    SUITE_ADD_TEST(suite, TestMultiReaderBlockSkipIntoShortReader);

    return suite;
}
