#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/index/IndexReader.h>
#include <CLucene/search/query/TermPositionIterator.h>
#include <CLucene/util/stringUtil.h>

#include <cstddef>
#include <ctime>
#include <exception>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "CLucene/analysis/Analyzers.h"
#include "CLucene/index/FieldConfig.h"
#include "CLucene/index/IndexVersion.h"
#include "CLucene/index/Term.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/store/_RAMDirectory.h"
#include "CLucene/store/store_v2/ByteArrayDataInput.h"
#include "CLucene/store/store_v2/GrowableByteArrayDataOutput.h"
#include "CuTest.h"
#include "test.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)
CL_NS_USE(util)

static constexpr int32_t doc_count = 100;

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

static std::string generateRandomIP() {
    std::string ip_v4;
    ip_v4.append(std::to_string(rand() % 256));
    ip_v4.append(".");
    ip_v4.append(std::to_string(rand() % 256));
    ip_v4.append(".");
    ip_v4.append(std::to_string(rand() % 256));
    ip_v4.append(".");
    ip_v4.append(std::to_string(rand() % 256));
    return ip_v4;
}

static void write_index(const std::string& name, RAMDirectory* dir,
                        const std::vector<std::string>& datas) {
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
    field_config |= lucene::document::Field::INDEX_NONORMS;
    field_config |= lucene::document::Field::INDEX_TOKENIZED;
    auto field_name = std::wstring(name.begin(), name.end());
    auto* field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
    field->setOmitTermFreqAndPositions(false);
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

struct MockIOContxt {
    int64_t count = 0;
};

void TestIndexRead(CuTest* tc) {
    std::srand(getDaySeed());

    std::string name = "name";
    std::vector<std::string> datas;
    datas.push_back("a1");
    datas.push_back("a2");
    datas.push_back("a3");
    datas.push_back("a4");
    datas.push_back("a5");
    datas.push_back("a6");
    datas.push_back("a7");
    datas.push_back("a8");
    datas.push_back("a9");

    RAMDirectory dir;
    write_index(name, &dir, datas);

    {
        auto* reader = IndexReader::open(&dir);

        MockIOContxt io_ctx;
        TermEnum* enumerator = reader->terms(&io_ctx);

        int32_t count = 0;
        Term* lastTerm = nullptr;
        try {
            do {
                lastTerm = enumerator->term();
                if (lastTerm != nullptr) {
                    count++;
                }
                _CLDECDELETE(lastTerm);
            } while (enumerator->next());
        }
        _CLFINALLY({
            enumerator->close();
            _CLDELETE(enumerator);
        });
        assertEquals(count, 10);

        reader->close();
        _CLLDELETE(reader);
    }

    std::cout << "\nTestIndexRead sucess" << std::endl;
}

void TestIndexReadSeek(CuTest* tc) {
    std::srand(getDaySeed());

    std::string name = "name";
    std::vector<std::string> datas;
    datas.push_back("a1");
    datas.push_back("a2");
    datas.push_back("a3");
    datas.push_back("a4");
    datas.push_back("a5");
    datas.push_back("a6");
    datas.push_back("a7");
    datas.push_back("a8");
    datas.push_back("a9");

    RAMDirectory dir;
    write_index(name, &dir, datas);

    {
        auto* reader = IndexReader::open(&dir);

        std::wstring ws_prefix = StringUtil::string_to_wstring("a5");
        Term* prefix_term = _CLNEW Term(L"name", ws_prefix.c_str());

        MockIOContxt io_ctx;
        TermEnum* enumerator = reader->terms(prefix_term, &io_ctx);

        int32_t count = 0;
        Term* lastTerm = nullptr;
        try {
            do {
                lastTerm = enumerator->term();
                if (lastTerm != nullptr) {
                    count++;
                }
                _CLDECDELETE(lastTerm);
            } while (enumerator->next());
        }
        _CLFINALLY({
            enumerator->close();
            _CLDELETE(enumerator);
            _CLDECDELETE(prefix_term);
        });
        assertEquals(count, 5);

        reader->close();
        _CLLDELETE(reader);
    }

    std::cout << "\nTestIndexReadSeek sucess" << std::endl;
}

CuSuite* testIndexReader2() {
    CuSuite* suite = CuSuiteNew(_T("CLucene Index Reader Test"));

    SUITE_ADD_TEST(suite, TestIndexRead);
    SUITE_ADD_TEST(suite, TestIndexReadSeek);

    return suite;
}
