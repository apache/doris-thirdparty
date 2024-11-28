#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/index/IndexReader.h>
#include <CLucene/search/query/TermPositionIterator.h>
#include <CLucene/util/stringUtil.h>

#include <ctime>
#include <exception>
#include <stdexcept>
#include <string>
#include <vector>

#include "CLucene/analysis/Analyzers.h"
#include "CLucene/index/IndexVersion.h"
#include "CLucene/index/Term.h"
#include "CLucene/store/FSDirectory.h"
#include "test.h"

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)
CL_NS_USE(util)

static constexpr int32_t doc_count = 10000;

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

static void write_index(const std::string& name, RAMDirectory* dir, IndexVersion index_version,
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
    field->setIndexVersion(index_version);
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

static void read_index(RAMDirectory* dir, int32_t doc_count) {
    auto* reader = IndexReader::open(dir);

    std::exception_ptr eptr;
    try {
        if (doc_count != reader->numDocs()) {
            std::string msg = "doc_count: " + std::to_string(doc_count) +
                              ", numDocs: " + std::to_string(reader->numDocs());
            _CLTHROWA(CL_ERR_IllegalArgument, msg.c_str());
        }

        Term* term = nullptr;
        TermEnum* enumerator = nullptr;
        try {
            enumerator = reader->terms();
            while (enumerator->next()) {
                term = enumerator->term();

                auto* term_pos = reader->termPositions(term);

                std::exception_ptr eptr;
                try {
                    TermPositionIterator iter(term_pos);
                    int32_t doc = 0;
                    while ((doc = iter.nextDoc()) != INT32_MAX) {
                        for (int32_t i = 0; i < iter.freq(); i++) {
                            int32_t pos = iter.nextPosition();
                            if (pos < 0 || pos > 3) {
                                std::string msg = "pos: " + std::to_string(pos);
                                _CLTHROWA(CL_ERR_IllegalArgument, msg.c_str());
                            }
                        }
                    }
                } catch (...) {
                    eptr = std::current_exception();
                }
                FINALLY(eptr, { _CLDELETE(term_pos); })

                _CLDECDELETE(term);
            }
        }
        _CLFINALLY({
            _CLDECDELETE(term);
            enumerator->close();
            _CLDELETE(enumerator);
        })

    } catch (...) {
        eptr = std::current_exception();
    }
    FINALLY(eptr, {
        reader->close();
        _CLLDELETE(reader);
    })
}

static void index_compaction(RAMDirectory* tmp_dir, std::vector<lucene::store::Directory*> srcDirs,
                             std::vector<lucene::store::Directory*> destDirs, int32_t count) {
    auto* analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>;
    auto* indexwriter = _CLNEW lucene::index::IndexWriter(tmp_dir, analyzer, true);

    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec(
            srcDirs.size(), std::vector<std::pair<uint32_t, uint32_t>>(count));
    int32_t idx = 0;
    int32_t id = 0;
    for (int32_t i = 0; i < count; i++) {
        for (int32_t j = 0; j < srcDirs.size(); j++) {
            if (id == count * destDirs.size()) {
                idx++;
                id = 0;
            }
            trans_vec[j][i] = std::make_pair(idx, id++);
        }
    }

    std::vector<uint32_t> dest_index_docs(destDirs.size());
    for (int32_t i = 0; i < destDirs.size(); i++) {
        dest_index_docs[i] = count * destDirs.size();
    }

    std::exception_ptr eptr;
    try {
        indexwriter->indexCompaction(srcDirs, destDirs, trans_vec, dest_index_docs);
    } catch (...) {
        eptr = std::current_exception();
    }
    FINALLY(eptr, {
        indexwriter->close();
        _CLDELETE(indexwriter);
        _CLDELETE(analyzer);
    })
}

void TestIndexCompressV2(CuTest* tc) {
    std::srand(getDaySeed());

    std::string name = "v2_field_name";
    std::vector<std::string> datas;
    for (int32_t i = 0; i < doc_count; i++) {
        std::string ip_v4 = generateRandomIP();
        datas.emplace_back(ip_v4);
    }

    RAMDirectory dir;
    write_index(name, &dir, IndexVersion::kV2, datas);

    try {
        read_index(&dir, doc_count);
    } catch (...) {
        assertTrue(false);
    }

    std::cout << "\nTestIndexCompressV2 sucess" << std::endl;
}

void TestIndexCompactionV2(CuTest* tc) {
    std::srand(getDaySeed());
    std::string name = "field_name";

    // index v2
    RAMDirectory in_dir;
    {
        std::vector<std::string> datas;
        for (int32_t i = 0; i < doc_count; i++) {
            std::string ip_v4 = generateRandomIP();
            datas.emplace_back(ip_v4);
        }
        write_index(name, &in_dir, IndexVersion::kV2, datas);
    }

    // index compaction v3
    RAMDirectory outdir1;
    RAMDirectory outdir2;
    RAMDirectory outdir3;
    {
        std::vector<lucene::store::Directory*> srcDirs;
        srcDirs.push_back(&in_dir);
        srcDirs.push_back(&in_dir);
        srcDirs.push_back(&in_dir);
        srcDirs.push_back(&in_dir);
        srcDirs.push_back(&in_dir);
        srcDirs.push_back(&in_dir);
        srcDirs.push_back(&in_dir);
        srcDirs.push_back(&in_dir);
        srcDirs.push_back(&in_dir);
        std::vector<lucene::store::Directory*> destDirs;
        destDirs.push_back(&outdir1);
        destDirs.push_back(&outdir2);
        destDirs.push_back(&outdir3);

        try {
            RAMDirectory empty_dir;
            index_compaction(&empty_dir, srcDirs, destDirs, doc_count);
        } catch (...) {
            assertTrue(false);
        }
    }

    std::cout << "TestIndexCompactionV2 sucess" << std::endl;
}

void TestIndexCompactionException(CuTest* tc) {
    std::srand(getDaySeed());
    std::string name = "field_name";

    // index v1
    RAMDirectory in_dir_v1;
    {
        std::vector<std::string> datas;
        for (int32_t i = 0; i < 10; i++) {
            std::string ip_v4 = generateRandomIP();
            datas.emplace_back(ip_v4);
        }
        write_index(name, &in_dir_v1, IndexVersion::kV1, datas);
    }

    // index v2
    RAMDirectory in_dir_v2;
    {
        std::vector<std::string> datas;
        for (int32_t i = 0; i < 10; i++) {
            std::string ip_v4 = generateRandomIP();
            datas.emplace_back(ip_v4);
        }
        write_index(name, &in_dir_v2, IndexVersion::kV2, datas);
    }

    // index compaction exception 1
    RAMDirectory out_dir;
    {
        std::vector<lucene::store::Directory*> srcDirs;
        srcDirs.push_back(&in_dir_v1);
        srcDirs.push_back(&in_dir_v2);
        std::vector<lucene::store::Directory*> destDirs;
        destDirs.push_back(&out_dir);

        bool flag = false;
        try {
            RAMDirectory empty_dir;
            index_compaction(&empty_dir, srcDirs, destDirs, 10);
        } catch (...) {
            flag = true;
        }
        assertTrue(flag);
    }

    std::cout << "TestIndexCompactionException sucess" << std::endl;
}

CuSuite* testIndexCompress() {
    CuSuite* suite = CuSuiteNew(_T("CLucene Index Compress Test"));

    SUITE_ADD_TEST(suite, TestIndexCompressV2);
    SUITE_ADD_TEST(suite, TestIndexCompactionV2);
    SUITE_ADD_TEST(suite, TestIndexCompactionException);

    return suite;
}
