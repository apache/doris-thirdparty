#include <CLucene.h>

#include <iostream>
#include <unordered_map>
#include <vector>

#include "CLucene/search/TermQuery.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/util/NumericUtils.h"
#include "CLucene/util/bkd/bkd_reader.h"
#include "CLucene/util/bkd/bkd_writer.h"
#include "simdjson.h"

CL_NS_USE(util)
CL_NS_USE(store)

using namespace simdjson;

using namespace lucene::search;
using namespace lucene::index;

void indexString(const std::string& path, const std::string& name,
                 const std::vector<std::string>& datas) {
    std::cout << "indexString size: " << datas.size() << std::endl;
    auto analyzer = _CLNEW lucene::analysis::standard::StandardAnalyzer();
    auto indexwriter = _CLNEW lucene::index::IndexWriter(path.c_str(), analyzer, true);
    indexwriter->setMaxBufferedDocs(100000000);
    indexwriter->setRAMBufferSizeMB(512);
    indexwriter->setMaxFieldLength(0x7FFFFFFFL);
    indexwriter->setMergeFactor(100000000);
    indexwriter->setUseCompoundFile(false);

    auto doc = _CLNEW lucene::document::Document();
    auto field_config = lucene::document::Field::STORE_NO | lucene::document::Field::INDEX_NONORMS;
    field_config |= lucene::document::Field::INDEX_UNTOKENIZED;
    auto field_name = std::wstring(name.begin(), name.end());
    auto field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
    doc->add(*field);

    for (int32_t i = 0; i < 100; i++) {
        for (auto& str : datas) {
            // std::cout << "name: " << name << ", value: " << str << std::endl;
            auto value = lucene::util::Misc::_charToWide(str.c_str(), str.size());
            field->setValue(value, false);
            indexwriter->addDocument(doc);
        }
    }
    indexwriter->close();

    _CLLDELETE(indexwriter);
    _CLLDELETE(doc);
    _CLLDELETE(analyzer);
}

int32_t main(int32_t argc, char** argv) {
    std::unordered_map<std::string, std::vector<std::string>> datas;
    {
        ondemand::parser parser;
        padded_string json = padded_string::load("/mnt/disk2/yangsiyu/data.json");
        ondemand::document tweets = parser.iterate(json);
        int32_t i = 0;
        for (auto tweet : tweets) {
            for (auto d : tweet.get_object()) {
                std::string key = (std::string)d.unescaped_key().value();
                if (key != "story_id") {
                    continue;
                }
                std::string value = (std::string)d.value().raw_json_token().value();
                datas[key].emplace_back(std::move(value));
            }
            // if (i++ == 5) {
            //     break;
            // }
        }
    }

    {
        std::string path = "/mnt/disk2/yangsiyu/index1/index1";
        indexString(path, "story_id", datas["story_id"]);

        IndexReader* reader = IndexReader::open(path.c_str());
        IndexSearcher index_searcher(reader);

        {
            Term* t = _CLNEW Term(_T("story_id"), _T("6302825"));
            Query* query = _CLNEW TermQuery(t);
            _CLLDECDELETE(t);

            std::vector<int32_t> result;
            index_searcher._search(query, [&result](const int32_t docid, const float_t /*score*/) {
                result.push_back(docid);
            });
            std::cout << "term count: " << result.size() << std::endl;

            _CLLDELETE(query);
        }

        {
            Term* upper = _CLNEW Term(_T("story_id"), _T("6000000"));
            Term* lower = _CLNEW Term(_T("story_id"), _T("6302825"));
            RangeQuery* query = _CLNEW RangeQuery(upper, lower, true);
            _CLDECDELETE(upper);
            _CLDECDELETE(lower);

            std::vector<int32_t> result;
            index_searcher._search(query, [&result](const int32_t docid, const float_t /*score*/) {
                result.push_back(docid);
            });
            std::cout << "range count: " << result.size() << std::endl;

            _CLLDELETE(query);
        }
    }
}