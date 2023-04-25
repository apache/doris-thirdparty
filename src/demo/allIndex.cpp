#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <stdlib.h>
#include <time.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/util/NumericUtils.h"
#include "CLucene/util/bkd/bkd_writer.h"
#include "simdjson.h"

CL_NS_USE(util)
CL_NS_USE(store)

using namespace simdjson;

enum class DataType { KINT, kSTRING };
enum class TokenizerType { kTOKENIZED, kUNTOKENIZED };
enum class AnalyzerType { kSTANDARD, kENGLISH, kCHINESE, kNone };

std::unordered_map<std::string, DataType> data_types;
std::unordered_map<std::string, TokenizerType> tokenizer_types;
std::unordered_map<std::string, AnalyzerType> analyzer_types;

void indexString(const std::string& path, const std::string& name,
                 const std::vector<std::string>& datas) {
    lucene::analysis::Analyzer* analyzer = nullptr;
    if (analyzer_types[name] == AnalyzerType::kSTANDARD) {
        analyzer = _CLNEW lucene::analysis::standard::StandardAnalyzer();
    } else if (analyzer_types[name] == AnalyzerType::kENGLISH) {
        analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>();
    } else if (analyzer_types[name] == AnalyzerType::kCHINESE) {
        auto chinese_analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
        chinese_analyzer->setLanguage(L"chinese");
        chinese_analyzer->initDict(
                "/mnt/disk2/yangsiyu/selectdb/doris-thirdparty/src/contribs-lib/CLucene/"
                "analysis/jieba/dict");
        analyzer = chinese_analyzer;
    } else {
        analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<TCHAR>();
    }

    auto char_string_reader = _CLNEW lucene::util::SStringReader<char>;

    auto indexwriter = _CLNEW lucene::index::IndexWriter(path.c_str(), analyzer, true);
    indexwriter->setMaxBufferedDocs(100000000);
    indexwriter->setRAMBufferSizeMB(512);
    indexwriter->setMaxFieldLength(0x7FFFFFFFL);
    indexwriter->setMergeFactor(100000000);
    indexwriter->setUseCompoundFile(false);

    auto doc = _CLNEW lucene::document::Document();
    auto field_config = lucene::document::Field::STORE_NO | lucene::document::Field::INDEX_NONORMS;
    if (tokenizer_types[name] == TokenizerType::kUNTOKENIZED) {
        field_config |= lucene::document::Field::INDEX_UNTOKENIZED;
    } else {
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
    }
    auto field_name = std::wstring(name.begin(), name.end());
    auto field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
    doc->add(*field);

    for (auto& str : datas) {
        // std::cout << "name: " << name << ", value: " << str << std::endl;

        if (analyzer_types[name] == AnalyzerType::kENGLISH) {
            char_string_reader->init(str.c_str(), str.size(), false);
            auto stream = analyzer->reusableTokenStream(field->name(), char_string_reader);
            field->setValue(stream);
        } else if (analyzer_types[name] == AnalyzerType::kCHINESE) {
            auto stringReader = _CLNEW lucene::util::SimpleInputStreamReader(
                    new lucene::util::AStringReader(str.c_str(), str.size()),
                    lucene::util::SimpleInputStreamReader::UTF8);
            field->setValue(stringReader);
        } else {
            auto value = lucene::util::Misc::_charToWide(str.c_str(), str.size());
            field->setValue(value, false);
        }
        indexwriter->addDocument(doc);
    }
    indexwriter->close();

    _CLLDELETE(indexwriter);
    _CLLDELETE(doc);
    _CLLDELETE(analyzer);
    _CLDELETE(char_string_reader)
}

template <class T>
void indexNumeric(const std::string& path, const std::string& name,
                  const std::vector<std::string>& datas) {
    const int32_t N = datas.size();
    Directory* dir(FSDirectory::getDirectory(path.c_str()));
    std::shared_ptr<bkd::bkd_writer> w = nullptr;
    if constexpr (std::is_same_v<T, int32_t>) {
        w = std::make_shared<bkd::bkd_writer>(N, 1, 1, 4, 512, 1024 * 10, N, true);
    } else {
        w = std::make_shared<bkd::bkd_writer>(N, 1, 1, 8, 512, 1024 * 10, N, true);
    }
    w->docs_seen_ = N;

    for (int32_t docID = 0; docID < N; docID++) {
        if constexpr (std::is_same_v<T, int32_t>) {
            std::vector<uint8_t> scratch(4);
            NumericUtils::intToSortableBytes(std::stoi(datas[docID]), scratch, 0);
            w->add(scratch.data(), scratch.size(), docID);
        } else {
            std::vector<uint8_t> scratch(8);
            NumericUtils::intToSortableBytes(std::stol(datas[docID]), scratch, 0);
            w->add(scratch.data(), scratch.size(), docID);
        }
    }

    int64_t indexFP = 0;
    {
        std::unique_ptr<IndexOutput> out(dir->createOutput("bkd"));
        std::unique_ptr<IndexOutput> meta_out(dir->createOutput("bkd_meta"));
        std::unique_ptr<IndexOutput> index_out(dir->createOutput("bkd_index"));

        indexFP = w->finish(out.get(), index_out.get());
        w->meta_finish(meta_out.get(), indexFP, 0);
    }

    dir->close();
    _CLDECDELETE(dir);
}

int32_t main(int32_t argc, char** argv) {
    {
        data_types["story_id"] = DataType::KINT;
        data_types["story_time"] = DataType::kSTRING;
        data_types["story_url"] = DataType::kSTRING;
        data_types["story_text"] = DataType::kSTRING;
        data_types["story_author"] = DataType::kSTRING;
        data_types["comment_id"] = DataType::KINT;
        data_types["comment_text"] = DataType::kSTRING;
        data_types["comment_author"] = DataType::kSTRING;
        data_types["comment_ranking"] = DataType::kSTRING;
        data_types["author_comment_count"] = DataType::kSTRING;
        data_types["story_comment_count"] = DataType::kSTRING;
    }
    {
        tokenizer_types["story_time"] = TokenizerType::kUNTOKENIZED;
        tokenizer_types["story_url"] = TokenizerType::kTOKENIZED;
        tokenizer_types["story_text"] = TokenizerType::kTOKENIZED;
        tokenizer_types["story_author"] = TokenizerType::kUNTOKENIZED;
        tokenizer_types["comment_text"] = TokenizerType::kTOKENIZED;
        tokenizer_types["comment_author"] = TokenizerType::kUNTOKENIZED;
        tokenizer_types["comment_ranking"] = TokenizerType::kUNTOKENIZED;
        tokenizer_types["author_comment_count"] = TokenizerType::kUNTOKENIZED;
        tokenizer_types["story_comment_count"] = TokenizerType::kUNTOKENIZED;
    }
    {
        analyzer_types["story_time"] = AnalyzerType::kNone;
        analyzer_types["story_url"] = AnalyzerType::kSTANDARD;
        analyzer_types["story_text"] = AnalyzerType::kENGLISH;
        analyzer_types["story_author"] = AnalyzerType::kNone;
        analyzer_types["comment_text"] = AnalyzerType::kCHINESE;
        analyzer_types["comment_author"] = AnalyzerType::kNone;
        analyzer_types["comment_ranking"] = AnalyzerType::kNone;
        analyzer_types["author_comment_count"] = AnalyzerType::kNone;
        analyzer_types["story_comment_count"] = AnalyzerType::kNone;
    }

    std::unordered_map<std::string, std::vector<std::string>> datas;
    {
        ondemand::parser parser;
        padded_string json = padded_string::load("/mnt/disk2/yangsiyu/data.json");
        ondemand::document tweets = parser.iterate(json);
        int32_t i = 0;
        for (auto tweet : tweets) {
            for (auto d : tweet.get_object()) {
                std::string key = (std::string)d.unescaped_key().value();
                std::string value = (std::string)d.value().raw_json_token().value();
                datas[key].emplace_back(std::move(value));
            }
            // if (i++ == 5) {
            //     break;
            // }
        }
    }

    for (auto& m : datas) {
        std::cout << m.first << ": " << m.second.size() << std::endl;
    }
    std::cout << "-----------------------" << std::endl;

    // index
    {
        int32_t idx = 1;
        for (auto& data : datas) {
            std::string name = data.first;
            // if (name != "story_id") {
            //     continue;
            // }
            std::string path = "/mnt/disk2/yangsiyu/index/index" + std::to_string(idx++);
            for (int32_t i = 0; i < 10000; i++) {
                if (data_types[name] == DataType::kSTRING) {
                    indexString(path, name, data.second);
                } else {
                    if (name == "story_time") {
                        indexNumeric<int32_t>(path, name, data.second);
                    } else {
                        indexNumeric<int64_t>(path, name, data.second);
                    }
                }
            }
        }
    }

    return 0;
}