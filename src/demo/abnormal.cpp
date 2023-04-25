#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <stdlib.h>
#include <time.h>

#include <algorithm>
#include <cstdint>
#include <fstream>
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

enum class AnalyzerType { kSTANDARD, kENGLISH, kCHINESE, kNONE };
enum class TokenizerType { kTOKENIZED, kUNTOKENIZED };

int32_t main(int32_t argc, char** argv) {
    std::string str;
    {
        std::ifstream inFile;
        // json
        // inFile.open("/mnt/disk2/yangsiyu/data.json", std::ios::in);
        // 二进制
        // inFile.open("/mnt/disk2/yangsiyu/manticoresearch/build/src/searchd", std::ios::in);
        // 中英文
        inFile.open("/mnt/disk2/yangsiyu/toutiao-text-classfication-dataset/toutiao_cat_data.txt",
                    std::ios::in);
        std::string line;
        int32_t i = 0;
        while (getline(inFile, line)) {
            str.append(line);
        }
        inFile.close();
    }

    std::cout << "file size: " << str.size() << std::endl;

    std::unordered_map<int32_t, AnalyzerType> analyzer_types;
    analyzer_types[0] = AnalyzerType::kSTANDARD;
    analyzer_types[1] = AnalyzerType::kENGLISH;
    analyzer_types[2] = AnalyzerType::kNONE;
    analyzer_types[3] = AnalyzerType::kNONE;
    analyzer_types[4] = AnalyzerType::kNONE;

    std::unordered_map<int32_t, TokenizerType> tokenizer_types;
    tokenizer_types[0] = TokenizerType::kTOKENIZED;
    tokenizer_types[1] = TokenizerType::kTOKENIZED;
    tokenizer_types[2] = TokenizerType::kTOKENIZED;
    tokenizer_types[3] = TokenizerType::kTOKENIZED;
    tokenizer_types[4] = TokenizerType::kUNTOKENIZED;

    {
        for (int32_t i = 0; i <= 4; i++) {
            std::string name = "json";
            std::string path = "/mnt/disk2/yangsiyu/index2";

            lucene::analysis::Analyzer* analyzer = nullptr;
            if (analyzer_types[i] == AnalyzerType::kSTANDARD) {
                analyzer = _CLNEW lucene::analysis::standard::StandardAnalyzer();
            } else if (analyzer_types[i] == AnalyzerType::kENGLISH) {
                analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>();
            } else if (analyzer_types[i] == AnalyzerType::kCHINESE) {
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
            auto field_config =
                    lucene::document::Field::STORE_NO | lucene::document::Field::INDEX_NONORMS;
            if (tokenizer_types[i] == TokenizerType::kTOKENIZED) {
                field_config |= lucene::document::Field::INDEX_TOKENIZED;
            } else {
                field_config |= lucene::document::Field::INDEX_UNTOKENIZED;
            }
            auto field_name = std::wstring(name.begin(), name.end());
            auto field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
            doc->add(*field);

            for (int32_t j = 0; j < 10; j++) {
                if (analyzer_types[i] == AnalyzerType::kSTANDARD) {
                    auto value = lucene::util::Misc::_charToWide(str.c_str(), str.size());
                    field->setValue(value, false);
                } else if (analyzer_types[i] == AnalyzerType::kENGLISH) {
                    char_string_reader->init(str.c_str(), str.size(), false);
                    auto stream = analyzer->reusableTokenStream(field->name(), char_string_reader);
                    field->setValue(stream);
                } else if (analyzer_types[i] == AnalyzerType::kCHINESE) {
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
            _CLDELETE(char_string_reader);
        }
    }

    return 0;
}