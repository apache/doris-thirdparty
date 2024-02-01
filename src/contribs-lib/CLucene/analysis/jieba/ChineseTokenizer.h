#ifndef _lucene_analysis_jiebatokenizer_
#define _lucene_analysis_jiebatokenizer_

#include <CLucene.h>

#include <memory>
#include <string_view>
#include "Jieba.hpp"

#include "CLucene/analysis/AnalysisHeader.h"
#include "CLucene/analysis/LanguageBasedAnalyzer.h"


CL_NS_DEF2(analysis,jieba)
CL_NS_USE(analysis)

struct ChineseDict {
    std::string dictPath_;
    std::vector<std::string> files_ = {
        "jieba.dict.utf8",
        "hmm_model.utf8",
        "user.dict.utf8",
        "idf.utf8",
        "stop_words.utf8"
    };
};

class JiebaSingleton {
public:
    static cppjieba::Jieba& getInstance(const ChineseDict* dict = nullptr) {
        static cppjieba::Jieba instance(dict->dictPath_ + "/" + dict->files_[0],
                                        dict->dictPath_ + "/" + dict->files_[1],
                                        dict->dictPath_ + "/" + dict->files_[2],
                                        dict->dictPath_ + "/" + dict->files_[3],
                                        dict->dictPath_ + "/" + dict->files_[4]);
        return instance;
    }

private:
    JiebaSingleton() = default;
};

class ChineseTokenizer : public lucene::analysis::Tokenizer {
private:
    AnalyzerMode mode{};

    /** the index used only for ioBuffer */
    int32_t bufferIndex = 0;

    /** data length */
    int32_t dataLen = 0;

    std::string buffer_;
    std::vector<std::string_view> tokens_text;

public:
    // Constructor
    explicit ChineseTokenizer(lucene::util::Reader *reader, AnalyzerMode mode);
    explicit ChineseTokenizer(lucene::util::Reader *reader, AnalyzerMode mode, bool lowercase);
    static void init(const ChineseDict* chineseDict);

    // Destructor
    ~ChineseTokenizer() override = default;

    // Override the next method to tokenize Chinese text using Jieba
    lucene::analysis::Token* next(lucene::analysis::Token* token) override;

    void reset(lucene::util::Reader *reader) override;
};

CL_NS_END2
#endif
