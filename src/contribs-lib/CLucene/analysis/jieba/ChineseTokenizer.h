#ifndef _lucene_analysis_jiebatokenizer_
#define _lucene_analysis_jiebatokenizer_

#include <CLucene.h>

#include <memory>
#include "Jieba.hpp"

#include "CLucene/analysis/AnalysisHeader.h"
#include "CLucene/analysis/LanguageBasedAnalyzer.h"


CL_NS_DEF2(analysis,jieba)
CL_NS_USE(analysis)

class JiebaSingleton {
public:
    static cppjieba::Jieba& getInstance(const std::string& dictPath = "") {
        static cppjieba::Jieba instance(dictPath + "/" + "jieba.dict.utf8",
                                        dictPath + "/" + "hmm_model.utf8",
                                        dictPath + "/" + "user.dict.utf8",
                                        dictPath + "/" + "idf.utf8",
                                        dictPath + "/" + "stop_words.utf8");
        return instance;
    }

private:
    JiebaSingleton() = default;
};

class ChineseTokenizer : public lucene::analysis::Tokenizer {
private:
    AnalyzerMode mode{};
    /** word offset, used to imply which character(in ) is parsed */
    int32_t offset{};

    /** the index used only for ioBuffer */
    int32_t bufferIndex{};

    /** data length */
    int32_t dataLen{};

    /**
     * character buffer, store the characters which are used to compose <br>
     * the returned Token
     */
    TCHAR buffer[LUCENE_MAX_WORD_LEN + 1]{};

    /**
     * I/O buffer, used to store the content of the input(one of the <br>
     * members of Tokenizer)
     */
    const char* ioBuffer{};
    std::vector<std::string> tokens_text;
    //std::vector<std::unique_ptr<Token>> tokens;

public:
    // Constructor
    explicit ChineseTokenizer(lucene::util::Reader *reader, AnalyzerMode mode);
    static void init(const std::string& dictPath="");

    // Destructor
    ~ChineseTokenizer() override = default;

    // Override the next method to tokenize Chinese text using Jieba
    lucene::analysis::Token* next(lucene::analysis::Token* token) override;

    void reset(lucene::util::Reader *reader) override {
        this->input = reader;
        this->offset = 0;
        this->bufferIndex = 0;
        this->dataLen = 0;
        this->tokens_text.clear();
    }
};

CL_NS_END2
#endif
