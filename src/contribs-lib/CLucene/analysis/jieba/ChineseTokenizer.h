#ifndef _lucene_analysis_jiebatokenizer_
#define _lucene_analysis_jiebatokenizer_

#include <CLucene.h>

#include <memory>
#include "Jieba.hpp"

#include "CLucene/analysis/AnalysisHeader.h"

CL_NS_DEF2(analysis,jieba)

class ChineseTokenizer : public lucene::analysis::Tokenizer {
private:
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
    TCHAR buffer[LUCENE_MAX_WORD_LEN]{};

    /**
     * I/O buffer, used to store the content of the input(one of the <br>
     * members of Tokenizer)
     */
    const TCHAR* ioBuffer{};
    //std::unique_ptr<cppjieba::Jieba> cppjieba;
    std::vector<std::string> tokens_text;
    std::vector<std::unique_ptr<Token>> tokens;

public:
    // Constructor
    explicit ChineseTokenizer(lucene::util::Reader *reader) : Tokenizer(reader) {

        buffer[0]=0;
    }

    // Destructor
    ~ChineseTokenizer() override {}

    // Override the next method to tokenize Chinese text using Jieba
    lucene::analysis::Token* next(lucene::analysis::Token* token);
};

CL_NS_END2
#endif