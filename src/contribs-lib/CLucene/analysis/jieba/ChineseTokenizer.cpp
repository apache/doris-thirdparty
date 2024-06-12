#include "CLucene/_ApiHeader.h"
#include "ChineseTokenizer.h"
#include "CLucene/util/CLStreams.h"
#include <memory>

CL_NS_DEF2(analysis, jieba)
CL_NS_USE(analysis)
CL_NS_USE(util)

ChineseTokenizer::ChineseTokenizer(lucene::util::Reader *reader, AnalyzerMode m) : Tokenizer(reader), mode(m) {
    reset(reader);
    Tokenizer::lowercase = false;
    Tokenizer::ownReader = false;
}

ChineseTokenizer::ChineseTokenizer(lucene::util::Reader *reader, AnalyzerMode m, bool lowercase, bool ownReader) : Tokenizer(reader), mode(m) {
    reset(reader);
    Tokenizer::lowercase = lowercase;
    Tokenizer::ownReader = ownReader;
}

void ChineseTokenizer::init(const ChineseDict* chineseDict) {
    JiebaSingleton::getInstance(chineseDict);
}

CL_NS(analysis)::Token* ChineseTokenizer::next(lucene::analysis::Token* token) {
    if (bufferIndex >= dataLen) {
        return nullptr;
    }

    std::string_view& token_text = tokens_text[bufferIndex++];
    size_t size = std::min(token_text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    if (Tokenizer::lowercase) {
        if (!token_text.empty() && static_cast<uint8_t>(token_text[0]) < 0x80) {
            std::transform(token_text.begin(), token_text.end(),
                           const_cast<char*>(token_text.data()),
                           [](char c) { return to_lower(c); });
        }
    }
    token->setNoCopy(token_text.data(), 0, size);
    return token;
}

void ChineseTokenizer::reset(lucene::util::Reader* reader) {
    this->input = reader;
    this->bufferIndex = 0;
    this->dataLen = 0;
    this->tokens_text.clear();

    buffer_.resize(input->size());
    int32_t numRead = input->readCopy(buffer_.data(), 0, buffer_.size());
    assert(buffer_.size() == numRead);
    
    switch (mode) {
        case AnalyzerMode::Search:
            JiebaSingleton::getInstance().CutForSearch(buffer_, tokens_text, true);
            break;
        case AnalyzerMode::All:
            JiebaSingleton::getInstance().CutAll(buffer_, tokens_text);
            break;
        case AnalyzerMode::Default:
            JiebaSingleton::getInstance().Cut(buffer_, tokens_text, true);
            break;
    }

    dataLen = tokens_text.size();
};

CL_NS_END2
