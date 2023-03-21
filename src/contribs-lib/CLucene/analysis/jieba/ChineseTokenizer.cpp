#include "CLucene/_ApiHeader.h"
#include "ChineseTokenizer.h"
#include "CLucene/util/CLStreams.h"
#include <memory>

CL_NS_DEF2(analysis, jieba)
CL_NS_USE(analysis)
CL_NS_USE(util)

ChineseTokenizer::ChineseTokenizer(lucene::util::Reader *reader) : Tokenizer(reader) {
    buffer[0] = 0;
}

void ChineseTokenizer::init(const std::string &dictPath) {
    JiebaSingleton::getInstance(dictPath);
}

CL_NS(analysis)::Token *ChineseTokenizer::next(lucene::analysis::Token *token) {
    // try to read all words
    if (dataLen == 0 || bufferIndex >= dataLen) {
        auto bufferLen = input->read((const void **) &ioBuffer, 1, 0);
        if (bufferLen == -1) {
            dataLen = 0;
            bufferIndex = 0;
            return NULL;
        }
        char tmp_buffer[4 * bufferLen];
        lucene_wcsntoutf8(tmp_buffer, ioBuffer, bufferLen, 4 * bufferLen);
        JiebaSingleton::getInstance().Cut(tmp_buffer, tokens_text, true);
        dataLen = tokens_text.size();
    }
    if (bufferIndex < dataLen) {
        auto token_text = tokens_text[bufferIndex];
        bufferIndex++;
        lucene_utf8towcs(buffer, token_text.c_str(), LUCENE_MAX_WORD_LEN);
        auto length = _tcslen(buffer);
        token->set(buffer, 0, length);
        return token;
    }
    return NULL;
}
CL_NS_END2