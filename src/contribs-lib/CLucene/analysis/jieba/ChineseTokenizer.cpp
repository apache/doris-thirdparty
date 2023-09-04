#include "CLucene/_ApiHeader.h"
#include "ChineseTokenizer.h"
#include "CLucene/util/CLStreams.h"
#include <memory>

CL_NS_DEF2(analysis, jieba)
CL_NS_USE(analysis)
CL_NS_USE(util)

ChineseTokenizer::ChineseTokenizer(lucene::util::Reader *reader, AnalyzerMode m) : Tokenizer(reader), mode(m) {
    memset(buffer, 0, LUCENE_MAX_WORD_LEN + 1);
}

void ChineseTokenizer::init(const std::string &dictPath) {
    JiebaSingleton::getInstance(dictPath);
}

CL_NS(analysis)::Token *ChineseTokenizer::next(lucene::analysis::Token *token) {
    // try to read all words
    const char *initBuffer;
    if (dataLen == 0 || bufferIndex >= dataLen) {
        int totalLen = 0;
        do {
            auto bufferLen = input->read((const void**)&ioBuffer, 1, LUCENE_IO_BUFFER_SIZE);
            if (bufferLen <= 0) {
                dataLen = 0;
                bufferIndex = 0;
                break;
            }
            if (totalLen < LUCENE_IO_BUFFER_SIZE) {
                initBuffer = ioBuffer;
            }
            totalLen+=bufferLen;
        } while (true);

        //char tmp_buffer[4 * totalLen + 1];
        //lucene_wcsntoutf8(tmp_buffer, initBuffer, totalLen, 4 * totalLen);
	std::string s(initBuffer, totalLen);
        switch (mode) {
        case AnalyzerMode::Search:
            JiebaSingleton::getInstance().CutForSearch(s, tokens_text, true);
            break;
        case AnalyzerMode::All:
            JiebaSingleton::getInstance().CutAll(s, tokens_text);
            break;
        case AnalyzerMode::Default:
            JiebaSingleton::getInstance().Cut(s, tokens_text, true);
            break;
        }
        dataLen = tokens_text.size();
    }
    if (bufferIndex < dataLen) {
        std::string& token_text = tokens_text[bufferIndex++];
        size_t size = std::min(token_text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
        token->setNoCopy(token_text.data(), 0, size);
        return token;
    }
    return nullptr;
}
CL_NS_END2
