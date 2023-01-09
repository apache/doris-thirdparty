#include "CLucene/_ApiHeader.h"
#include "ChineseTokenizer.h"
#include "CLucene/util/CLStreams.h"
#include <filesystem>
namespace fs = std::filesystem;

CL_NS_DEF2(analysis,jieba)
CL_NS_USE(analysis)
CL_NS_USE(util)

std::string get_dict_path() {
    if(const char* env_p = std::getenv("DICT_PATH")) {
        return env_p;
    }
    return "";
}

static unique_ptr<cppjieba::Jieba> cppjieba = std::make_unique<cppjieba::Jieba>(
        get_dict_path() + "dict/jieba.dict.utf8",
        get_dict_path() + "dict/hmm_model.utf8",
        get_dict_path() + "dict/user.dict.utf8",
        get_dict_path() + "dict/idf.utf8",
        get_dict_path() + "dict/stop_words.utf8");

CL_NS(analysis)::Token* ChineseTokenizer::next(lucene::analysis::Token* token) {
    // try to read all words
    if (dataLen == 0) {
        auto bufferLen = input->read((const void **) &ioBuffer, 1, 0);
        if (bufferLen == -1) {
            dataLen = 0;
            return NULL;
        }
        char tmp_buffer[4 * bufferLen];
        lucene_wcsntoutf8(tmp_buffer, ioBuffer, bufferLen, 4 * bufferLen);
        cppjieba->Cut(tmp_buffer, tokens_text, true);
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