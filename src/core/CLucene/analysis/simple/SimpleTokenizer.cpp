#include "SimpleTokenizer.h"

#include <unicode/unistr.h>

namespace lucene::analysis_v2 {

SimpleTokenizer::SimpleTokenizer() {
    Tokenizer::lowercase = false;
    Tokenizer::ownReader = false;
}

SimpleTokenizer::SimpleTokenizer(bool lowercase, bool ownReader) : SimpleTokenizer() {
    Tokenizer::lowercase = lowercase;
    Tokenizer::ownReader = ownReader;
}

Token* SimpleTokenizer::next(Token* token) {
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

void SimpleTokenizer::reset(lucene::util::Reader* input) {
    bufferIndex = 0;
    dataLen = 0;
    tokens_text.clear();

    buffer_.resize(input->size());
    int32_t numRead = input->readCopy(buffer_.data(), 0, buffer_.size());
    assert(buffer_.size() == numRead);

    cut();

    dataLen = tokens_text.size();
}

void SimpleTokenizer::cut() {
    uint8_t* s = (uint8_t*)buffer_.data();
    int32_t length = (int32_t)buffer_.size();

    for (int32_t i = 0; i < length;) {
        uint8_t firstByte = s[i];

        if (is_alnum(firstByte)) {
            int32_t start = i;
            while (i < length) {
                uint8_t nextByte = s[i];
                if (!is_alnum(nextByte)) {
                    break;
                }
                s[i] = to_lower(nextByte);
                i++;
            }
            std::string_view token((const char*)(s + start), i - start);
            tokens_text.emplace_back(std::move(token));
        } else {
            UChar32 c = U_UNASSIGNED;
            const int32_t prev_i = i;

            U8_NEXT(s, i, length, c);

            if (c == U_UNASSIGNED) {
                _CLTHROWT(CL_ERR_Runtime, "invalid UTF-8 sequence");
            }

            const UCharCategory category = static_cast<UCharCategory>(u_charType(c));
            if (category == U_OTHER_LETTER) {
                const int32_t len = i - prev_i;
                tokens_text.emplace_back(reinterpret_cast<const char*>(s + prev_i), len);
            }
        }
    }
}

} // namespace lucene::analysis_v2