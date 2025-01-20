#include "ICUTokenizer.h"

#include <unicode/unistr.h>

#include <memory>
#include <string>

namespace lucene::analysis {

ICUTokenizer::ICUTokenizer() {
    Tokenizer::lowercase = false;
    Tokenizer::ownReader = false;

    config_ = std::make_shared<DefaultICUTokenizerConfig>(true, true);
    breaker_ = std::make_unique<CompositeBreakIterator>(config_);
}

ICUTokenizer::ICUTokenizer(bool lowercase, bool ownReader) : ICUTokenizer() {
    Tokenizer::lowercase = lowercase;
    Tokenizer::ownReader = ownReader;
}

void ICUTokenizer::initialize(const std::string& dictPath) {
    config_->initialize(dictPath);
    breaker_->initialize();
}

Token* ICUTokenizer::next(Token* token) {
    int32_t start = breaker_->current();
    assert(start != UBRK_DONE);

    int32_t end = breaker_->next();
    while (end != UBRK_DONE && breaker_->getRuleStatus() == 0) {
        start = end;
        end = breaker_->next();
    }

    if (end == UBRK_DONE) {
        return nullptr;
    }

    utf8Str_.clear();
    auto subString = buffer_.tempSubString(start, end - start);
    if (Tokenizer::lowercase) {
        subString.toLower().toUTF8String(utf8Str_);
    } else {
        subString.toUTF8String(utf8Str_);
    }

    token->setNoCopy(utf8Str_.data(), 0, utf8Str_.size());
    return token;
}

void ICUTokenizer::reset(lucene::util::Reader* input) {
    const char* buf = nullptr;
    int32_t len = input->read((const void**)&buf, 0, input->size());
    buffer_ = icu::UnicodeString::fromUTF8(icu::StringPiece(buf, len));
    if (!buffer_.isEmpty() && buffer_.isBogus()) {
        _CLTHROWT(CL_ERR_Runtime, "Failed to convert UTF-8 string to UnicodeString.");
    }
    breaker_->setText(buffer_.getBuffer(), 0, buffer_.length());
}

} // namespace lucene::analysis