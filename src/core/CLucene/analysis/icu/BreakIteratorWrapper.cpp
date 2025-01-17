#include "BreakIteratorWrapper.h"

#include <unicode/unistr.h>

#include <mutex>
#include <string>

#include "CLucene/analysis/icu/ICUCommon.h"
#include "ICUTokenizerConfig.h"

namespace lucene::analysis {

icu::UnicodeSet BreakIteratorWrapper::EMOJI_RK;
icu::UnicodeSet BreakIteratorWrapper::EMOJI;

BreakIteratorWrapper::BreakIteratorWrapper(icu::BreakIterator* rbbi) : rbbi_(rbbi) {}

void BreakIteratorWrapper::initialize() {
    static std::once_flag once_flag;
    std::call_once(once_flag, []() {
        UErrorCode status = U_ZERO_ERROR;
        EMOJI_RK.applyPattern("[*#0-9©®™〰〽]", status);
        if (U_FAILURE(status)) {
            std::string error_msg = "EMOJI RK failed to initialize: ";
            error_msg += u_errorName(status);
            _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
        }
        EMOJI.applyPattern("[[:Emoji:][:Extended_Pictographic:]]", status);
        if (U_FAILURE(status)) {
            std::string error_msg = "EMOJI failed to initialize: ";
            error_msg += u_errorName(status);
            _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
        }
    });
}

int32_t BreakIteratorWrapper::next() {
    int32_t current = rbbi_->current();
    int32_t next = rbbi_->next();
    status_ = calcStatus(current, next);
    return next;
}

int32_t BreakIteratorWrapper::calcStatus(int32_t current, int32_t next) {
    if (next != UBRK_DONE && isEmoji(current, next)) {
        return ICUTokenizerConfig::EMOJI_SEQUENCE_STATUS;
    } else {
        return rbbi_->getRuleStatus();
    }
}

bool BreakIteratorWrapper::isEmoji(int32_t current, int32_t next) {
    int32_t begin = start_ + current;
    int32_t end = start_ + next;
    UChar32 codepoint = 0;
    U16_GET(text_, 0, begin, end, codepoint);
    if (EMOJI.contains(codepoint)) {
        if (EMOJI_RK.contains(codepoint)) {
            int32_t trailer = begin + U16_LENGTH(codepoint);
            return trailer < end && (text_[trailer] == 0xFE0F || text_[trailer] == 0x20E3);
        } else {
            return true;
        }
    }
    return false;
}

void BreakIteratorWrapper::setText(const UChar* text, int32_t start, int32_t length) {
    text_ = text;
    start_ = start;

    UErrorCode status = U_ZERO_ERROR;
    UTextPtr utext(utext_openUChars(nullptr, text + start, length, &status));
    if (U_FAILURE(status)) {
        std::string error_msg = "Failed to create UText: ";
        error_msg += u_errorName(status);
        _CLTHROWT(CL_ERR_Runtime, error_msg.c_str());
    }

    rbbi_->setText(utext.get(), status);
    if (U_FAILURE(status)) {
        std::string error_msg = "Failed to set text: ";
        error_msg += u_errorName(status);
        _CLTHROWT(CL_ERR_Runtime, error_msg.c_str());
    }

    status_ = UBRK_WORD_NONE;
}

} // namespace lucene::analysis