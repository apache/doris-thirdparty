#pragma once

#include <unicode/umachine.h>
#include <unicode/utext.h>

#include <memory>
#include <unordered_set>

#include "ICUCommon.h"

namespace lucene::analysis {

class BreakIteratorWrapper {
public:
    BreakIteratorWrapper(icu::BreakIterator* rbbi);
    ~BreakIteratorWrapper() = default;

    void initialize();
    int32_t current() { return rbbi_->current(); }
    int32_t getRuleStatus() { return status_; }
    int32_t next();
    int32_t calcStatus(int32_t current, int32_t next);
    bool isEmoji(int32_t current, int32_t next);
    void setText(const UChar* text, int32_t start, int32_t length);

private:
    static icu::UnicodeSet EMOJI_RK;
    static icu::UnicodeSet EMOJI;

    BreakIteratorPtr rbbi_;
    const UChar* text_ = nullptr;
    int32_t start_ = 0;
    int32_t status_ = UBRK_WORD_NONE;
};
using BreakIteratorWrapperPtr = std::unique_ptr<BreakIteratorWrapper>;

} // namespace lucene::analysis