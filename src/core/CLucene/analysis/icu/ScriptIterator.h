#pragma once

#include <unicode/umachine.h>
#include <unicode/utext.h>

#include <memory>
#include <vector>

#include "ICUCommon.h"

namespace lucene::analysis {

class ScriptIterator {
public:
    ScriptIterator(bool combineCJ);
    ~ScriptIterator() = default;

    void initialize();

    int32_t getScriptStart() { return scriptStart_; }
    int32_t getScriptLimit() { return scriptLimit_; }
    int32_t getScriptCode() { return scriptCode_; }

    bool next();
    void setText(const UChar* text, int32_t start, int32_t length);

private:
    int32_t getScript(UChar32 codepoint) const;
    static bool isSameScript(int32_t currentScript, int32_t script, UChar32 codepoint);
    static bool isCombiningMark(UChar32 codepoint);

    static std::vector<int32_t> kBasicLatin;

    const UChar* text_ = nullptr;
    int32_t start_ = 0;
    int32_t index_ = 0;
    int32_t limit_ = 0;

    int32_t scriptStart_ = 0;
    int32_t scriptLimit_ = 0;
    int32_t scriptCode_ = USCRIPT_INVALID_CODE;

    bool combineCJ_ = false;
};
using ScriptIteratorPtr = std::unique_ptr<ScriptIterator>;

} // namespace lucene::analysis