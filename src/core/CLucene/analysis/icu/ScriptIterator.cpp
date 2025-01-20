#include "ScriptIterator.h"

#include <unicode/unistr.h>

#include <mutex>
#include <string>

namespace lucene::analysis {

std::vector<int32_t> ScriptIterator::kBasicLatin(128);

ScriptIterator::ScriptIterator(bool combineCJ) : combineCJ_(combineCJ) {}

void ScriptIterator::initialize() {
    static std::once_flag once_flag;
    std::call_once(once_flag, []() {
        UErrorCode status = U_ZERO_ERROR;
        for (int32_t i = 0; i < 128; i++) {
            kBasicLatin[i] = uscript_getScript(i, &status);
            if (U_FAILURE(status)) {
                std::string error_msg = "Get script failed: ";
                error_msg += u_errorName(status);
                _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
            }
        }
        return kBasicLatin;
    });
}

bool ScriptIterator::next() {
    if (scriptLimit_ >= limit_) {
        return false;
    }

    scriptCode_ = USCRIPT_COMMON;
    scriptStart_ = scriptLimit_;

    while (index_ < limit_) {
        UChar32 ch = 0;
        U16_GET(text_, start_, index_, limit_, ch);
        int32_t script = getScript(ch);
        if (isSameScript(scriptCode_, script, ch) || isCombiningMark(ch)) {
            index_ += U16_LENGTH(ch);
            if (scriptCode_ <= USCRIPT_INHERITED && script > USCRIPT_INHERITED) {
                scriptCode_ = script;
            }
        } else {
            break;
        }
    }

    scriptLimit_ = index_;
    return true;
}

void ScriptIterator::setText(const UChar* text, int32_t start, int32_t length) {
    text_ = text;
    start_ = start;
    index_ = start;
    limit_ = start + length;
    scriptStart_ = start;
    scriptLimit_ = start;
    scriptCode_ = USCRIPT_INVALID_CODE;
}

int32_t ScriptIterator::getScript(UChar32 codepoint) const {
    if (0 <= codepoint && codepoint < 128) {
        return kBasicLatin[codepoint];
    } else {
        UErrorCode err = U_ZERO_ERROR;
        int32_t script = uscript_getScript(codepoint, &err);
        if (U_FAILURE(err)) {
            std::string error_msg = "Get Script error: ";
            error_msg += u_errorName(err);
            error_msg += ", script: " + std::to_string(script);
            _CLTHROWT(CL_ERR_Runtime, error_msg.c_str());
        }
        if (combineCJ_) {
            if (script == USCRIPT_HAN || script == USCRIPT_HIRAGANA || script == USCRIPT_KATAKANA) {
                return USCRIPT_JAPANESE;
            } else if (codepoint >= 0xFF10 && codepoint <= 0xFF19) {
                return USCRIPT_LATIN;
            } else {
                return script;
            }
        } else {
            return script;
        }
    }
}

bool ScriptIterator::isSameScript(int32_t currentScript, int32_t script, UChar32 codepoint) {
    return (currentScript == script) || (currentScript <= USCRIPT_INHERITED) ||
           (script <= USCRIPT_INHERITED) ||
           uscript_hasScript(codepoint, (UScriptCode)currentScript);
}

bool ScriptIterator::isCombiningMark(UChar32 codepoint) {
    auto type = (UCharCategory)u_charType(codepoint);
    return (type == U_COMBINING_SPACING_MARK || type == U_NON_SPACING_MARK ||
            type == U_ENCLOSING_MARK);
}

} // namespace lucene::analysis