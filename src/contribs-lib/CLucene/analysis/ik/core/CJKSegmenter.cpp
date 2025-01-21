#include "CJKSegmenter.h"
CL_NS_USE2(analysis, ik)

CJKSegmenter::CJKSegmenter() = default;

void CJKSegmenter::analyze(AnalyzeContext& context) {
    if (context.getCurrentCharType() != CharacterUtil::CHAR_USELESS) {
        // Prioritize processing the prefixes that have already been matched
        if (!tmp_hits_.empty()) {
            auto it = tmp_hits_.begin();
            while (it != tmp_hits_.end()) {
                Hit& hit = *it;
                Dictionary::getSingleton()->matchWithHit(context.getTypedRuneArray(),
                                                         context.getCursor(), hit);
                if (hit.isMatch()) {
                    Lexeme newLexeme(context.getBufferOffset(), hit.getByteBegin(),
                                     hit.getByteEnd() - hit.getByteBegin(), Lexeme::Type::CNWord,
                                     hit.getCharBegin(), hit.getCharEnd());
                    context.addLexeme(newLexeme);

                    if (!hit.isPrefix()) {
                        it = tmp_hits_.erase(it);
                    } else {
                        ++it;
                    }
                } else if (hit.isUnmatch()) {
                    // Hit is not a word, remove
                    it = tmp_hits_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        // Perform a single-character match at the current pointer position.
        auto singleCharHit = Dictionary::getSingleton()->matchInMainDict(
                context.getTypedRuneArray(), context.getCursor(), 1);
        if (singleCharHit.isMatch()) {
            Lexeme newLexeme(context.getBufferOffset(), context.getCurrentCharOffset(),
                             context.getCurrentCharLen(), Lexeme::Type::CNChar,
                             singleCharHit.getCharBegin(), singleCharHit.getCharEnd());
            context.addLexeme(newLexeme);
        }
        if (singleCharHit.isPrefix()) {
            tmp_hits_.push_back(singleCharHit);
        }
    } else {
        tmp_hits_.clear();
    }

    if (context.isBufferConsumed()) {
        tmp_hits_.clear();
    }

    if (tmp_hits_.empty()) {
        context.unlockBuffer(CJKSegmenter::SEGMENTER_TYPE);
    } else {
        context.lockBuffer(CJKSegmenter::SEGMENTER_TYPE);
    }
}

void CJKSegmenter::reset() {
    tmp_hits_.clear();
}