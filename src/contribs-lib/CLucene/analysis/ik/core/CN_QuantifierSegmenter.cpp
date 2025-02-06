#include "CN_QuantifierSegmenter.h"
CL_NS_USE2(analysis, ik)

const std::u32string CN_QuantifierSegmenter::CHINESE_NUMBERS =
        U"一二两三四五六七八九十零壹贰叁肆伍陆柒捌玖拾百千万亿拾佰仟萬億兆卅廿";
static constexpr size_t UNICODE_MAX = 0x10000;
static std::vector<bool> chinese_number_chars_table_;

CN_QuantifierSegmenter::CN_QuantifierSegmenter() {
    number_start_ = -1;
    number_end_ = -1;
    count_hits_.clear();

    if (chinese_number_chars_table_.empty()) {
        chinese_number_chars_table_.resize(UNICODE_MAX, false);
        for (auto ch : CHINESE_NUMBERS) {
            chinese_number_chars_table_[ch] = true;
        }
    }
}

void CN_QuantifierSegmenter::analyze(AnalyzeContext& context) {
    // Handle Chinese numerals
    processCNumber(context);
    // Handle Chinese measure words
    processCount(context);

    if (number_start_ == -1 && number_end_ == -1 && count_hits_.empty()) {
        context.unlockBuffer(CN_QuantifierSegmenter::SEGMENTER_TYPE);
    } else {
        context.lockBuffer(CN_QuantifierSegmenter::SEGMENTER_TYPE);
    }
}

void CN_QuantifierSegmenter::reset() {
    number_start_ = -1;
    number_end_ = -1;
    count_hits_.clear();
}

void CN_QuantifierSegmenter::processCNumber(AnalyzeContext& context) {
    if (number_start_ == -1 && number_end_ == -1) {
        // Initial state
        char32_t currentChar = context.getCurrentChar();
        if (CharacterUtil::CHAR_CHINESE == context.getCurrentCharType() &&
            currentChar < UNICODE_MAX && chinese_number_chars_table_[currentChar]) {
            // Record the starting and ending positions of numeral words.
            number_start_ = context.getCursor();
            number_end_ = context.getCursor();
        }
    } else {
        // Processing status
        if (CharacterUtil::CHAR_CHINESE == context.getCurrentCharType() &&
            chinese_number_chars_table_[context.getCurrentChar()] != 0) {
            // Record the end position of numeral words
            number_end_ = context.getCursor();
        } else {
            // Output numeral
            outputNumLexeme(context);
            number_start_ = -1;
            number_end_ = -1;
        }
    }

    if ((number_start_ != -1 && number_end_ != -1) && context.isBufferConsumed()) {
        outputNumLexeme(context);
        number_start_ = -1;
        number_end_ = -1;
    }
}

void CN_QuantifierSegmenter::processCount(AnalyzeContext& context) {
    // Determine whether a quantifier scan needs to be initiated
    if (!needCountScan(context)) {
        return;
    }
    const auto& typedRuneArray = context.getTypedRuneArray();

    if (CharacterUtil::CHAR_CHINESE == context.getCurrentCharType()) {
        // Prioritize processing the prefixes that have already been matched
        if (!count_hits_.empty()) {
            auto it = count_hits_.begin();
            while (it != count_hits_.end()) {
                Dictionary::getSingleton()->matchWithHit(typedRuneArray, it->getCharEnd(), *it);
                if (it->isMatch()) {
                    Lexeme newLexeme(context.getBufferOffset(), it->getByteBegin(),
                                     it->getByteEnd() - it->getByteBegin(), Lexeme::Type::Count,
                                     it->getCharBegin(), it->getCharEnd());
                    context.addLexeme(newLexeme);
                    if (!it->isPrefix()) {
                        it = count_hits_.erase(it);
                    } else {
                        ++it;
                    }
                } else if (it->isUnmatch()) {
                    it = count_hits_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        // Perform a single-character match at the current pointer position.
        auto singleCharHit = Dictionary::getSingleton()->matchInQuantifierDict(
                typedRuneArray, context.getCursor(), 1);
        if (singleCharHit.isMatch()) {
            Lexeme newLexeme(context.getBufferOffset(), context.getCurrentCharOffset(),
                             context.getCurrentCharLen(), Lexeme::Type::Count, context.getCursor(),
                             context.getCursor());
            context.addLexeme(newLexeme);

            if (singleCharHit.isPrefix()) {
                count_hits_.push_back(singleCharHit);
            }
        } else if (singleCharHit.isPrefix()) {
            count_hits_.push_back(singleCharHit);
        }
    } else {
        count_hits_.clear();
    }
    if (context.isBufferConsumed()) {
        count_hits_.clear();
    }
}

bool CN_QuantifierSegmenter::needCountScan(AnalyzeContext& context) {
    if ((number_start_ != -1 && number_end_ != -1) || !count_hits_.empty()) {
        return true;
    } else {
        // Find an adjacent numerals
        if (!context.getOrgLexemes()->isEmpty()) {
            auto l = context.getOrgLexemes()->peekLast();
            if ((l->getType() == Lexeme::Type::CNum || l->getType() == Lexeme::Type::Arabic) &&
                (l->getCharEnd() + 1 == context.getCursor())) {
                return true;
            }
        }
    }
    return false;
}

void CN_QuantifierSegmenter::outputNumLexeme(AnalyzeContext& context) {
    if (number_start_ > -1 && number_end_ > -1) {
        const auto& typedRuneArray = context.getTypedRuneArray();

        Lexeme newLexeme(context.getBufferOffset(), typedRuneArray[number_start_].offset,
                         (number_end_ - number_start_ + 1) * 3, Lexeme::Type::CNum, number_start_,
                         number_end_);
        context.addLexeme(newLexeme);
    }
}
