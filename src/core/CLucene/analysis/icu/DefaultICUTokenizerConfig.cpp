#include "DefaultICUTokenizerConfig.h"

#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <atomic>

namespace lucene::analysis {

BreakIteratorPtr DefaultICUTokenizerConfig::cjkBreakIterator_;
BreakIteratorPtr DefaultICUTokenizerConfig::defaultBreakIterator_;
BreakIteratorPtr DefaultICUTokenizerConfig::myanmarSyllableIterator_;

DefaultICUTokenizerConfig::DefaultICUTokenizerConfig(bool cjkAsWords, bool myanmarAsWords) {
    cjkAsWords_ = cjkAsWords;
    myanmarAsWords_ = myanmarAsWords;
}

void DefaultICUTokenizerConfig::initialize(const std::string& dictPath) {
    static std::atomic<bool> initialized_(false);
    if (!initialized_) {
        static std::mutex mutex;
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized_) {
            try {
                UErrorCode status = U_ZERO_ERROR;
                cjkBreakIterator_.reset(
                        icu::BreakIterator::createWordInstance(icu::Locale::getRoot(), status));
                if (U_FAILURE(status)) {
                    std::string error_msg = "Failed to create CJK BreakIterator: ";
                    error_msg += u_errorName(status);
                    _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
                }

                readBreakIterator(defaultBreakIterator_, dictPath + "/uax29/Default.txt");
                readBreakIterator(myanmarSyllableIterator_,
                                  dictPath + "/uax29/MyanmarSyllable.txt");

                initialized_ = true;
            } catch (...) {
                cjkBreakIterator_.reset();
                defaultBreakIterator_.reset();
                myanmarSyllableIterator_.reset();
                throw; // Clean up resources and rethrow the original exception to the caller
            }
        }
    }
}

icu::BreakIterator* DefaultICUTokenizerConfig::getBreakIterator(int32_t script) {
    UErrorCode status = U_ZERO_ERROR;
    icu::BreakIterator* clone = nullptr;
    switch (script) {
    case USCRIPT_JAPANESE:
        clone = cjkBreakIterator_->clone();
        break;
    case USCRIPT_MYANMAR:
        if (myanmarAsWords_) {
            clone = defaultBreakIterator_->clone();
        } else {
            clone = myanmarSyllableIterator_->clone();
        }
        break;
    default:
        clone = defaultBreakIterator_->clone();
        break;
    }
    if (clone == nullptr) {
        std::string error_msg = "UBreakIterator clone failed: ";
        error_msg += u_errorName(status);
        _CLTHROWT(CL_ERR_Runtime, error_msg.c_str());
    }
    return clone;
}

void DefaultICUTokenizerConfig::readBreakIterator(BreakIteratorPtr& rbbi,
                                                  const std::string& filename) {
    std::ifstream in(filename, std::ios::binary);
    if (!in) {
        std::string error_msg = "Unable to open the file: " + filename;
        _CLTHROWT(CL_ERR_IO, error_msg.c_str());
    }

    std::ostringstream ss;
    ss << in.rdbuf();
    in.close();

    std::string utf8Content = ss.str();
    icu::UnicodeString rulesData(utf8Content.data());

    UParseError parseErr;
    UErrorCode status = U_ZERO_ERROR;
    rbbi = std::make_unique<icu::RuleBasedBreakIterator>(rulesData, parseErr, status);
    if (U_FAILURE(status)) {
        std::string error_msg = "ubrk_openRules failed: ";
        error_msg += u_errorName(status);
        _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
    }
    if (parseErr.line != 0 || parseErr.offset != 0) {
        std::string error_msg = "Syntax error in break rules at line ";
        error_msg += std::to_string(parseErr.line);
        error_msg += ", offset ";
        error_msg += std::to_string(parseErr.offset);
        _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
    }
}

} // namespace lucene::analysis