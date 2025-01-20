#pragma once

#include "ICUCommon.h"

namespace lucene::analysis {

class ICUTokenizerConfig {
public:
    ICUTokenizerConfig() = default;
    virtual ~ICUTokenizerConfig() = default;

    virtual void initialize(const std::string& dictPath) = 0;
    virtual icu::BreakIterator* getBreakIterator(int32_t script) = 0;
    virtual bool combineCJ() = 0;

    static const int32_t EMOJI_SEQUENCE_STATUS = 299;
};
using ICUTokenizerConfigPtr = std::shared_ptr<ICUTokenizerConfig>;

} // namespace lucene::analysis