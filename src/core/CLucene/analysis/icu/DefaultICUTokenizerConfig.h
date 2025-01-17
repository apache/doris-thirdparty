#pragma once

#include "ICUTokenizerConfig.h"

namespace lucene::analysis {

class DefaultICUTokenizerConfig : public ICUTokenizerConfig {
public:
    DefaultICUTokenizerConfig(bool cjkAsWords, bool myanmarAsWords);
    ~DefaultICUTokenizerConfig() override = default;

    void initialize(const std::string& dictPath) override;
    bool combineCJ() override { return cjkAsWords_; }
    icu::BreakIterator* getBreakIterator(int32_t script) override;

private:
    static void readBreakIterator(BreakIteratorPtr& rbbi, const std::string& filename);

private:
    static BreakIteratorPtr cjkBreakIterator_;
    static BreakIteratorPtr defaultBreakIterator_;
    static BreakIteratorPtr myanmarSyllableIterator_;

    bool cjkAsWords_ = false;
    bool myanmarAsWords_ = false;
};

} // namespace lucene::analysis