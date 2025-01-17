#pragma once

#include <unicode/utext.h>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "CompositeBreakIterator.h"
#include "DefaultICUTokenizerConfig.h"
#include "ICUCommon.h"

namespace lucene::analysis {

class ICUTokenizer : public Tokenizer {
public:
    ICUTokenizer();
    ICUTokenizer(bool lowercase, bool ownReader);
    ~ICUTokenizer() override = default;

    void initialize(const std::string& dictPath);
    Token* next(Token* token) override;
    void reset(lucene::util::Reader* input) override;

private:
    std::string utf8Str_;
    icu::UnicodeString buffer_;

    ICUTokenizerConfigPtr config_;
    CompositeBreakIteratorPtr breaker_;
};

} // namespace lucene::analysis