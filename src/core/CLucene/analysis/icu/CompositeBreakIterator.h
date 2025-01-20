#pragma once

#include <unicode/umachine.h>
#include <unicode/unistr.h>
#include <unicode/utext.h>

#include <memory>
#include <vector>

#include "BreakIteratorWrapper.h"
#include "ICUCommon.h"
#include "ICUTokenizerConfig.h"
#include "ScriptIterator.h"

namespace lucene::analysis {

class CompositeBreakIterator {
public:
    CompositeBreakIterator(const ICUTokenizerConfigPtr& config);
    ~CompositeBreakIterator() = default;

    void initialize();
    int32_t next();
    int32_t current();
    int32_t getRuleStatus();
    int32_t getScriptCode();
    void setText(const UChar* text, int32_t start, int32_t length);

private:
    BreakIteratorWrapper* getBreakIterator(int32_t scriptCode);

    const UChar* text_ = nullptr;

    ICUTokenizerConfigPtr config_;
    std::vector<BreakIteratorWrapperPtr> wordBreakers_;
    BreakIteratorWrapper* rbbi_ = nullptr;
    ScriptIteratorPtr scriptIterator_;
};
using CompositeBreakIteratorPtr = std::unique_ptr<CompositeBreakIterator>;

} // namespace lucene::analysis