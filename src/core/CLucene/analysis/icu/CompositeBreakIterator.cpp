#include "CompositeBreakIterator.h"

#include <unicode/unistr.h>

#include <memory>

namespace lucene::analysis {

CompositeBreakIterator::CompositeBreakIterator(const ICUTokenizerConfigPtr& config)
        : config_(config) {
    scriptIterator_ = std::make_unique<ScriptIterator>(config->combineCJ());
    wordBreakers_.resize(u_getIntPropertyMaxValue(UCHAR_SCRIPT) + 1);
}

void CompositeBreakIterator::initialize() {
    scriptIterator_->initialize();
}

int32_t CompositeBreakIterator::next() {
    int32_t next = rbbi_->next();
    while (next == UBRK_DONE && scriptIterator_->next()) {
        rbbi_ = getBreakIterator(scriptIterator_->getScriptCode());
        rbbi_->setText(text_, scriptIterator_->getScriptStart(),
                       scriptIterator_->getScriptLimit() - scriptIterator_->getScriptStart());
        next = rbbi_->next();
    }
    return (next == UBRK_DONE) ? UBRK_DONE : next + scriptIterator_->getScriptStart();
}

int32_t CompositeBreakIterator::current() {
    int32_t current = rbbi_->current();
    return (current == UBRK_DONE) ? UBRK_DONE : current + scriptIterator_->getScriptStart();
}

int32_t CompositeBreakIterator::getRuleStatus() {
    return rbbi_->getRuleStatus();
}

int32_t CompositeBreakIterator::getScriptCode() {
    return scriptIterator_->getScriptCode();
}

void CompositeBreakIterator::setText(const UChar* text, int32_t start, int32_t length) {
    text_ = text;
    scriptIterator_->setText(text_, start, length);
    if (scriptIterator_->next()) {
        rbbi_ = getBreakIterator(scriptIterator_->getScriptCode());
        rbbi_->setText(text_, scriptIterator_->getScriptStart(),
                       scriptIterator_->getScriptLimit() - scriptIterator_->getScriptStart());
    } else {
        rbbi_ = getBreakIterator(USCRIPT_COMMON);
        rbbi_->setText(text_, 0, 0);
    }
}

BreakIteratorWrapper* CompositeBreakIterator::getBreakIterator(int32_t scriptCode) {
    if (!wordBreakers_[scriptCode]) {
        auto wordBreak =
                std::make_unique<BreakIteratorWrapper>(config_->getBreakIterator(scriptCode));
        wordBreak->initialize();
        wordBreakers_[scriptCode].swap(wordBreak);
    }
    return wordBreakers_[scriptCode].get();
}

} // namespace lucene::analysis