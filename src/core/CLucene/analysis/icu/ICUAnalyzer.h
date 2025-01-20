#pragma once

#include <memory>

#include "ICUTokenizer.h"

namespace lucene::analysis {

class ICUAnalyzer : public Analyzer {
public:
    ICUAnalyzer() {
        _lowercase = true;
        _ownReader = false;
    }

    ~ICUAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    void initDict(const std::string& dictPath) override { dictPath_ = dictPath; }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override {
        auto tokenizer = _CLNEW ICUTokenizer(_lowercase, _ownReader);
        tokenizer->initialize(dictPath_);
        return tokenizer;
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     lucene::util::Reader* reader) override {
        if (tokenizer_ == nullptr) {
            tokenizer_ = std::make_unique<ICUTokenizer>(_lowercase, _ownReader);
            tokenizer_->initialize(dictPath_);
        }
        tokenizer_->reset(reader);
        return tokenizer_.get();
    };

private:
    std::string dictPath_;
    std::unique_ptr<ICUTokenizer> tokenizer_;
};

} // namespace lucene::analysis