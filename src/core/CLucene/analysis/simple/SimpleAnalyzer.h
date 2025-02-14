#pragma once

#include <memory>

#include "SimpleTokenizer.h"

namespace lucene::analysis_v2 {

class SimpleAnalyzer : public Analyzer {
public:
    SimpleAnalyzer() {
        _lowercase = true;
        _ownReader = false;
    }

    ~SimpleAnalyzer() override = default;

    bool isSDocOpt() override { return true; }

    TokenStream* tokenStream(const TCHAR* fieldName, lucene::util::Reader* reader) override {
        auto tokenizer = _CLNEW SimpleTokenizer(_lowercase, _ownReader);
        tokenizer->reset(reader);
        return tokenizer;
    }

    TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                     lucene::util::Reader* reader) override {
        if (tokenizer_ == nullptr) {
            tokenizer_ = std::make_unique<SimpleTokenizer>(_lowercase, _ownReader);
        }
        tokenizer_->reset(reader);
        return tokenizer_.get();
    };

private:
    std::unique_ptr<SimpleTokenizer> tokenizer_;
};

} // namespace lucene::analysis_v2