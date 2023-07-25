#pragma once

#include "CLucene/analysis/standard95/StandardTokenizer.h"

namespace lucene::analysis::standard95 {

class StandardAnalyzer : public Analyzer {
 public:
  TokenStream* tokenStream(const TCHAR* fieldName,
                           lucene::util::Reader* reader) override {
    return _CLNEW StandardTokenizer(reader, useStopWords_);
  }

  TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                   lucene::util::Reader* reader) override {
    if (tokenizer_ == nullptr) {
      tokenizer_ = new StandardTokenizer(reader, useStopWords_);
    } else {
      tokenizer_->reset(reader);
    }
    return tokenizer_;
  };

  virtual ~StandardAnalyzer() {
    if (tokenizer_) {
      delete tokenizer_;
      tokenizer_ = nullptr;
    }
  }

  void useStopWords(bool useStopWords) {
    useStopWords_ = useStopWords;
  }

 private:
  bool useStopWords_ = true;

  StandardTokenizer* tokenizer_ = nullptr;
};

}