#pragma once

#include "CLucene/analysis/standard95/StandardTokenizer.h"

namespace lucene::analysis::standard95 {

class StandardAnalyzer : public Analyzer {
 public:
  StandardAnalyzer() : Analyzer() { 
    _lowercase = true;
    _ownReader = false;
    _stopwords = nullptr;
  }

  bool isSDocOpt() override { return true; }
  
  TokenStream* tokenStream(const TCHAR* fieldName,
                           lucene::util::Reader* reader) override {
    return _CLNEW StandardTokenizer(reader, _lowercase, _stopwords, _ownReader);
  }

  TokenStream* reusableTokenStream(const TCHAR* fieldName,
                                   lucene::util::Reader* reader) override {
    if (tokenizer_ == nullptr) {
      tokenizer_ = new StandardTokenizer(reader, _lowercase, _stopwords);
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

 private:
  StandardTokenizer* tokenizer_ = nullptr;
};

}