#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <unordered_set>

#include "CLucene/analysis/AnalysisHeader.h"
#include "CLucene/analysis/standard95/StandardTokenizerImpl.h"
#include "CLucene/util/stringUtil.h"

namespace lucene::analysis::standard95 {

static std::unordered_set<std::string_view> stop_words = {
    "a",    "an",   "and",  "are",  "as",   "at",    "be",   "but",   "by",
    "for",  "if",   "in",   "into", "is",   "it",    "no",   "not",   "of",
    "on",   "or",   "such", "that", "the",  "their", "then", "there", "these",
    "they", "this", "to",   "was",  "will", "with"};

class StandardTokenizer : public Tokenizer {
 public:
  StandardTokenizer(lucene::util::Reader* in, bool useStopWords)
      : Tokenizer(in), useStopWords_(useStopWords) {
    scanner_ = std::make_unique<StandardTokenizerImpl>(in);
  }

  Token* next(Token* token) override {
    skippedPositions = 0;

    while (true) {
      int32_t tokenType = scanner_->getNextToken();

      if (tokenType == StandardTokenizerImpl::YYEOF) {
        return nullptr;
      }

      if (scanner_->yylength() <= maxTokenLength) {
        std::string_view term = scanner_->getText();
        if (tokenType == StandardTokenizerImpl::WORD_TYPE) {
          std::transform(term.begin(), term.end(),
                         const_cast<char*>(term.data()),
                         [](char c) { return to_lower(c); });
          if (useStopWords_ && stop_words.count(term)) {
            skippedPositions++;
            continue;
          }
        }
        // std::cout << term << std::endl;
        token->setNoCopy(term.data(), 0, term.size());
        return token;
      } else {
        skippedPositions++;
      }
    }

    return nullptr;
  }

  void reset(lucene::util::Reader* input) override {
    Tokenizer::reset(input);
    scanner_->yyreset(input);
    skippedPositions = 0;
  };

 private:
  bool useStopWords_ = true;

  std::unique_ptr<StandardTokenizerImpl> scanner_;

  int32_t skippedPositions = 0;
  int32_t maxTokenLength = 255;
};

}  // namespace lucene::analysis::standard95