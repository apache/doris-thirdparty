#pragma once

#include <unicode/utext.h>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "CLucene/analysis/icu/ICUCommon.h"

using namespace lucene::analysis;

namespace lucene::analysis_v2 {

class SimpleTokenizer : public Tokenizer {
public:
    SimpleTokenizer();
    SimpleTokenizer(bool lowercase, bool ownReader);
    ~SimpleTokenizer() override = default;

    Token* next(Token* token) override;
    void reset(lucene::util::Reader* input) override;

    void cut();

private:
    int32_t bufferIndex = 0;
    int32_t dataLen = 0;
    std::string buffer_;
    std::vector<std::string_view> tokens_text;
};

} // namespace lucene::analysis_v2