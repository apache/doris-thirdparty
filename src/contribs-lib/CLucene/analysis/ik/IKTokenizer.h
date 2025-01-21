#ifndef CLUCENE_IKTOKENIZER_H
#define CLUCENE_IKTOKENIZER_H
#include <memory>
#include <string_view>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "CLucene/analysis/LanguageBasedAnalyzer.h"
#include "CLucene/analysis/ik/cfg/Configuration.h"
#include "CLucene/analysis/ik/core/IKSegmenter.h"
CL_NS_DEF2(analysis, ik)
CL_NS_USE(analysis)

class IKSegmentSingleton {
public:
    static IKSegmenter& getInstance() {
        static IKSegmenter instance;
        return instance;
    }

private:
    IKSegmentSingleton() = default;
};

class IKTokenizer : public lucene::analysis::Tokenizer {
private:
    int32_t buffer_index_ {0};
    int32_t data_length_ {0};
    std::string buffer_;
    std::vector<std::string> tokens_text_;
    std::shared_ptr<Configuration> config_;

public:
    explicit IKTokenizer(lucene::util::Reader* reader, std::shared_ptr<Configuration> config);
    explicit IKTokenizer(lucene::util::Reader* reader, std::shared_ptr<Configuration> config,
                         bool is_smart, bool use_lowercase, bool own_reader = false);
    ~IKTokenizer() override = default;

    lucene::analysis::Token* next(lucene::analysis::Token* token) override;
    void reset(lucene::util::Reader* reader) override;
};

CL_NS_END2
#endif //CLUCENE_IKTOKENIZER_H
