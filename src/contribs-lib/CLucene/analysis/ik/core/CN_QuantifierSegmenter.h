#ifndef CLUCENE_CN_QUANTIFIERSEGMENTER_H
#define CLUCENE_CN_QUANTIFIERSEGMENTER_H

#include <memory>
#include <vector>

#include "AnalyzeContext.h"
#include "CLucene/analysis/ik/util/IKContainer.h"
#include "ISegmenter.h"
CL_NS_DEF2(analysis, ik)

class CN_QuantifierSegmenter : public ISegmenter {
public:
    static constexpr AnalyzeContext::SegmenterType SEGMENTER_TYPE =
            AnalyzeContext::SegmenterType::CN_QUANTIFIER;
    static const std::string SEGMENTER_NAME;
    static const std::u32string CHINESE_NUMBERS;

    CN_QuantifierSegmenter();
    ~CN_QuantifierSegmenter() override = default;

    void analyze(AnalyzeContext& context) override;
    void reset() override;

private:
    void processCNumber(AnalyzeContext& context);
    void processCount(AnalyzeContext& context);
    bool needCountScan(AnalyzeContext& context);
    void outputNumLexeme(AnalyzeContext& context);

    int number_start_;
    int number_end_;
    IKVector<Hit> count_hits_;
};
CL_NS_END2
#endif //CLUCENE_CN_QUANTIFIERSEGMENTER_H
