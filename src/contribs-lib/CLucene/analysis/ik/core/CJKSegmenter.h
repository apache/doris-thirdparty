#ifndef CLUCENE_CJKSEGMENTER_H
#define CLUCENE_CJKSEGMENTER_H

#include <list>
#include <memory>
#include <string>

#include "AnalyzeContext.h"
#include "CLucene/analysis/ik/dic/Dictionary.h"
#include "CLucene/analysis/ik/util/IKContainer.h"
#include "CharacterUtil.h"
#include "ISegmenter.h"

CL_NS_DEF2(analysis, ik)

class CJKSegmenter : public ISegmenter {
private:
    static constexpr AnalyzeContext::SegmenterType SEGMENTER_TYPE =
            AnalyzeContext::SegmenterType::CJK_SEGMENTER;
    IKList<Hit> tmp_hits_;

public:
    CJKSegmenter();

    void analyze(AnalyzeContext& context) override;
    void reset() override;
};

CL_NS_END2
#endif //CLUCENE_CJKSEGMENTER_H
