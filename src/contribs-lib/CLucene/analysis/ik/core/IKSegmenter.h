#ifndef CLUCENE_IKSEGMENTER_H
#define CLUCENE_IKSEGMENTER_H

#include <iostream>
#include <memory>
#include <vector>

#include "AnalyzeContext.h"
#include "CJKSegmenter.h"
#include "CN_QuantifierSegmenter.h"
#include "IKArbitrator.h"
#include "ISegmenter.h"
#include "LetterSegmenter.h"
CL_NS_DEF2(analysis, ik)

class IKSegmenter {
public:
    IKSegmenter();
    void setContext(lucene::util::Reader* input, std::shared_ptr<Configuration> config);
    bool next(Lexeme& lexeme);
    void reset(lucene::util::Reader* newInput);
    int getLastUselessCharNum();

private:
    std::vector<std::unique_ptr<ISegmenter>> loadSegmenters();
    IKMemoryPool<Cell> pool_;
    lucene::util::Reader* input_;
    std::unique_ptr<AnalyzeContext> context_;
    std::vector<std::unique_ptr<ISegmenter>> segmenters_;
    IKArbitrator arbitrator_;
    std::shared_ptr<Configuration> config_;
};
CL_NS_END2

#endif //CLUCENE_IKSEGMENTER_H
