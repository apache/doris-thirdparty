#ifndef CLUCENE_ISEGMENTER_H
#define CLUCENE_ISEGMENTER_H
#include "AnalyzeContext.h"
#include "CLucene/_ApiHeader.h"

CL_NS_DEF2(analysis, ik)

class CLUCENE_EXPORT ISegmenter {
public:
    virtual ~ISegmenter() {}

    // Read the next possible token from the analyzer
    // param context Segmentation algorithm context
    virtual void analyze(AnalyzeContext& context) = 0;

    // Reset the sub-analyzer state
    virtual void reset() = 0;
};

CL_NS_END2
#endif //CLUCENE_ISEGMENTER_H
