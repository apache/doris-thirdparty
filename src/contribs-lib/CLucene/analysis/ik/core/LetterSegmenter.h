#ifndef CLUCENE_LETTERSEGMENTER_H
#define CLUCENE_LETTERSEGMENTER_H

#include <algorithm>
#include <memory>
#include <vector>

#include "AnalyzeContext.h"
#include "CLucene/analysis/ik/util/IKContainer.h"
#include "ISegmenter.h"

CL_NS_DEF2(analysis, ik)

class LetterSegmenter : public ISegmenter {
public:
    static constexpr AnalyzeContext::SegmenterType SEGMENTER_TYPE =
            AnalyzeContext::SegmenterType::LETTER_SEGMENTER;
    static const std::string SEGMENTER_NAME;
    LetterSegmenter();
    ~LetterSegmenter() override = default;

    void analyze(AnalyzeContext& context) override;
    void reset() override;

private:
    bool processEnglishLetter(AnalyzeContext& context);
    bool processArabicLetter(AnalyzeContext& context);
    bool processMixLetter(AnalyzeContext& context);
    bool isLetterConnector(char input);
    bool isNumConnector(char input);

    int start_ {-1};
    int end_ {-1};
    int english_start_ {-1};
    int english_end_ {-1};
    int arabic_start_ {-1};
    int arabic_end_ {-1};

    IKVector<char> letter_connectors_;
    IKVector<char> num_connectors_;
};
CL_NS_END2

#endif //CLUCENE_LETTERSEGMENTER_H