#include "IKSegmenter.h"
CL_NS_USE2(analysis, ik)

IKSegmenter::IKSegmenter()
        : pool_(10000),
          context_(std::make_unique<AnalyzeContext>(pool_)),
          segmenters_(loadSegmenters()),
          arbitrator_(IKArbitrator(pool_)) {}

void IKSegmenter::setContext(lucene::util::Reader* input, std::shared_ptr<Configuration> config) {
    context_->reset();
    input_ = input;
    config_ = config;
    context_->setConfig(config);
}

std::vector<std::unique_ptr<ISegmenter>> IKSegmenter::loadSegmenters() {
    std::vector<std::unique_ptr<ISegmenter>> segmenters;
    segmenters.push_back(std::make_unique<LetterSegmenter>());
    segmenters.push_back(std::make_unique<CN_QuantifierSegmenter>());
    segmenters.push_back(std::make_unique<CJKSegmenter>());
    return segmenters;
}

bool IKSegmenter::next(Lexeme& lexeme) {
    while (!context_->getNextLexeme(lexeme)) {
        // Read data from the reader and fill the buffer
        int available = context_->fillBuffer(input_);
        if (available <= 0) {
            context_->reset();
            return false;
        } else {
            context_->initCursor();
            do {
                for (const auto& segmenter : segmenters_) {
                    segmenter->analyze(*context_);
                }
                // The buffer is nearly read, new characters need to be read in.
                if (context_->needRefillBuffer()) {
                    break;
                }
            } while (context_->moveCursor());
            for (const auto& segmenter : segmenters_) {
                segmenter->reset();
            }
        }
        arbitrator_.process(*context_, config_->isUseSmart());
        context_->outputToResult();
        context_->markBufferOffset();
    }
    return true;
}

void IKSegmenter::reset(lucene::util::Reader* newInput) {
    input_ = newInput;
    context_->reset();
    for (const auto& segmenter : segmenters_) {
        segmenter->reset();
    }
}

int IKSegmenter::getLastUselessCharNum() {
    return context_->getLastUselessCharNum();
}