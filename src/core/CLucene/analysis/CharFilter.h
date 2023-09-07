#pragma once

#include <memory>

#include "CLucene/util/CLStreams.h"

namespace lucene::analysis {

class CharFilter : public lucene::util::Reader {
public:
    CharFilter(lucene::util::Reader* input) : input_(input) {}

    virtual ~CharFilter() {
        if (input_) {
            delete input_;
            input_ = nullptr;
        }
    }

    int64_t position() override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException CharFilter::position");
    }

    int64_t skip(int64_t ntoskip) override {
        _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException CharFilter::skip");
    }

    size_t size() override {
        _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException CharFilter::size");
    }

protected:
    lucene::util::Reader* input_ = nullptr;
};

} // namespace lucene::analysis