#pragma once

#include <zstd.h>

#include <cstdint>
#include <iostream>
#include <string_view>
#include <vector>

#include "CLucene.h"
#include "CLucene/store/IndexOutput.h"

namespace store_v2 {

// Note: This is only safe for usage that is bounded in the number of bytes written
class GrowableByteArrayDataOutput : public CL_NS(store)::IndexOutput {
public:
    GrowableByteArrayDataOutput() : bytes_(INITIAL_SIZE) {}
    ~GrowableByteArrayDataOutput() override = default;

    void writeByte(uint8_t b) override {
        ensureCapacity(1);
        bytes_[nextWrite_++] = b;
    }

    void writeBytes(const uint8_t* b, const int32_t len) override { writeBytes(b, len, 0); }

    void writeBytes(const uint8_t* b, const int32_t len, const int32_t offset) override {
        if (len == 0) {
            return;
        }
        ensureCapacity(len);
        std::copy(b + offset, b + offset + len, bytes_.data() + nextWrite_);
        nextWrite_ += len;
    }

    void close() override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException GrowableByteArrayDataOutput::close");
    }

    int64_t getFilePointer() const override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException GrowableByteArrayDataOutput::getFilePointer");
    }

    void seek(const int64_t pos) override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException GrowableByteArrayDataOutput::seek");
    }

    int64_t length() const override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException GrowableByteArrayDataOutput::length");
    }

    void flush() override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException GrowableByteArrayDataOutput::flush");
    }

    void writeTo(CL_NS(store)::IndexOutput* out) { out->writeBytes(bytes_.data(), nextWrite_); }

    void writeCompressedTo(CL_NS(store)::IndexOutput* out) {
        if (nextWrite_ == 0) {
            return;
        }

        auto compress = [](const std::string_view& source) {
            size_t compressBound = ZSTD_compressBound(source.size());
            std::string compressed(compressBound, 0);

            size_t compressedSize = ZSTD_compress(compressed.data(), compressBound, source.data(),
                                                  source.size(), 3);

            if (ZSTD_isError(compressedSize)) {
                _CLTHROWA(CL_ERR_Runtime, "Compression failed");
            }

            compressed.resize(compressedSize);
            return compressed;
        };

        auto compress_data = compress(std::string_view((const char*)bytes_.data(), nextWrite_));
        out->writeVInt(compress_data.size());
        out->writeBytes((const uint8_t*)compress_data.data(), compress_data.size());

        nextWrite_ = 0;
    }

    size_t size() const { return nextWrite_; }

private:
    void ensureCapacity(int capacityToWrite) {
        assert(capacityToWrite > 0);
        if (nextWrite_ + capacityToWrite > bytes_.capacity()) {
            size_t newCapacity = std::max(bytes_.capacity() * 2, nextWrite_ + capacityToWrite);
            bytes_.reserve(newCapacity);
        }
        if (nextWrite_ + capacityToWrite > bytes_.size()) {
            bytes_.resize(nextWrite_ + capacityToWrite);
        }
    }

private:
    static constexpr int32_t INITIAL_SIZE = 1 << 8;

    size_t nextWrite_ = 0;
    std::vector<uint8_t> bytes_;
};

} // namespace store_v2