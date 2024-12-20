#pragma once

#include <zstd.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <vector>

#include "CLucene.h"
#include "CLucene/store/IndexInput.h"

namespace store_v2 {

// DataInput backed by a byte array. <b>WARNING:</b> This class omits all low-level checks.
class ByteArrayDataInput : public CL_NS(store)::IndexInput {
public:
    ByteArrayDataInput() : owns_(true), bytes_(new std::vector<uint8_t>()) {}

    ByteArrayDataInput(std::vector<uint8_t>* bytes) { reset(bytes); }

    ~ByteArrayDataInput() override {
        if (owns_) {
            if (bytes_ != nullptr) {
                delete bytes_;
                bytes_ = nullptr;
            }
        }
    }

    ByteArrayDataInput& operator=(const ByteArrayDataInput& other) {
        if (this == &other) {
            return *this;
        }

        if (owns_ && bytes_ != nullptr) {
            delete bytes_;
            bytes_ = nullptr;
        }

        owns_ = true;
        pos_ = other.pos_;
        limit_ = other.limit_;
        bytes_ = new std::vector<uint8_t>(*other.bytes_);

        return *this;
    }

    void reset(std::vector<uint8_t>* bytes) { reset(bytes, 0, bytes->size()); }

    void reset(std::vector<uint8_t>* bytes, int32_t offset, int32_t len) {
        bytes_ = bytes;
        pos_ = offset;
        limit_ = offset + len;
    }

    uint8_t readByte() override { return (*bytes_)[pos_++]; }

    void readBytes(uint8_t* b, const int32_t len) override { readBytes(b, len, 0); }

    void readBytes(uint8_t* b, const int32_t len, int32_t offset) override {
        std::copy(bytes_->begin() + pos_, bytes_->begin() + pos_ + len, b + offset);
        pos_ += len;
    }

    int64_t length() const override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException ByteArrayDataInput::length");
    }

    void seek(const int64_t pos) override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException ByteArrayDataInput::seek");
    }

    int64_t getFilePointer() const override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException ByteArrayDataInput::getFilePointer");
    }

    IndexInput* clone() const override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException ByteArrayDataInput::clone");
    }

    void close() override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException ByteArrayDataInput::close");
    }

    const char* getDirectoryType() const override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException ByteArrayDataInput::getDirectoryType");
    }

    const char* getObjectName() const override {
        _CLTHROWA(CL_ERR_UnsupportedOperation,
                  "UnsupportedOperationException ByteArrayDataInput::getObjectName");
    }

    bool eof() { return pos_ == limit_; }

    void readCompressedFrom(CL_NS(store)::IndexInput* in) {
        auto decompress = [](const std::vector<uint8_t>& compressed,
                             std::vector<uint8_t>* decompressed) {
            unsigned long long decompressedSize =
                    ZSTD_getFrameContentSize(compressed.data(), compressed.size());

            if (decompressedSize == ZSTD_CONTENTSIZE_ERROR) {
                _CLTHROWA(CL_ERR_Runtime, "Not compressed by zstd");
            }
            if (decompressedSize == ZSTD_CONTENTSIZE_UNKNOWN) {
                _CLTHROWA(CL_ERR_Runtime, "Original size unknown");
            }

            decompressed->resize(decompressedSize, 0);

            size_t actualDecompressedSize = ZSTD_decompress(decompressed->data(), decompressedSize,
                                                            compressed.data(), compressed.size());

            if (ZSTD_isError(actualDecompressedSize)) {
                _CLTHROWA(CL_ERR_Runtime, "Decompression failed");
            }

            return actualDecompressedSize;
        };

        int32_t compress_size = in->readVInt();
        std::vector<uint8_t> compress_buffer(compress_size);
        in->readBytes(compress_buffer.data(), compress_size);

        pos_ = 0;
        bytes_->clear();
        limit_ = decompress(compress_buffer, bytes_);
    }

private:
    bool owns_ = false;

    int32_t pos_ = 0;
    int32_t limit_ = 0;
    std::vector<uint8_t>* bytes_ = nullptr;
};

} // namespace store_v2