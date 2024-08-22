#pragma once

#include <zstd.h>

#include <algorithm>
#include <cstdint>
#include <vector>
#include <iostream>

#include "CLucene.h"
#include "CLucene/store/IndexInput.h"

namespace v2 {

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

    void reset(std::vector<uint8_t>* bytes) { reset(bytes, 0, bytes->size()); }

    void reset(std::vector<uint8_t>* bytes, int32_t offset, int32_t len) {
        bytes_ = bytes;
        pos_ = offset;
        limit_ = offset + len;
    }

    uint8_t readByte() override { return (*bytes_)[pos_++]; }

    void readBytes(uint8_t* b, const int32_t len) override { readBytes(b, 0, len); }

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
            // 获取解压缩后的大小
            unsigned long long decompressedSize =
                    ZSTD_getFrameContentSize(compressed.data(), compressed.size());

            if (decompressedSize == ZSTD_CONTENTSIZE_ERROR) {
                _CLTHROWA(CL_ERR_Runtime, "Not compressed by zstd");
            }
            if (decompressedSize == ZSTD_CONTENTSIZE_UNKNOWN) {
                _CLTHROWA(CL_ERR_Runtime, "Original size unknown");
            }

            // 创建一个字符串来存放解压缩后的数据
            decompressed->resize(decompressedSize, 0);

            // 执行解压缩
            size_t actualDecompressedSize = ZSTD_decompress(decompressed->data(), decompressedSize,
                                                            compressed.data(), compressed.size());

            // 检查解压缩是否成功
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

} // namespace v2