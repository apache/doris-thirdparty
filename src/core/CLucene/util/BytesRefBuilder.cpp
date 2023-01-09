#include "BytesRefBuilder.h"

CL_NS_DEF(util)


BytesRefBuilder::BytesRefBuilder() : ref_(std::make_shared<BytesRef>()) {}

std::vector<uint8_t> BytesRefBuilder::bytes() { return ref_->bytes; }

int BytesRefBuilder::length() { return ref_->length; }

void BytesRefBuilder::setLength(int length) { ref_->length = length; }

uint8_t BytesRefBuilder::byteAt(int offset) { return ref_->bytes.at(offset); }

void BytesRefBuilder::setByteAt(int offset, uint8_t b) { ref_->bytes.at(offset) = b; }

void BytesRefBuilder::grow(int capacity) {
    ref_->bytes.resize(capacity);
}

void BytesRefBuilder::append(uint8_t b) {
    grow(ref_->length + 1);
    ref_->bytes.at(ref_->length++) = b;
}

void BytesRefBuilder::append(std::vector<uint8_t> &b, int off, int len) {
    grow(ref_->length + len);
    //std::copy(b, off, ref_->bytes, ref_->length, len);
    //TODO:need to check overflow here
    std::copy(b.begin()+off, b.begin()+off+len, ref_->bytes.begin()+ref_->length);
    ref_->length += len;
}

void BytesRefBuilder::append(const std::shared_ptr<BytesRef>& ref) {
    append(ref->bytes, ref->offset, ref->length);
}

void BytesRefBuilder::append(const std::shared_ptr<BytesRefBuilder>& builder) {
    append(builder->get());
}

void BytesRefBuilder::clear() { setLength(0); }

std::shared_ptr<BytesRef> BytesRefBuilder::get() {
    assert((ref_->offset == 0,
            L"Modifying the offset of the returned ref is illegal"));
    return ref_;
}
CL_NS_END