#include "BytesRef.h"
#include "FutureArrays.h"

#include <string.h>

CL_NS_DEF(util)

std::vector<uint8_t> BytesRef::EMPTY_BYTES = std::vector<uint8_t>(0);

BytesRef::BytesRef() : BytesRef((EMPTY_BYTES)) {}

BytesRef::BytesRef(std::vector<uint8_t>& bytes, int offset, int length)
        : bytes(std::move(bytes)), offset(offset), length(length) {
}

BytesRef::BytesRef(std::vector<uint8_t> &bytes) : BytesRef(bytes, 0, bytes.size()) {
}

BytesRef::BytesRef(int capacity) { this->bytes = std::vector<uint8_t>(capacity); }

bool BytesRef::BytesEquals(BytesRef &other) {
    return FutureArrays::Equals(bytes, offset, offset + length,
                                other.bytes, other.offset, other.offset + other.length);
}

std::shared_ptr<BytesRef> BytesRef::clone() {
    return std::make_shared<BytesRef>(bytes, offset, length);
}

std::shared_ptr<BytesRef> BytesRef::deepCopyOf(const std::shared_ptr<BytesRef>& other) {
    std::shared_ptr<BytesRef> copy = std::make_shared<BytesRef>();
    std::copy(other->bytes.begin(),
              other->bytes.begin() + other->offset + other->length,
              copy->bytes.begin());
    copy->offset = 0;
    copy->length = other->length;
    return copy;
}

int BytesRef::CompareTo(BytesRef &other) {
    return FutureArrays::CompareUnsigned(
            bytes, offset, offset + length, other.bytes,
            other.offset, other.offset + other.length);
}

bool BytesRef::isValid() {
    if (bytes.empty()) {
        return false;
    }
    if (length < 0) {
        return false;
    }
    if (length > bytes.size()) {
        return false;
    }
    if (offset < 0) {
        return false;
    }
    if (offset > bytes.size()) {
        return false;
    }
    if (offset + length < 0) {
        return false;
    }
    if (offset + length > bytes.size()) {
        return false;
    }
    return true;
}
CL_NS_END
