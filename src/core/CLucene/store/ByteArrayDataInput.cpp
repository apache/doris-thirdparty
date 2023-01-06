#include "ByteArrayDataInput.h"
#include "CLucene/debug/error.h"
#include "CLucene/util/BytesRef.h"

CL_NS_DEF(store)

using BytesRef = util::BytesRef;

ByteArrayDataInput::ByteArrayDataInput(std::vector<uint8_t> &b) {
    reset(b);
}

ByteArrayDataInput::ByteArrayDataInput(std::vector<uint8_t> &bytes, int offset,
                                       int len) {
    reset(bytes, offset, len);
}

ByteArrayDataInput::ByteArrayDataInput() { reset(BytesRef::EMPTY_BYTES); }

void ByteArrayDataInput::reset(std::vector<uint8_t>& b) {
    reset(b, 0, b.size());
}

void ByteArrayDataInput::rewind() { pos = 0; }

int ByteArrayDataInput::getPosition() { return pos; }

void ByteArrayDataInput::setPosition(int p) { pos = p; }

void ByteArrayDataInput::reset(std::vector<uint8_t> &b, int offset, int len) {
    bytes = b;
    pos = offset;
    limit = offset + len;
}

int ByteArrayDataInput::length() { return limit; }

bool ByteArrayDataInput::eof() { return pos == limit; }

void ByteArrayDataInput::skipBytes(int64_t count) { pos += count; }

short ByteArrayDataInput::readShort() {
    return static_cast<short>(((bytes.at(pos++) & 0xFF) << 8) |
                              (bytes.at(pos++) & 0xFF));
}

int ByteArrayDataInput::readInt() {
    return ((bytes.at(pos++) & 0xFF) << 24) | ((bytes.at(pos++) & 0xFF) << 16) |
           ((bytes.at(pos++) & 0xFF) << 8) | (bytes.at(pos++) & 0xFF);
}

int64_t ByteArrayDataInput::readLong() {
    int i1 = ((bytes.at(pos++) & 0xff) << 24) |
             ((bytes.at(pos++) & 0xff) << 16) |
             ((bytes.at(pos++) & 0xff) << 8) | (bytes.at(pos++) & 0xff);
    int i2 = ((bytes.at(pos++) & 0xff) << 24) |
             ((bytes.at(pos++) & 0xff) << 16) |
             ((bytes.at(pos++) & 0xff) << 8) | (bytes.at(pos++) & 0xff);
    return ((static_cast<int64_t>(i1)) << 32) | (i2 & 0xFFFFFFFFLL);
}

int ByteArrayDataInput::readVInt() {
    uint8_t b = bytes.at(pos++);
    if (b >= 0) {
        return b;
    }
    int i = b & 0x7F;
    b = bytes.at(pos++);
    i |= (b & 0x7F) << 7;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7F) << 14;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7F) << 21;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
    i |= (b & 0x0F) << 28;
    if ((b & 0xF0) == 0) {
        return i;
    }
    _CLTHROWA(CL_ERR_Runtime, "Invalid vInt detected (too many bits)");
}

int64_t ByteArrayDataInput::readVLong() {
    uint8_t b = bytes.at(pos++);
    if (b >= 0) {
        return b;
    }
    int64_t i = b & 0x7FLL;
    b = bytes.at(pos++);
    i |= (b & 0x7FLL) << 7;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7FLL) << 14;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7FLL) << 21;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7FLL) << 28;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7FLL) << 35;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7FLL) << 42;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7FLL) << 49;
    if (b >= 0) {
        return i;
    }
    b = bytes.at(pos++);
    i |= (b & 0x7FLL) << 56;
    if (b >= 0) {
        return i;
    }
    _CLTHROWA(CL_ERR_Runtime, "Invalid vLong detected (negative values disallowed)");
}

uint8_t ByteArrayDataInput::readByte() { return bytes.at(pos++); }

void ByteArrayDataInput::readBytes(std::vector<uint8_t> &b, int len, int offset) {
    std::copy(bytes.begin() + pos, bytes.begin() + pos + len, b.begin() + offset);
    pos += len;
}

CL_NS_END