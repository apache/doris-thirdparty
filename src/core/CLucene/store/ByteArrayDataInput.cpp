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

int ByteArrayDataInput::getPosition() const { return pos; }

void ByteArrayDataInput::setPosition(int p) { pos = p; }

void ByteArrayDataInput::reset(std::vector<uint8_t> &b, int offset, int len) {
    bytes = b;
    pos = offset;
    limit = offset + len;
}

int ByteArrayDataInput::length() const { return limit; }

bool ByteArrayDataInput::eof() const { return pos == limit; }

void ByteArrayDataInput::skipBytes(int64_t count) { pos += count; }

short ByteArrayDataInput::readShort() {
    return static_cast<short>(((readByte() & 0xFF) << 8) | (readByte() & 0xFF));
}

int ByteArrayDataInput::readInt() {
    int32_t b = (readByte() << 24);
    b |= (readByte() << 16);
    b |= (readByte() <<  8);
    return (b | readByte());
}

int64_t ByteArrayDataInput::readLong() {
    int64_t i = ((int64_t)readInt() << 32);
    return (i | ((int64_t)readInt() & 0xFFFFFFFFL));
}

int ByteArrayDataInput::readVInt() {
    uint8_t b = readByte();
    int32_t i = b & 0x7F;
    for (int32_t shift = 7; (b & 0x80) != 0; shift += 7) {
        b = readByte();
        i |= (b & 0x7F) << shift;
    }
    return i;
}

int64_t ByteArrayDataInput::readVLong() {
        uint8_t b = readByte();
        int64_t i = b & 0x7F;
        for (int32_t shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (((int64_t)b) & 0x7FL) << shift;
        }
        return i;
}

uint8_t ByteArrayDataInput::readByte() { return bytes.at(pos++); }

void ByteArrayDataInput::readBytes(std::vector<uint8_t> &b, int len, int offset) {
    std::copy(bytes.begin() + pos, bytes.begin() + pos + len, b.begin() + offset);
    pos += len;
}

CL_NS_END