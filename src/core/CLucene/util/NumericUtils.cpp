#include "NumericUtils.h"
#include "CLucene/debug/error.h"

CL_NS_DEF(util)


NumericUtils::NumericUtils() = default;// no instance!

int NumericUtils::floatToInt(float value) {
    union UnionFloatInt {
        int int_;
        float float_;
    };
    UnionFloatInt float_int{};
    float_int.float_ = value;
    return float_int.int_;
}

int64_t NumericUtils::doubleToLong(double value) {
    union UnionLongDouble {
        int64_t long_;
        double double_;
    };
    UnionLongDouble long_double{};
    long_double.double_ = value;
    return long_double.long_;
}

int64_t NumericUtils::sortableDoubleBits(int64_t bits)
{
    return bits ^ (bits >> 63) & 0x7fffffffffffffffLL;
}

int NumericUtils::sortableFloatBits(int bits)
{
    return bits ^ (bits >> 31) & 0x7fffffff;
}

void NumericUtils::doubleToSortableBytes(double value, std::vector<uint8_t> &result, int offset) {
    auto v = sortableDoubleBits(doubleToLong(value));
    longToSortableBytes(v, result, offset);
}


void NumericUtils::floatToSortableBytes(float value, std::vector<uint8_t> &result, int offset) {
    auto v = sortableFloatBits(floatToInt(value));
    intToSortableBytes(v, result, offset);
}

void NumericUtils::int16ToSortableBytes(int value, std::vector<uint8_t> &result,
                                      int offset) {
    // Flip the sign bit, so negative ints sort before positive ints correctly:
    value ^= 0x8000;
    result[offset] = static_cast<char>(value >> 8);
    result[offset + 1] = static_cast<char>(value);
}

void NumericUtils::intToSortableBytes(int value, std::vector<uint8_t> &result,
                                      int offset) {
    // Flip the sign bit, so negative ints sort before positive ints correctly:
    value ^= 0x80000000;
    result[offset] = static_cast<char>(value >> 24);
    result[offset + 1] = static_cast<char>(value >> 16);
    result[offset + 2] = static_cast<char>(value >> 8);
    result[offset + 3] = static_cast<char>(value);
}

int NumericUtils::sortableBytesToInt(std::vector<uint8_t> &encoded, int offset) {
    int x = ((encoded[offset] & 0xFF) << 24) |
            ((encoded[offset + 1] & 0xFF) << 16) |
            ((encoded[offset + 2] & 0xFF) << 8) | (encoded[offset + 3] & 0xFF);
    // Re-flip the sign bit to restore the original value:
    return x ^ 0x80000000;
}

void NumericUtils::longToSortableBytes(int64_t value,
                                       std::vector<uint8_t> &result, int offset) {
    // Flip the sign bit so negative longs sort before positive longs:
    value ^= 0x8000000000000000LL;
    result[offset] = static_cast<char>(value >> 56);
    result[offset + 1] = static_cast<char>(value >> 48);
    result[offset + 2] = static_cast<char>(value >> 40);
    result[offset + 3] = static_cast<char>(value >> 32);
    result[offset + 4] = static_cast<char>(value >> 24);
    result[offset + 5] = static_cast<char>(value >> 16);
    result[offset + 6] = static_cast<char>(value >> 8);
    result[offset + 7] = static_cast<char>(value);
}

int64_t NumericUtils::sortableBytesToLong(std::vector<uint8_t> &encoded,
                                          int offset) {
    int64_t v = ((encoded[offset] & 0xFFLL) << 56) |
                ((encoded[offset + 1] & 0xFFLL) << 48) |
                ((encoded[offset + 2] & 0xFFLL) << 40) |
                ((encoded[offset + 3] & 0xFFLL) << 32) |
                ((encoded[offset + 4] & 0xFFLL) << 24) |
                ((encoded[offset + 5] & 0xFFLL) << 16) |
                ((encoded[offset + 6] & 0xFFLL) << 8) |
                (encoded[offset + 7] & 0xFFLL);
    // Flip the sign bit back
    v ^= 0x8000000000000000LL;
    return v;
}

void NumericUtils::subtract(int bytesPerDim, int dim, std::vector<uint8_t> &a,
                            std::vector<uint8_t> &b, std::vector<uint8_t> &result) {
    int start = dim * bytesPerDim;
    int end = start + bytesPerDim;
    int borrow = 0;
    for (int i = end - 1; i >= start; i--) {
        int diff = (a[i] & 0xff) - (b[i] & 0xff) - borrow;
        if (diff < 0) {
            diff += 256;
            borrow = 1;
        } else {
            borrow = 0;
        }
        result[i - start] = static_cast<char>(diff);
    }
    if (borrow != 0) {
        _CLTHROWA(CL_ERR_IllegalArgument, "a < b");
    }
}
CL_NS_END