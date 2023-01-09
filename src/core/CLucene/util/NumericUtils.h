#pragma once

#include "CLucene/StdHeader.h"

#include <memory>
#include <stdexcept>
#include <vector>

CL_NS_DEF(util)
class NumericUtils final : public std::enable_shared_from_this<NumericUtils> {
private:
    NumericUtils();

    /**
         * Converts a <code>double</code> value to a sortable signed
         * <code>long</code>. The value is converted by getting their IEEE 754
         * floating-point &quot;double format&quot; bit layout and then some bits are
         * swapped, to be able to compare the result as long. By this the precision is
         * not reduced, but the value can easily used as a long. The sort order
         * (including {@link Double#NaN}) is defined by
         * {@link Double#compareTo}; {@code NaN} is greater than positive infinity.
         * @see #sortableLongToDouble
         */
public:
    static void intToSortableBytes(int value, std::vector<uint8_t> &result,
                                   int offset);
    static void int16ToSortableBytes(int value, std::vector<uint8_t> &result,
                         int offset);
    /**
         * Decodes an integer value previously written with {@link
         * #intToSortableBytes}
         * @see #intToSortableBytes(int, byte[], int)
         */
    static int sortableBytesToInt(std::vector<uint8_t> &encoded, int offset);

    /**
         * Encodes an long {@code value} such that unsigned byte order comparison
         * is consistent with {@link Long#compare(long, long)}
         * @see #sortableBytesToLong(byte[], int)
         */
    static void longToSortableBytes(int64_t value, std::vector<uint8_t> &result,
                                    int offset);

    /**
         * Decodes a long value previously written with {@link #longToSortableBytes}
         * @see #longToSortableBytes(long, byte[], int)
         */
    static int64_t sortableBytesToLong(std::vector<uint8_t> &encoded, int offset);

    static void subtract(int bytesPerDim, int dim, std::vector<uint8_t> &a,
                         std::vector<uint8_t> &b, std::vector<uint8_t> &result);
    static int floatToInt(float value);
    static int64_t doubleToLong(double value);
    static void floatToSortableBytes(float value, std::vector<uint8_t> &result, int offset);
    static void doubleToSortableBytes(double value, std::vector<uint8_t> &result, int offset);
    static int64_t sortableDoubleBits(int64_t bits);
    static int sortableFloatBits(int bits);
};

CL_NS_END
