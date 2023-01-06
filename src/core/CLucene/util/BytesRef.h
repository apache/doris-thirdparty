#pragma once
#include "CLucene/SharedHeader.h"

#include <any>
#include <memory>
#include <string>
#include <vector>

CL_NS_DEF(util)

/** Represents byte[], as a slice (offset + length) into an
 *  existing byte[].  The {@link #bytes} member should never be null;
 *  use {@link #EMPTY_BYTES} if necessary.
 *
 * <p><b>Important note:</b> Unless otherwise noted, Lucene uses this class to
 * represent terms that are encoded as <b>UTF8</b> bytes in the index. To
 * convert them to a Java {@link std::wstring} (which is UTF16), use {@link
 * #utf8ToString}. Using code like {@code new std::wstring(bytes, offset, length)} to
 * do this is <b>wrong</b>, as it does not respect the correct character set and
 * may return wrong results (depending on the platform's defaults)!
 *
 * <p>{@code BytesRef} implements {@link Comparable}. The underlying byte arrays
 * are sorted lexicographically, numerically treating elements as unsigned.
 * This is identical to Unicode codepoint order.
 */
class BytesRef final : public std::enable_shared_from_this<BytesRef> {
    /** An empty byte array for convenience */
public:
    static std::vector<uint8_t> EMPTY_BYTES;

    /** The contents of the BytesRef. Should never be {@code null}. */
    std::vector<uint8_t> bytes;

    /** Offset of first valid byte. */
    int offset = 0;

    /** Length of used bytes. */
    int length = 0;

    /** Create a BytesRef with {@link #EMPTY_BYTES} */
    BytesRef();

    /** This instance will directly reference bytes w/o making a copy.
   * bytes should not be null.
   */
    BytesRef(std::vector<uint8_t> &bytes, int offset, int length);

    /** This instance will directly reference bytes w/o making a copy.
   * bytes should not be null */
    explicit BytesRef(std::vector<uint8_t> &bytes);

    /**
   * Create a BytesRef pointing to a new array of size <code>capacity</code>.
   * Offset and length will both be zero.
   */
    explicit BytesRef(int capacity);

    /**
   * Expert: compares the bytes against another BytesRef,
   * returning true if the bytes are equal.
   *
   * @param other Another BytesRef, should not be null.
   * @lucene.internal
   */
    bool BytesEquals(BytesRef &other);

    /**
   * Returns a shallow clone of this instance (the underlying bytes are
   * <b>not</b> copied and will be shared by both the returned object and this
   * object.
   *
   * @see #deepCopyOf
   */
    std::shared_ptr<BytesRef> clone();
    static std::shared_ptr<BytesRef> deepCopyOf(const std::shared_ptr<BytesRef>& other);

    /** Unsigned byte order comparison */
    int CompareTo(BytesRef& other);

    /**
   * Creates a new BytesRef that points to a copy of the bytes from
   * <code>other</code>
   * <p>
   * The returned BytesRef will have a length of other.length
   * and an offset of zero.
   */
    //static std::shared_ptr<BytesRef> deepCopyOf(std::shared_ptr<BytesRef> other);

    /**
   * Performs internal consistency checks.
   * Always returns true (or throws IllegalStateException)
   */
    bool isValid();
};
CL_NS_END
