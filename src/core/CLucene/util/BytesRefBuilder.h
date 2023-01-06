#pragma once
#include "CLucene/SharedHeader.h"
#include "CLucene/util/BytesRef.h"

#include <vector>

CL_NS_DEF(util)

/**
 * A builder for {@link BytesRef} instances.
 * @lucene.internal
 */
class BytesRefBuilder : public std::enable_shared_from_this<BytesRefBuilder>
{

private:
  const std::shared_ptr<BytesRef> ref_;

  /** Sole constructor. */
public:
  BytesRefBuilder();

  /** Return a reference to the bytes of this builder. */
  virtual std::vector<uint8_t> bytes();

  /** Return the number of bytes in this buffer. */
  virtual int length();

  /** Set the length. */
  virtual void setLength(int length);

  /** Return the byte at the given offset. */
  virtual uint8_t byteAt(int offset);

  /** Set a byte. */
  virtual void setByteAt(int offset, uint8_t b);

  /**
   * Ensure that this builder can hold at least <code>capacity</code> bytes
   * without resizing.
   */
  virtual void grow(int capacity);

  /**
   * Append a single byte to this builder.
   */
  virtual void append(uint8_t b);

  /**
   * Append the provided bytes to this builder.
   */
  virtual void append(std::vector<uint8_t> &b, int off, int len);

  /**
   * Append the provided bytes to this builder.
   */
  virtual void append(const std::shared_ptr<BytesRef>& ref);

  /**
   * Append the provided bytes to this builder.
   */
  virtual void append(const std::shared_ptr<BytesRefBuilder>& builder);

  /**
   * Reset this builder to the empty state.
   */
  virtual void clear();

  /**
   * Return a {@link BytesRef} that points to the internal content of this
   * builder. Any update to the content of this builder might invalidate
   * the provided <code>ref</code> and vice-versa.
   */
  virtual std::shared_ptr<BytesRef> get();

};

CL_NS_END