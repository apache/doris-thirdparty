#ifndef _lucene_store_ByteArrayDataInput_
#define _lucene_store_ByteArrayDataInput_

#include "CLucene/SharedHeader.h"

#include <cstdint>
#include <memory>
#include <vector>

CL_NS_DEF(store)
/**
 * DataInput backed by a byte array.
 * <b>WARNING:</b> This class omits all low-level checks.
 * @lucene.experimental
 */
class ByteArrayDataInput {

private:
    std::vector<uint8_t> bytes;

    int pos = 0;
    int limit = 0;

public:
    explicit ByteArrayDataInput(std::vector<uint8_t> &bytes);

    ByteArrayDataInput(std::vector<uint8_t> &bytes, int offset, int len);

    ByteArrayDataInput();

    void reset(std::vector<uint8_t> &bytes);


    // NOTE: sets pos to 0, which is not right if you had
    // called reset w/ non-zero offset!!
    void rewind();

    int getPosition() const;

    void setPosition(int pos);

    void reset(std::vector<uint8_t> &bytes, int offset, int len);

    int length() const;

    bool eof() const;

    void skipBytes(int64_t count);

    short readShort();

    int readInt();

    int64_t readLong();

    int readVInt();

    int64_t readVLong();

    // NOTE: AIOOBE not EOF if you read too much
    uint8_t readByte();

    // NOTE: AIOOBE not EOF if you read too much
    void readBytes(std::vector<uint8_t> &b, int len, int offset);
};

CL_NS_END
#endif