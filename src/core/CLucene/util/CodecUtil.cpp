#include "CodecUtil.h"
#include "BytesRef.h"
#include "CLucene/debug/error.h"

CL_NS_DEF(util)

CodecUtil::CodecUtil() = default;// no instance

void CodecUtil::writeHeader(store::IndexOutput *out, const std::wstring &codec,
                            int version) {
    std::vector<uint8_t> codecVector(codec.begin(), codec.end());
    std::shared_ptr<BytesRef> bytes = std::make_shared<BytesRef>(codecVector);
    if (bytes->length != codec.size() || bytes->length >= 128) {
        _CLTHROWA(CL_ERR_IllegalArgument, "codec must be simple ASCII, less than 128 characters in length");
    }
    out->writeInt(CODEC_MAGIC);
    out->writeString(codec);
    out->writeInt(version);
}

int CodecUtil::checkHeader(store::IndexInput *in_,
                           const std::wstring &codec,
                           int minVersion, int maxVersion) {
    // Safety to guard against reading a bogus string:
    int actualHeader = in_->readInt();
    if (actualHeader != CODEC_MAGIC) {
        _CLTHROWA(CL_ERR_CorruptIndex, "codec header mismatch");
    }
    return checkHeaderNoMagic(in_, codec, minVersion, maxVersion);
}

int CodecUtil::checkHeaderNoMagic(store::IndexInput *in_,
                                  const std::wstring &codec, int minVersion,
                                  int maxVersion) {
    auto name = in_->readString();
    const std::wstring actualCodec(name);
    _CLDELETE_CARRAY(name);
    if (actualCodec != codec) {
        _CLTHROWA(CL_ERR_CorruptIndex, "codec mismatch");
    }

    int actualVersion = in_->readInt();
    if (actualVersion < minVersion) {
        _CLTHROWA(CL_ERR_NumberFormat, "codec version too old");
    }
    if (actualVersion > maxVersion) {
        _CLTHROWA(CL_ERR_NumberFormat, "codec version too new");
    }

    return actualVersion;
}

CL_NS_END