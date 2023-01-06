#pragma once
#include "CLucene/_ApiHeader.h"
#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"

#include <memory>
#include <vector>

CL_NS_DEF(util)

class CodecUtil final : public std::enable_shared_from_this<CodecUtil> {
private:
    CodecUtil();

    /**
   * Constant to identify the start of a codec header.
   */
public:
    static constexpr int CODEC_MAGIC = 0x3fd76c17;
    /**
   * Constant to identify the start of a codec footer.
   */
    static const int FOOTER_MAGIC = ~CODEC_MAGIC;

    static void writeHeader(store::IndexOutput* out,
                            const std::wstring &codec,
                            int version);
    static int checkHeader(store::IndexInput* in_,
                           const std::wstring &codec, int minVersion,
                           int maxVersion);
    static int checkHeaderNoMagic(store::IndexInput* in_,
                                  const std::wstring &codec, int minVersion,
                                  int maxVersion);
};

CL_NS_END