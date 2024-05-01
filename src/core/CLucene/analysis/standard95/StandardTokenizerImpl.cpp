#include "StandardTokenizerImpl.h"

#include "CLucene/debug/error.h"
#include "CLucene/util/stringUtil.h"
#include "StandardTokenizerImpl-inl.h"

namespace lucene::analysis::standard95 {

static inline uint32_t decodeUtf8ToCodepoint(const std::string_view& utf8,
                                             size_t& bytesRead) {
  if (utf8.empty()) {
    return -1;
  }

  uint8_t firstByte = static_cast<uint8_t>(utf8[0]);
  bytesRead = 0;

  if ((firstByte & 0x80) == 0x00)
    bytesRead = 1;
  else if ((firstByte & 0xE0) == 0xC0)
    bytesRead = 2;
  else if ((firstByte & 0xF0) == 0xE0)
    bytesRead = 3;
  else if ((firstByte & 0xF8) == 0xF0)
    bytesRead = 4;
  else
    return -1;

  if (utf8.size() < bytesRead) {
    return -1;
  }

  uint32_t codepoint = 0;
  switch (bytesRead) {
    case 1:
      codepoint = (utf8[0] & 0x7F);
      break;
    case 2:
      codepoint = ((utf8[0] & 0x1F) << 6) | ((utf8[1] & 0x3F));
      break;
    case 3:
      codepoint = ((utf8[0] & 0x0F) << 12) | ((utf8[1] & 0x3F) << 6) |
                  ((utf8[2] & 0x3F));
      break;
    case 4:
      codepoint = ((utf8[0] & 0x07) << 18) | ((utf8[1] & 0x3F) << 12) |
                  ((utf8[2] & 0x3F) << 6) | ((utf8[3] & 0x3F));
      break;
    default:
      return -1;
  }

  return codepoint;
}

const std::vector<std::string> StandardTokenizerImpl::ZZ_ERROR_MSG = {
    "Unknown internal scanner error", "Error: could not match input",
    "Error: pushback value was too large"};

StandardTokenizerImpl::StandardTokenizerImpl(lucene::util::Reader* reader)
    : zzReader(reader), zzBuffer((reader == nullptr) ? 0 : reader->size()) {}

std::string_view StandardTokenizerImpl::getText() {
  return std::string_view(zzBuffer.data() + zzStartRead,
                          zzMarkedPos - zzStartRead);
}

bool StandardTokenizerImpl::zzRefill() {
  if (zzStartRead > 0) {
      return true;
  }

  int32_t numRead = zzReader->readCopy(zzBuffer.data(), 0, zzBuffer.size());
  if (numRead > 0) {
      assert(zzBuffer.size() == numRead);
      zzEndRead += numRead;

      int32_t n = StringUtil::validate_utf8(std::string_view(zzBuffer.data(), zzBuffer.size()));
      if (n != 0) {
          return true;
      }

      return false;
  }

  return true;
}

void StandardTokenizerImpl::yyclose() {
  zzAtEOF = true;
  zzEndRead = zzStartRead;
}

void StandardTokenizerImpl::yyreset(lucene::util::Reader* reader) {
  zzReader = reader;
  zzBuffer.resize(reader->size());
  yyResetPosition();
  zzLexicalState = YYINITIAL;
}

void StandardTokenizerImpl::yyResetPosition() {
  zzAtBOL = true;
  zzAtEOF = false;
  zzCurrentPos = 0;
  zzMarkedPos = 0;
  zzStartRead = 0;
  zzEndRead = 0;
  zzFinalHighSurrogate = 0;
  yyline = 0;
  yycolumn = 0;
}

void StandardTokenizerImpl::zzScanError(int32_t errorCode) {
  std::string message;
  try {
    message = ZZ_ERROR_MSG[errorCode];
  } catch (const std::out_of_range& e) {
    message = ZZ_ERROR_MSG[ZZ_UNKNOWN_ERROR];
  }
  _CLTHROWA(CL_ERR_Runtime, message.c_str());
}

int32_t StandardTokenizerImpl::getNextToken() {
  int32_t zzInput = 0;
  int32_t zzAction = 0;

  int32_t zzCurrentPosL = 0;
  int32_t zzMarkedPosL = 0;
  int32_t zzEndReadL = zzEndRead;
  std::vector<char>& zzBufferL = zzBuffer;

  const std::vector<int32_t>& zzTransL = ZZ_TRANS;
  const std::vector<int32_t>& zzRowMapL = ZZ_ROWMAP;
  const std::vector<int32_t>& zzAttrL = ZZ_ATTRIBUTE;

  while (true) {
    zzMarkedPosL = zzMarkedPos;

    zzAction = -1;

    zzCurrentPosL = zzCurrentPos = zzStartRead = zzMarkedPosL;

    zzState = ZZ_LEXSTATE[zzLexicalState];

    int32_t zzAttributes = zzAttrL[zzState];
    if ((zzAttributes & 1) == 1) {
      zzAction = zzState;
    }

    {
      while (true) {
        if (zzCurrentPosL < zzEndReadL && (zzCurrentPosL - zzStartRead) < ZZ_BUFFERSIZE) {
          size_t len = 0;
          zzInput = decodeUtf8ToCodepoint(
              std::string_view(zzBufferL.data() + zzCurrentPosL, zzEndReadL),
              len);
          if (zzInput == -1) {
            return YYEOF;
          }
          zzCurrentPosL += len;
        } else if (zzAtEOF) {
          zzInput = YYEOF;
          goto zzForActionBreak;
        } else {
          zzCurrentPos = zzCurrentPosL;
          zzMarkedPos = zzMarkedPosL;
          bool eof = zzRefill();
          zzCurrentPosL = zzCurrentPos;
          zzMarkedPosL = zzMarkedPos;
          zzBufferL = zzBuffer;
          zzEndReadL = zzEndRead;
          if (eof) {
            zzInput = YYEOF;
            goto zzForActionBreak;
          } else {
            size_t len = 0;
            zzInput = decodeUtf8ToCodepoint(
                std::string_view(zzBufferL.data() + zzCurrentPosL, zzEndReadL),
                len);
            if (zzInput == -1) {
              return YYEOF;
            }
            zzCurrentPosL += len;
          }
        }
        int32_t zzNext = zzTransL[zzRowMapL[zzState] + zzCMap(zzInput)];
        if (zzNext == -1) {
          goto zzForActionBreak;
        }
        zzState = zzNext;

        zzAttributes = zzAttrL[zzState];
        if ((zzAttributes & 1) == 1) {
          zzAction = zzState;
          zzMarkedPosL = zzCurrentPosL;
          if ((zzAttributes & 8) == 8) {
            goto zzForActionBreak;
          }
        }
      }
    }
  zzForActionBreak:

    zzMarkedPos = zzMarkedPosL;

    if (zzInput == YYEOF && zzStartRead == zzCurrentPos) {
      zzAtEOF = true;
      { return YYEOF; }
    } else {
      switch (zzAction < 0 ? zzAction : ZZ_ACTION[zzAction]) {
        case 1: {
          break;
        }
          // fall through
        case 10:
          break;
        case 2: {
          return NUMERIC_TYPE;
        }
          // fall through
        case 11:
          break;
        case 3: {
          return WORD_TYPE;
        }
          // fall through
        case 12:
          break;
        case 4: {
          return EMOJI_TYPE;
        }
          // fall through
        case 13:
          break;
        case 5: {
          return SOUTH_EAST_ASIAN_TYPE;
        }
          // fall through
        case 14:
          break;
        case 6: {
          return HANGUL_TYPE;
        }
          // fall through
        case 15:
          break;
        case 7: {
          return IDEOGRAPHIC_TYPE;
        }
          // fall through
        case 16:
          break;
        case 8: {
          return KATAKANA_TYPE;
        }
          // fall through
        case 17:
          break;
        case 9: {
          return HIRAGANA_TYPE;
        }
          // fall through
        case 18:
          break;
        default:
          zzScanError(ZZ_NO_MATCH);
      }
    }
  }
}

}  // namespace lucene::analysis::standard95
