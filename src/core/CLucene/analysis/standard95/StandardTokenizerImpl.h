#pragma once

#include <string.h>

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include "CLucene/SharedHeader.h"
#include "CLucene/util/CLStreams.h"

namespace lucene::analysis::standard95 {

class StandardTokenizerImpl {
 public:
  StandardTokenizerImpl(lucene::util::Reader* reader);

  int32_t getNextToken();
  std::string_view getText();
  bool zzRefill();
  void yyclose();
  void yyreset(lucene::util::Reader* reader);
  inline int32_t yylength() { return zzMarkedPos - zzStartRead; }

 private:
  static inline int32_t zzCMap(int32_t input) {
    int32_t offset = input & 255;
    return offset == input ? ZZ_CMAP_BLOCKS[offset]
                           : ZZ_CMAP_BLOCKS[ZZ_CMAP_TOP[input >> 8] | offset];
  }

  static void zzScanError(int32_t errorCode);

  void yyResetPosition();

 public:
  static constexpr int32_t YYEOF = -1;

 private:
  int32_t ZZ_BUFFERSIZE = 255;

 public:
  static constexpr int32_t YYINITIAL = 0;

 private:
  static const std::vector<int32_t> ZZ_LEXSTATE;
  static const std::vector<int32_t> ZZ_CMAP_TOP;

 private:
  static const std::vector<int32_t> ZZ_CMAP_BLOCKS;
  static const std::vector<int32_t> ZZ_ACTION;
  static const std::vector<int32_t> ZZ_ROWMAP;
  static const std::vector<int32_t> ZZ_TRANS;
  static const std::vector<int32_t> ZZ_ATTRIBUTE;
  static const std::vector<std::string> ZZ_ERROR_MSG;

  static constexpr int32_t ZZ_UNKNOWN_ERROR = 0;
  static constexpr int32_t ZZ_NO_MATCH = 1;
  static constexpr int32_t ZZ_PUSHBACK_2BIG = 2;

 public:
  static constexpr int32_t WORD_TYPE = 0;
  static constexpr int32_t NUMERIC_TYPE = 1;
  static constexpr int32_t SOUTH_EAST_ASIAN_TYPE = 2;
  static constexpr int32_t IDEOGRAPHIC_TYPE = 3;
  static constexpr int32_t HIRAGANA_TYPE = 4;
  static constexpr int32_t KATAKANA_TYPE = 5;
  static constexpr int32_t HANGUL_TYPE = 6;
  static constexpr int32_t EMOJI_TYPE = 7;

 private:
  lucene::util::Reader* zzReader = nullptr;
  std::vector<char> zzBuffer;

  int32_t zzState = 0;
  int32_t zzLexicalState = YYINITIAL;
  int32_t zzMarkedPos = 0;
  int32_t zzCurrentPos = 0;
  int32_t zzStartRead = 0;
  int32_t zzEndRead = 0;
  bool zzAtEOF = false;
  int32_t zzFinalHighSurrogate = 0;
  int32_t yyline = 0;
  int32_t yycolumn = 0;
  bool zzAtBOL = true;
};

}