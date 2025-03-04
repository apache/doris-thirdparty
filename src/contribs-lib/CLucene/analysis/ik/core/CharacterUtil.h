#ifndef CLUCENE_CHARACTERUTIL_H
#define CLUCENE_CHARACTERUTIL_H

#include <functional>
#include <memory>
#include <vector>

#include "CLucene/_ApiHeader.h"
#include "CLucene/analysis/jieba/Unicode.hpp"

CL_NS_DEF2(analysis, ik)

class CLUCENE_EXPORT CharacterUtil {
public:
    static constexpr int32_t CHAR_USELESS = 0;            // Useless character
    static constexpr int32_t CHAR_ARABIC = 0x00000001;    // Arabic number
    static constexpr int32_t CHAR_ENGLISH = 0x00000002;   // English letter
    static constexpr int32_t CHAR_CHINESE = 0x00000004;   // Chinese character
    static constexpr int32_t CHAR_OTHER_CJK = 0x00000008; // Other CJK character
    static constexpr int32_t CHAR_SURROGATE = 0x00000016; // Surrogate pair character

    using RuneStrLite = cppjieba::RuneStrLite;
    using RuneStr = cppjieba::RuneStr;
    using RuneStrArray = cppjieba::RuneStrArray;

    class CLUCENE_EXPORT TypedRune : public RuneStr {
    public:
        int32_t char_type;
        TypedRune() : RuneStr(), char_type(0) {}
        TypedRune(int32_t rune, size_t offset, size_t len, size_t unicode_offset,
                  size_t unicode_length)
                : RuneStr(rune, offset, len, unicode_offset, unicode_length),
                  char_type(CharacterUtil::identifyCharType(rune)) {}

        void init(const RuneStr& runeStr) {
            rune = runeStr.rune;
            offset = runeStr.offset;
            len = runeStr.len;
            unicode_offset = runeStr.unicode_offset;
            unicode_length = runeStr.unicode_length;
            char_type = CharacterUtil::identifyCharType(rune);
        }

        size_t getByteLength() const { return len; }
        int32_t getChar() const { return rune; }
        size_t getBytePosition() const { return offset; }

        size_t getNextBytePosition() const { return offset + len; }

        void regularize(bool use_lowercase = true);
    };

    using TypedRuneArray = std::vector<TypedRune>;

    static int32_t identifyCharType(int32_t rune);

    static void decodeStringToRunes(const char* str, size_t length, TypedRuneArray& typed_runes,
                                    bool use_lowercase);

    static int32_t regularize(int32_t rune, bool use_lowercase);

    static RuneStrLite decodeChar(const char* str, size_t length);
    static bool decodeString(const char* str, size_t length, RuneStrArray& runes);

    static void regularizeCharInfo(TypedRune& type_rune, bool use_lowercase);

    static size_t adjustToCompleteChar(const char* buffer, size_t buffer_length);
};

CL_NS_END2
#endif //CLUCENE_CHARACTERUTIL_H
