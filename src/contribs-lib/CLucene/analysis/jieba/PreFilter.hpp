#ifndef CPPJIEBA_PRE_FILTER_H
#define CPPJIEBA_PRE_FILTER_H

#include "Trie.hpp"
#include "Logging.hpp"

namespace cppjieba {

class PreFilter {
 public:
  enum class Language {
    CHINESE,
    EN_NUMBER,

    INVALID
  };

  //TODO use WordRange instead of Range
  struct Range {
    Language type = Language::CHINESE;
    RuneStrArray::const_iterator begin;
    RuneStrArray::const_iterator end;
  }; // struct Range

  PreFilter(const unordered_set<Rune>& symbols, const string& sentence)
      : symbols_(symbols) {
    if (!DecodeRunesInString(sentence, sentence_)) {
      XLOG(ERROR) << "decode failed. ";
    }
    cursor_ = sentence_.begin();
  }

  PreFilter(const unordered_set<Rune>& symbols, const string& sentence,
            bool isAlnum)
      : PreFilter(symbols, sentence) {
    isAlnum_ = isAlnum;
  }

  ~PreFilter() {
  }
  bool HasNext() const {
    return cursor_ != sentence_.end();
  }
  Range Next() {
    Range range;
    range.begin = cursor_;
    while (cursor_ != sentence_.end()) {
      if (IsIn(symbols_, cursor_->rune)) {
        if (range.begin == cursor_) {
          cursor_ ++;
        }
        range.end = cursor_;
        return range;
      }
      if (isAlnum_ && IsAlnum(cursor_->rune)) {
        return FindRange(IsAlnum, range);       
      }
      cursor_ ++;
    }
    range.end = sentence_.end();
    return range;
  }

  static inline bool IsAlnum(uint32_t codepoint) {
    return IsNumber(codepoint) || IsLetter(codepoint);
  }

  static inline bool IsNumber(uint32_t codepoint) {
    return (codepoint >= 0x0030 && codepoint <= 0x0039);
  }

  static inline bool IsLetter(uint32_t codepoint) {
    return (codepoint >= 0x0041 && codepoint <= 0x005A) ||
          (codepoint >= 0x0061 && codepoint <= 0x007A);
  }

  template <typename Predicate>
  Range FindRange(Predicate pred, Range range) {
    if (!pred(range.begin->rune)) {
      range.end = cursor_;
      return range;
    }
    while (cursor_ != sentence_.end() && pred(cursor_->rune)) {
      cursor_++;
    }
    range.end = cursor_;
    range.type = Language::EN_NUMBER;
    return range;
  }

 private:
  bool isAlnum_ = false;
  RuneStrArray::const_iterator cursor_;
  RuneStrArray sentence_;
  const unordered_set<Rune>& symbols_;
}; // class PreFilter

} // namespace cppjieba

#endif // CPPJIEBA_PRE_FILTER_H
