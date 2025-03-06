#ifndef CPPJIEBA_FULLSEGMENT_H
#define CPPJIEBA_FULLSEGMENT_H

#include <algorithm>
#include <set>
#include <cassert>
#include "Logging.hpp"
#include "DictTrie.hpp"
#include "SegmentBase.hpp"
#include "Unicode.hpp"

namespace cppjieba {
class FullSegment: public SegmentBase {
 public:
  FullSegment(const string& dictPath) {
    dictTrie_ = new DictTrie(dictPath);
    isNeedDestroy_ = true;
  }
  FullSegment(const DictTrie* dictTrie, const string& stopWordPath = "")
    : dictTrie_(dictTrie), isNeedDestroy_(false) {
    assert(dictTrie_);
    LoadStopWordDict(stopWordPath);
  }
  ~FullSegment() {
    if (isNeedDestroy_) {
      delete dictTrie_;
    }
  }

  void Cut(const std::string& sentence, vector<std::string_view>& words) const {
    PreFilter pre_filter(symbols_, sentence, true);
    PreFilter::Range range;
    vector<WordRange> wrs;
    wrs.reserve(sentence.size()/2);
    while (pre_filter.HasNext()) {
      range = pre_filter.Next();
      if (range.type == PreFilter::Language::CHINESE) {
        Cut(range.begin, range.end, wrs);
      } else {
        CutAlnum(range.begin, range.end, wrs);
      }
    }
    words.clear();
    words.reserve(wrs.size());
    for (auto& wr : wrs) {
      uint32_t len = wr.right->offset - wr.left->offset + wr.right->len;
      std::string_view word(sentence.data() + wr.left->offset, len);
      if (stopWords_.count(word)) {
        continue;
      }
      words.emplace_back(word);
    }
  }

  void Cut(const string& sentence, 
        vector<string>& words) const {
    vector<Word> tmp;
    Cut(sentence, tmp);
    GetStringsFromWords(tmp, words, [this](const std::string& word) {
      return stopWords_.count(word);
    });
  }
  void Cut(const string& sentence, 
        vector<Word>& words) const {
    PreFilter pre_filter(symbols_, sentence, true);
    PreFilter::Range range;
    vector<WordRange> wrs;
    wrs.reserve(sentence.size()/2);
    while (pre_filter.HasNext()) {
      range = pre_filter.Next();
      if (range.type == PreFilter::Language::CHINESE) {
        Cut(range.begin, range.end, wrs);
      } else {

        CutAlnum(range.begin, range.end, wrs);
      }
    }
    words.clear();
    words.reserve(wrs.size());
    GetWordsFromWordRanges(sentence, wrs, words);
  }
  void Cut(RuneStrArray::const_iterator begin, 
        RuneStrArray::const_iterator end, 
        vector<WordRange>& res) const {
    // result of searching in trie tree
    LocalVector<pair<size_t, const DictUnit*> > tRes;

    // max index of res's words
    size_t maxIdx = 0;

    // always equals to (uItr - begin)
    size_t uIdx = 0;

    // tmp variables
    size_t wordLen = 0;
    assert(dictTrie_);
    vector<struct Dag> dags;
    dictTrie_->Find(begin, end, dags);
    for (size_t i = 0; i < dags.size(); i++) {
      for (size_t j = 0; j < dags[i].nexts.size(); j++) {
        size_t nextoffset = dags[i].nexts[j].first;
        assert(nextoffset < dags.size());
        const DictUnit* du = dags[i].nexts[j].second;
        if (du == NULL) {
          if (dags[i].nexts.size() == 1 && maxIdx <= uIdx) {
            WordRange wr(begin + i, begin + nextoffset);
            res.push_back(wr);
          }
        } else {
          wordLen = du->word.size();
          if (wordLen >= 2 || (dags[i].nexts.size() == 1 && maxIdx <= uIdx)) {
            WordRange wr(begin + i, begin + nextoffset);
            res.push_back(wr);
          }
        }
        maxIdx = uIdx + wordLen > maxIdx ? uIdx + wordLen : maxIdx;
      }
      uIdx++;
    }
  }

  void CutAlnum(RuneStrArray::const_iterator begin,
                RuneStrArray::const_iterator end,
                vector<WordRange>& res) const {
    WordRange wr(begin, end - 1);
    res.push_back(wr);

    auto cursor = begin;
    while (cursor != end) {
      if (PreFilter::IsNumber(cursor->rune)) {
        FindRange(PreFilter::IsNumber, cursor, begin, end, res);
        continue;
      }
      if (PreFilter::IsLetter(cursor->rune)) {
        FindRange(PreFilter::IsLetter, cursor, begin, end, res);
        continue;
      }
      cursor++;
    }
  }

  template <typename Predicate>
  void FindRange(Predicate pred, RuneStrArray::const_iterator& cursor,
                 RuneStrArray::const_iterator begin,
                 RuneStrArray::const_iterator end,
                 vector<WordRange>& res) const {
    auto wrBegin = cursor;
    while (cursor != end && pred(cursor->rune)) {
      cursor++;
    }
    auto wrEnd = cursor;
    if (wrBegin == begin && wrEnd == end) {
      return;
    }
    if (wrEnd - wrBegin <= 1) {
      return;
    }
    WordRange wr(wrBegin, wrEnd - 1);
    res.push_back(wr);
  }

  void LoadStopWordDict(const string& filePath) {
    ifstream ifs(filePath.c_str());
    stopWordList_.reserve(1000);
    if (ifs.is_open()) {
      string line;
        while (getline(ifs, line)) {
        stopWordList_.push_back(line);
      }
      for (auto& word : stopWordList_) {
        stopWords_.insert(std::string_view(word.data(), word.size()));
      }
    }
  }

 private:
  const DictTrie* dictTrie_;
  bool isNeedDestroy_;

  std::vector<std::string> stopWordList_;
  unordered_set<std::string_view> stopWords_;

};
}

#endif
