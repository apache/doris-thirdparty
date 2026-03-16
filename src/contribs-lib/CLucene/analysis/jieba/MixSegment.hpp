#ifndef CPPJIEBA_MIXSEGMENT_H
#define CPPJIEBA_MIXSEGMENT_H

#include <cassert>
#include <string_view>
#include "MPSegment.hpp"
#include "HMMSegment.hpp"
#include "StringUtil.hpp"
#include "PosTagger.hpp"

namespace cppjieba {
class MixSegment: public SegmentTagged {
 public:
  MixSegment(const string& mpSegDict, const string& hmmSegDict, 
        const string& userDict = "") 
    : mpSeg_(mpSegDict, userDict), 
      hmmSeg_(hmmSegDict) {
  }
  MixSegment(const DictTrie* dictTrie, const HMMModel* model, const string& stopWordPath = "") 
    : mpSeg_(dictTrie), hmmSeg_(model) {
    LoadStopWordDict(stopWordPath);
  }
  ~MixSegment() {
  }

  void Cut(const string& sentence, vector<std::string_view>& words, bool hmm = true) const {
    PreFilter pre_filter(symbols_, sentence);
    PreFilter::Range range;
    vector<WordRange> wrs;
    wrs.reserve(sentence.size() / 2);
    while (pre_filter.HasNext()) {
      range = pre_filter.Next();
      Cut(range.begin, range.end, wrs, hmm);
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

  void Cut(const string& sentence, vector<string>& words) const {
    Cut(sentence, words, true);
  }
  void Cut(const string& sentence, vector<string>& words, bool hmm) const {
    vector<Word> tmp;
    Cut(sentence, tmp, hmm);
    GetStringsFromWords(tmp, words, [this](const std::string& word) {
      return stopWords_.count(word);
    });
  }
  void Cut(const string& sentence, vector<Word>& words, bool hmm = true) const {
    PreFilter pre_filter(symbols_, sentence);
    PreFilter::Range range;
    vector<WordRange> wrs;
    wrs.reserve(sentence.size() / 2);
    while (pre_filter.HasNext()) {
      range = pre_filter.Next();
      Cut(range.begin, range.end, wrs, hmm);
    }
    words.clear();
    words.reserve(wrs.size());
    GetWordsFromWordRanges(sentence, wrs, words);
  }

  void Cut(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end, vector<WordRange>& res, bool hmm) const {
    if (!hmm) {
      mpSeg_.Cut(begin, end, res);
      return;
    }
    vector<WordRange> words;
    assert(end >= begin);
    words.reserve(end - begin);
    mpSeg_.Cut(begin, end, words);

    vector<WordRange> hmmRes;
    hmmRes.reserve(end - begin);
    for (size_t i = 0; i < words.size(); i++) {
      //if mp Get a word, it's ok, put it into result
      if (words[i].left != words[i].right || (words[i].left == words[i].right && mpSeg_.IsUserDictSingleChineseWord(words[i].left->rune))) {
        res.push_back(words[i]);
        continue;
      }

      // if mp Get a single one and it is not in userdict, collect it in sequence
      size_t j = i;
      while (j < words.size() && words[j].left == words[j].right && !mpSeg_.IsUserDictSingleChineseWord(words[j].left->rune)) {
        j++;
      }

      // Cut the sequence with hmm
      assert(j - 1 >= i);
      // TODO
      hmmSeg_.Cut(words[i].left, words[j - 1].left + 1, hmmRes);
      //put hmm result to result
      for (size_t k = 0; k < hmmRes.size(); k++) {
        res.push_back(hmmRes[k]);
      }

      //clear tmp vars
      hmmRes.clear();

      //let i jump over this piece
      i = j - 1;
    }
  }

  const DictTrie* GetDictTrie() const {
    return mpSeg_.GetDictTrie();
  }

  bool Tag(const string& src, vector<pair<string, string> >& res) const {
    return tagger_.Tag(src, res, (SegmentTagged &)*this);
  }

  string LookupTag(const string &str) const {
    return tagger_.LookupTag(str, (SegmentTagged &)*this);
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
  MPSegment mpSeg_;
  HMMSegment hmmSeg_;
  PosTagger tagger_;

  std::vector<std::string> stopWordList_;
  unordered_set<std::string_view> stopWords_;

}; // class MixSegment

} // namespace cppjieba

#endif
