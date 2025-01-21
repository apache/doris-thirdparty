#ifndef CLUCENE_ANALYZECONTEXT_H
#define CLUCENE_ANALYZECONTEXT_H

#include <list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>

#include "CLucene/_ApiHeader.h"
#include "CLucene/analysis/ik/cfg/Configuration.h"
#include "CLucene/analysis/ik/core/CharacterUtil.h"
#include "CLucene/analysis/ik/core/LexemePath.h"
#include "CLucene/analysis/ik/dic/Dictionary.h"
#include "CLucene/analysis/ik/util/IKContainer.h"
#include "CLucene/analysis/ik/util/IKMemoryPool.h"
#include "CLucene/util/CLStreams.h"
#include "phmap.h"

CL_NS_DEF2(analysis, ik)

class CLUCENE_EXPORT AnalyzeContext {
private:
    static const size_t BUFF_SIZE = 4096;
    static const size_t BUFF_EXHAUST_CRITICAL = 100;

    static constexpr uint8_t CJK_SEGMENTER_FLAG = 0x01;    // 0001
    static constexpr uint8_t CN_QUANTIFIER_FLAG = 0x02;    // 0010
    static constexpr uint8_t LETTER_SEGMENTER_FLAG = 0x04; // 0100

    // String buffer
    std::string segment_buff_;
    // An array storing Unicode code points (runes)Character information array
    CharacterUtil::TypedRuneArray typed_runes_;
    // Total length of bytes analyzed
    size_t buffer_offset_;
    // Current character position pointer
    size_t cursor_;
    // Length of available bytes in the last read
    size_t available_;
    // Number of non-CJK characters at the end
    size_t last_useless_char_num_;

    // Sub-tokenizer lock
    uint8_t buffer_locker_ {0};
    //std::set<std::string> buffer_locker_;
    // Original tokenization result set
    QuickSortSet org_lexemes_;
    // LexemePath position index table
    phmap::flat_hash_map<size_t, LexemePath*> path_map_;
    // Final tokenization result set
    IKQue<Lexeme> results_;
    // Tokenizer configuration
    std::shared_ptr<Configuration> config_;
    IKMemoryPool<Cell>& pool_;
    void outputSingleCJK(size_t index);
    void compound(Lexeme& lexeme);

public:
    enum class SegmenterType {
        CJK_SEGMENTER,
        CN_QUANTIFIER,
        LETTER_SEGMENTER
    };
    const CharacterUtil::TypedRuneArray& getTypedRuneArray() const { return typed_runes_; }
    explicit AnalyzeContext(IKMemoryPool<Cell>& pool);
    virtual ~AnalyzeContext();

    void reset();
    void setConfig(std::shared_ptr<Configuration> configuration);

    size_t getCursor() const { return cursor_; }
    const char* getSegmentBuff() const { return segment_buff_.c_str(); }
    size_t getBufferOffset() const { return buffer_offset_; }
    size_t getLastUselessCharNum() const { return last_useless_char_num_; }

    size_t fillBuffer(lucene::util::Reader* reader);
    bool moveCursor();
    void initCursor();
    bool isBufferConsumed() const;
    bool needRefillBuffer() const;
    void markBufferOffset();

    void lockBuffer(SegmenterType type);
    void unlockBuffer(SegmenterType type);
    bool isBufferLocked() const;

    void addLexeme(Lexeme& lexeme);
    void addLexemePath(LexemePath* path);
    bool getNextLexeme(Lexeme& lexeme);
    QuickSortSet* getOrgLexemes() { return &org_lexemes_; }
    void outputToResult();

    size_t getCurrentCharLen() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].len : 0;
    }

    size_t getCurrentCharOffset() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].offset : 0;
    }

    int32_t getCurrentCharType() const { return typed_runes_[cursor_].char_type; }

    int32_t getCurrentChar() const {
        return cursor_ < typed_runes_.size() ? typed_runes_[cursor_].getChar() : 0;
    }
};

CL_NS_END2
#endif //CLUCENE_ANALYZECONTEXT_H
