#ifndef CLUCENE_QUICKSORTSET_H
#define CLUCENE_QUICKSORTSET_H

#include <memory>
#include <optional>

#include "CLucene/_ApiHeader.h"
#include "CLucene/analysis/ik/util/IKMemoryPool.h"
#include "Lexeme.h"

CL_NS_DEF2(analysis, ik)

class Cell {
public:
    Cell() = default;
    explicit Cell(Lexeme&& lexeme) : lexeme_(std::move(lexeme)) {}
    ~Cell() = default;

    bool operator<(const Cell& other) const { return lexeme_ < other.lexeme_; };
    bool operator==(const Cell& other) const { return lexeme_ == other.lexeme_; };

    Cell* getPrev() const { return prev_; }
    Cell* getNext() const { return next_; }
    const Lexeme& getLexeme() const { return lexeme_; }
    Lexeme& getLexeme() { return lexeme_; }

private:
    // Do not change the position of the declarations of next_ and prev_, as this is related to the
    // mergeFreeList in IKMemoryPool.
    Cell* next_ = nullptr;
    Cell* prev_ = nullptr;
    Lexeme lexeme_;

    friend class QuickSortSet;
};

// IK Segmenter's Lexeme Quick Sort Set
class CLUCENE_EXPORT QuickSortSet {
    friend class IKArbitrator;

protected:
    Cell* head_ = nullptr;
    Cell* tail_ = nullptr;
    size_t cell_size_ = 0;

public:
    IKMemoryPool<Cell>& pool_;

    QuickSortSet(IKMemoryPool<Cell>& pool) : pool_(pool) {}
    virtual ~QuickSortSet();

    QuickSortSet(const QuickSortSet&) = delete;
    QuickSortSet& operator=(const QuickSortSet&) = delete;

    bool addLexeme(Lexeme& lexeme);
    const Lexeme* peekFirst() const;
    std::optional<Lexeme> pollFirst();
    const Lexeme* peekLast() const;
    std::optional<Lexeme> pollLast();
    void clear();

    size_t getPathBegin() const;
    size_t getPathEnd() const;

    size_t getSize() const { return cell_size_; }
    bool isEmpty() const { return cell_size_ == 0; }
    Cell* getHead() { return head_; }
    const Cell* getHead() const { return head_; }

private:
    Cell* allocateCell(Lexeme&& lexeme);
    void deallocateCell(Cell* cell);
};

CL_NS_END2
#endif //CLUCENE_QUICKSORTSET_H
