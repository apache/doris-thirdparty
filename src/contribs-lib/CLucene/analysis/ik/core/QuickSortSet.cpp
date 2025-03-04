#include "QuickSortSet.h"

#include "CLucene/util/Misc.h"

CL_NS_DEF2(analysis, ik)

QuickSortSet::~QuickSortSet() {
    clear();
}

void QuickSortSet::clear() {
    if (head_) {
        pool_.mergeFreeList(head_, tail_, cell_size_);
        head_ = nullptr;
        tail_ = nullptr;
        cell_size_ = 0;
    }
}

// Add a lexeme to the linked list set
bool QuickSortSet::addLexeme(Lexeme& lexeme) {
    Cell* new_cell = nullptr;
    if (cell_size_ == 0) {
        new_cell = allocateCell(std::move(lexeme));
        head_ = tail_ = new_cell;
        cell_size_++;
        return true;
    }
    if (lexeme < tail_->lexeme_) {
        new_cell = allocateCell(std::move(lexeme));
        tail_->next_ = new_cell;
        new_cell->prev_ = tail_;
        tail_ = new_cell;
        cell_size_++;
        return true;
    }
    if (head_->lexeme_ < lexeme) {
        new_cell = allocateCell(std::move(lexeme));
        head_->prev_ = new_cell;
        new_cell->next_ = head_;
        head_ = new_cell;
        cell_size_++;
        return true;
    }
    if (tail_->lexeme_ == lexeme) {
        deallocateCell(new_cell);
        return false;
    }

    auto index = tail_;
    while (index && index->lexeme_ < lexeme) {
        index = index->prev_;
    }
    if (index && index->lexeme_ == lexeme) {
        return false;
    }
    new_cell = allocateCell(std::move(lexeme));
    new_cell->prev_ = index;
    if (auto next = index->next_) {
        new_cell->next_ = next;
        next->prev_ = new_cell;
    }
    index->next_ = new_cell;
    cell_size_++;
    return true;
}

const Lexeme* QuickSortSet::peekFirst() const {
    return head_ ? &head_->lexeme_ : nullptr;
}

std::optional<Lexeme> QuickSortSet::pollFirst() {
    if (!head_) return std::nullopt;
    Cell* old_head = head_;
    Lexeme result = std::move(old_head->getLexeme());

    head_ = head_->next_;
    if (head_)
        head_->prev_ = nullptr;
    else
        tail_ = nullptr;

    deallocateCell(old_head);
    --cell_size_;
    return result;
}

const Lexeme* QuickSortSet::peekLast() const {
    return tail_ ? &tail_->lexeme_ : nullptr;
}

std::optional<Lexeme> QuickSortSet::pollLast() {
    if (!tail_) return std::nullopt;
    Cell* old_tail = tail_;
    Lexeme result = std::move(old_tail->getLexeme());

    tail_ = tail_->prev_;
    if (tail_)
        tail_->next_ = nullptr;
    else
        head_ = nullptr;

    deallocateCell(old_tail);
    --cell_size_;
    return result;
}

size_t QuickSortSet::getPathBegin() const {
    return head_ ? head_->lexeme_.getByteBegin() : 0;
}

size_t QuickSortSet::getPathEnd() const {
    return tail_ ? tail_->lexeme_.getByteBegin() + tail_->lexeme_.getByteLength() : 0;
}

Cell* QuickSortSet::allocateCell(lucene::analysis::ik::Lexeme&& lexeme) {
    void* memory = pool_.allocate();
    return new (memory) Cell(std::move(lexeme));
}

void QuickSortSet::deallocateCell(Cell* cell) {
    if (cell) {
        cell->~Cell();
        pool_.deallocate(cell);
    }
}

CL_NS_END2