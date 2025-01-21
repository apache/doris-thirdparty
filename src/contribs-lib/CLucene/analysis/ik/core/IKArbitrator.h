#ifndef CLUCENE_IKARBITRATOR_H
#define CLUCENE_IKARBITRATOR_H

#include <memory>
#include <set>
#include <stack>

#include "AnalyzeContext.h"
#include "CLucene/analysis/ik/util/IKContainer.h"
#include "LexemePath.h"
#include "QuickSortSet.h"

CL_NS_DEF2(analysis, ik)

class IKArbitrator {
public:
    IKArbitrator(IKMemoryPool<Cell>& pool) : pool_(pool) {}
    // Ambiguity handling
    void process(AnalyzeContext& context, bool use_smart);

private:
    IKMemoryPool<Cell>& pool_;
    // Ambiguity identification
    LexemePath* judge(Cell* lexeme_cell, size_t full_text_length);

    // Forward traversal, add lexeme, construct a non-ambiguous token combination
    void forwardPath(Cell* lexeme_cell, LexemePath* path_option, IKStack<Cell*>& conflictStack);
    void forwardPath(Cell* lexeme_cell, LexemePath* path_option);
    // Roll back the token chain until it can accept the specified token
    void backPath(const Lexeme& lexeme, LexemePath* path_option);
};

CL_NS_END2
#endif //CLUCENE_IKARBITRATOR_H
