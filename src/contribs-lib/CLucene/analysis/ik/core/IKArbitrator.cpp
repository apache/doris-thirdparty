#include "IKArbitrator.h"

CL_NS_USE2(analysis, ik)

void IKArbitrator::process(AnalyzeContext& context, bool use_smart) {
    auto org_lexemes = context.getOrgLexemes();
    auto org_lexeme = org_lexemes->pollFirst();
    LexemePath* cross_path = new LexemePath(pool_);

    auto process_path = [&](LexemePath* path) {
        if (path->size() == 1 || !use_smart) {
            // crossPath has no ambiguity or does not handle ambiguity
            // Directly output the current crossPath
            context.addLexemePath(path);
        } else {
            // Ambiguity handling for the current crossPath
            // Output the ambiguity resolution result: judgeResult
            context.addLexemePath(judge(path->getHead(), path->getPathLength()));
            delete path;
        }
    };

    while (org_lexeme) {
        if (!cross_path->addCrossLexeme(*org_lexeme)) {
            // Find the next crossPath that does not intersect with crossPath.
            process_path(cross_path);
            // Add orgLexeme to the new crossPath
            cross_path = new LexemePath(pool_);
            cross_path->addCrossLexeme(*org_lexeme);
        }
        org_lexeme = org_lexemes->pollFirst();
    }

    // Process the final path
    process_path(cross_path);
}

// Performs ambiguity resolution on a given lexeme path.
LexemePath* IKArbitrator::judge(Cell* lexeme_cell, size_t full_text_length) {
    // Candidate result path
    LexemePath* path_option = new LexemePath(pool_);

    // Traverse crossPath once and return the stack of conflicting Lexemes
    IKStack<Cell*> lexemeStack;
    forwardPath(lexeme_cell, path_option, lexemeStack);
    LexemePath* best_path = new LexemePath(*path_option, pool_);

    // Process ambiguous words if they exist
    while (!lexemeStack.empty()) {
        auto c = lexemeStack.top();
        lexemeStack.pop();
        backPath(c->getLexeme(), path_option);
        forwardPath(c, path_option);
        if (*path_option < *best_path) {
            delete best_path;
            best_path = new LexemePath(*path_option, pool_);
        }
    }
    delete path_option;
    // Return the optimal solution
    return best_path;
}

void IKArbitrator::forwardPath(Cell* lexeme_cell, LexemePath* path_option) {
    auto current_cell = lexeme_cell;
    while (current_cell) {
        path_option->addNotCrossLexeme(current_cell->getLexeme());
        current_cell = current_cell->getNext();
    }
}

void IKArbitrator::forwardPath(Cell* lexeme_cell, LexemePath* path_option,
                               IKStack<Cell*>& conflictStack) {
    auto current_cell = lexeme_cell;
    while (current_cell) {
        if (!path_option->addNotCrossLexeme(current_cell->getLexeme())) {
            conflictStack.push(current_cell);
        }
        current_cell = current_cell->getNext();
    }
}

void IKArbitrator::backPath(const Lexeme& lexeme, LexemePath* option) {
    while (option->checkCross(lexeme)) {
        option->removeTail();
    }
}