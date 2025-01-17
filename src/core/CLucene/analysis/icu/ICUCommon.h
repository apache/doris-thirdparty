#pragma once

#include <memory>

#include "CLucene.h"
#include "CLucene/debug/error.h"
#include "unicode/brkiter.h"
#include "unicode/rbbi.h"
#include "unicode/ubrk.h"
#include "unicode/uchar.h"
#include "unicode/uniset.h"
#include "unicode/unistr.h"
#include "unicode/uscript.h"
#include "unicode/utext.h"
#include "unicode/utf8.h"

namespace lucene::analysis {

using BreakIteratorPtr = std::unique_ptr<icu::BreakIterator>;

struct UTextDeleter {
    void operator()(UText* utext) const {
        if (utext != nullptr) {
            utext_close(utext);
        }
    }
};

using UTextPtr = std::unique_ptr<UText, UTextDeleter>;

} // namespace lucene::analysis