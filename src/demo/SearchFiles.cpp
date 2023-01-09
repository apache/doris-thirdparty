/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include <iostream>
#include <vector>
#include <stdio.h>

#include "CLucene/StdHeader.h"
#include "CLucene/_clucene-config.h"

#include "CLucene.h"
#include "CLucene/config/repl_tchar.h"
#include "CLucene/config/repl_wchar.h"
#include "CLucene/util/Misc.h"

using namespace std;
using namespace lucene::analysis;
using namespace lucene::index;
using namespace lucene::util;
using namespace lucene::queryParser;
using namespace lucene::document;
using namespace lucene::search;

void clearTermSet(TermSet &termSet) {
    for (auto pTerm: termSet) {
        _CLLDECDELETE(pTerm);
    }
    termSet.clear();
}

std::vector<std::wstring> simple_analyzer(const std::wstring& field_name, const std::wstring& value) {
    lucene::analysis::SimpleAnalyzer<TCHAR> analyzer;
    std::vector<std::wstring> analyse_result;

    auto reader =
            _CLNEW lucene::util::StringReader(value.c_str());
    auto token_stream =
            analyzer.tokenStream(field_name.c_str(), reader);

    lucene::analysis::Token token;
    while (token_stream->next(&token)) {
        std::wstring tk(token.termBuffer<TCHAR>(), token.termLength<TCHAR>());
        analyse_result.emplace_back(tk);
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }
    std::set<std::wstring> unrepeated_result(analyse_result.begin(), analyse_result.end());
    analyse_result.assign(unrepeated_result.begin(), unrepeated_result.end());
    return analyse_result;
}

void SearchFiles(const char *index, int &total) {
    standard::StandardAnalyzer analyzer;

    IndexReader *reader = IndexReader::open(index);

    IndexReader *newreader = reader->reopen();
    if (newreader != reader) {
        _CLLDELETE(reader);
        reader = newreader;
    }
    IndexSearcher s(reader);

    std::wstring field_ws = L"request";
    std::wstring token_wss = L"GET /english/frntpage.htm HTTP/1.0";
    auto q = _CLNEW lucene::search::BooleanQuery();
    auto tokens = simple_analyzer(field_ws, token_wss);
    for (auto token_ws: tokens)
    {
        lucene::index::Term *term =
                _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str());
        static_cast<lucene::search::BooleanQuery *>(q)
                ->add(_CLNEW lucene::search::TermQuery(term), true,
                      lucene::search::BooleanClause::MUST);
        _CLDECDELETE(term);
    }


    uint64_t str = Misc::currentTimeMillis();

    std::vector<uint32_t> result;
    s._search(q,
                            [&result, &total](const int32_t docid, const float_t /*score*/) {
                                // docid equal to rowid in segment
                                result.push_back(docid);
                                printf("docid is %d\n", docid);
                                total += 1;
                            });
    _CLLDELETE(q);

    s.close();
    reader->close();
    _CLLDELETE(reader);
    printf("\nFile %s search time taken: %d ms\n\n", index,(int32_t) (Misc::currentTimeMillis() - str));
}
