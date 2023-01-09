/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "test.h"
#include "CLucene/analysis/cjk/CJKAnalyzer.h"
#include "CLucene/analysis/LanguageBasedAnalyzer.h"
#ifdef _CL_HAVE_IO_H
#include <io.h>
#endif
#ifdef _CL_HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef _CL_HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef _CL_HAVE_DIRECT_H
#include <direct.h>
#endif

CL_NS_USE2(analysis, cjk)

void test(CuTest *tc, char* orig, Reader* reader, bool verbose, int64_t bytes) {
    StandardAnalyzer analyzer;
    TokenStream* stream = analyzer.tokenStream(NULL, reader);

    uint64_t start = Misc::currentTimeMillis();

    int32_t count = 0;
    Token t;
    char atmp[LUCENE_MAX_WORD_LEN + 1];
    TCHAR ttmp[LUCENE_MAX_WORD_LEN + 1];
    for (; stream->next(&t);) {
        if (verbose) {
            CuMessage(tc, _T("Text=%s start=%d end=%d\n"), t.termBuffer<TCHAR>(), t.startOffset(), t.endOffset());
        }
        int len = t.termLength<TCHAR>();

        //use the lucene strlwr function (so copy to TCHAR first then back)
        strncpy(atmp, orig + t.startOffset(), len);
        atmp[len] = 0;
        STRCPY_AtoT(ttmp, atmp, len + 1);
        _tcslwr(ttmp);

        if (_tcsncmp(t.termBuffer<TCHAR>(), ttmp, len) != 0) {
            TCHAR err[1024];
            _sntprintf(err, 1024, _T("token '%s' didnt match original text at %d-%d"), t.termBuffer<TCHAR>(), t.startOffset(), t.endOffset());
            CuAssert(tc, err, false);
        }

        // _CLDELETE(t);
        count++;
    }

    uint64_t end = Misc::currentTimeMillis();
    int64_t time = end - start;
    CuMessageA(tc, "%d milliseconds to extract ", time);
    CuMessageA(tc, "%d tokens\n", count);
    CuMessageA(tc, "%f microseconds/token\n", (time * 1000.0) / count);
    CuMessageA(tc, "%f megabytes/hour\n", (bytes * 1000.0 * 60.0 * 60.0) / (time * 1000000.0));

    _CLDELETE(stream);
}

void _testFile(CuTest *tc, const char* fname, bool verbose) {
    struct fileStat buf;
    fileStat(fname, &buf);
    int64_t bytes = buf.st_size;

    char* orig = _CL_NEWARRAY(char, bytes);
    {
        FILE* f = fopen(fname, "rb");
        int64_t r = fread(orig, bytes, 1, f);
        fclose(f);
    }

    CuMessageA(tc, " Reading test file containing %d bytes.\n", bytes);
    jstreams::FileReader fr(fname, "ASCII");
    const TCHAR *start;
    size_t total = 0;
    int32_t numRead;
    do {
        numRead = fr.read((const void**)&start, 1, 0);
        if (numRead == -1)
            break;
        total += numRead;
    } while (numRead >= 0);

    jstreams::FileReader reader(fname, "ASCII");

    test(tc, orig, &reader, verbose, total);

    _CLDELETE_CaARRAY(orig);
}

void testFile(CuTest *tc) {
    char loc[1024];
    strcpy(loc, clucene_data_location);
    strcat(loc, "/reuters-21578/feldman-cia-worldfactbook-data.txt");
    CuAssert(tc, _T("reuters-21578/feldman-cia-worldfactbook-data.txt does not exist"), Misc::dir_Exists(loc));
    _testFile(tc, loc, false);
}

void _testCJK(CuTest *tc, const char* astr, const char** results, bool ignoreSurrogates = true) {
    SimpleInputStreamReader r(new AStringReader(astr), SimpleInputStreamReader::UTF8);

    CJKTokenizer* tokenizer = _CLNEW CJKTokenizer(&r);
    tokenizer->setIgnoreSurrogates(ignoreSurrogates);
    int pos = 0;
    Token tok;
    TCHAR tres[LUCENE_MAX_WORD_LEN];

    while (results[pos] != NULL) {
        CLUCENE_ASSERT(tokenizer->next(&tok) != NULL);

        lucene_utf8towcs(tres, results[pos], LUCENE_MAX_WORD_LEN);
        //wcout << tres << "  actual " << tok.termBuffer<TCHAR>() << std::endl;
        CuAssertStrEquals(tc, _T("unexpected token value"), tres, tok.termBuffer<TCHAR>());
        pos++;
    }
    CLUCENE_ASSERT(!tokenizer->next(&tok));

    _CLDELETE(tokenizer);
}

void testCJK(CuTest *tc) {
    //utf16 test
    //we have a very large unicode character:
    //xEFFFF = utf8(F3 AF BF BF) = utf16(DB7F DFFF) = utf8(ED AD BF, ED BF BF)
    static const char* exp4[4] = {"我爱","爱你","", NULL};
    _testCJK(tc, "我爱你",exp4, false);

    static const char* exp3[4] = {"\xED\xAD\xBF\xED\xBF\xBF\xe5\x95\xa4", "\xe5\x95\xa4\xED\xAD\xBF\xED\xBF\xBF", "", NULL};
    _testCJK(tc, "\xED\xAD\xBF\xED\xBF\xBF\xe5\x95\xa4\xED\xAD\xBF\xED\xBF\xBF", exp3, false);

    static const char* exp1[5] = {"test", "t\xc3\xbcrm", "values", NULL};
    _testCJK(tc, "test t\xc3\xbcrm values", exp1);

    static const char* exp2[6] = {"a", "\xe5\x95\xa4\xe9\x85\x92", "\xe9\x85\x92\xe5\x95\xa4", "", "x", NULL};
    _testCJK(tc, "a\xe5\x95\xa4\xe9\x85\x92\xe5\x95\xa4x", exp2);
}

void testSimpleJiebaTokenizer(CuTest* tc) {
    LanguageBasedAnalyzer a;
    CL_NS(util)::StringReader reader(_T("我爱你中国"));
    reader.mark(50);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    ts = a.tokenStream(_T("contents"), &reader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("我爱你")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("中国")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaTokenizer2(CuTest* tc) {
    LanguageBasedAnalyzer a;
    CL_NS(util)::StringReader reader(_T("人民可以得到更多实惠"));
    reader.mark(50);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    ts = a.tokenStream(_T("contents"), &reader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("人民")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("可以")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("得到")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("更")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("多")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("实惠")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaTokenizer3(CuTest* tc) {
    LanguageBasedAnalyzer a;
    CL_NS(util)::StringReader reader(_T("中国人民银行"));
    reader.mark(50);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    ts = a.tokenStream(_T("contents"), &reader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("中国人民银行")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaTokenizer4(CuTest* tc) {
    LanguageBasedAnalyzer a;
    CL_NS(util)::StringReader reader(_T("人民，银行"));
    reader.mark(50);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    ts = a.tokenStream(_T("contents"), &reader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("人民")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("，")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("银行")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testChineseAnalyzer(CuTest* tc) {
    LanguageBasedAnalyzer a;
    CL_NS(util)::StringReader reader(_T("我爱你"));
    reader.mark(50);
    TokenStream* ts;
    Token t;

    //test with cjk
    a.setLanguage(_T("cjk"));
    a.setStem(false);
    ts = a.tokenStream(_T("contents"), &reader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("我爱")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("爱你")) == 0);
    _CLDELETE(ts);
}

void testChinese(CuTest *tc) {
    RAMDirectory dir;

    auto analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
    analyzer->setLanguage(L"cjk");

    IndexWriter w(&dir, analyzer, true);
    auto field_name = lucene::util::Misc::_charToWide("chinese");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    doc.add(*field);


    const char* field_value_data = "人民可以得到更多实惠";
    auto stringReader =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader);
    w.addDocument(&doc);

    const char* field_value_data1 = "中国人民银行";
    auto stringReader1 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data1), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader1);
    w.addDocument(&doc);

    const char* field_value_data2 = "洛杉矶人，洛杉矶居民";
    auto stringReader2 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data2), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader2);
    w.addDocument(&doc);

    const char* field_value_data3 = "民族，人民";
    auto stringReader3 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data3), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader3);
    w.addDocument(&doc);

    w.close();

    IndexSearcher searcher(&dir);
    Term *t1 = _CLNEW Term(_T("chinese"), _T("人民"));
    auto *query1 = _CLNEW TermQuery(t1);
    Hits *hits1 = searcher.search(query1);
    CLUCENE_ASSERT(3 == hits1->length());
    Term *t2 = _CLNEW Term(_T("chinese"), _T("民族"));
    auto *query2 = _CLNEW TermQuery(t2);
    Hits *hits2 = searcher.search(query2);
    CLUCENE_ASSERT(1 == hits2->length());

    doc.clear();
    //_CLDELETE(field)
}

void testJiebaMatch(CuTest* tc) {
    RAMDirectory dir;

    auto analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
    analyzer->setLanguage(L"chinese");

    IndexWriter w(&dir, analyzer, true);
    auto field_name = lucene::util::Misc::_charToWide("chinese");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    doc.add(*field);


    const char* field_value_data = "人民可以得到更多实惠";
    auto stringReader =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader);
    w.addDocument(&doc);

    const char* field_value_data1 = "中国人民银行";
    auto stringReader1 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data1), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader1);
    w.addDocument(&doc);

    const char* field_value_data2 = "洛杉矶人，洛杉矶居民";
    auto stringReader2 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data2), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader2);
    w.addDocument(&doc);

    const char* field_value_data3 = "民族，人民";
    auto stringReader3 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data3), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader3);
    w.addDocument(&doc);

    w.close();

    IndexSearcher searcher(&dir);

    lucene::util::Reader* reader = nullptr;
    std::vector<std::wstring> analyse_result;
    const char* value = "民族";
    analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer(L"chinese", false);
    reader = _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(value), lucene::util::SimpleInputStreamReader::UTF8);

    lucene::analysis::TokenStream* token_stream = analyzer->tokenStream(field_name, reader);

    lucene::analysis::Token token;

    while (token_stream->next(&token)) {
        if(token.termLength<TCHAR>() != 0) {
            analyse_result.emplace_back(token.termBuffer<TCHAR>(), token.termLength<TCHAR>());
        }
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    lucene::search::Query* query = _CLNEW lucene::search::BooleanQuery();
    for (const auto& t : analyse_result) {
        //std::wstring token_ws = std::wstring(token.begin(), token.end());
        auto* term =
                _CLNEW lucene::index::Term(field_name, t.c_str());
        dynamic_cast<lucene::search::BooleanQuery*>(query)
                ->add(_CLNEW lucene::search::TermQuery(term), true,
                      lucene::search::BooleanClause::SHOULD);
        _CLDECDELETE(term);
    }

    Hits *hits1 = searcher.search(query);
    CLUCENE_ASSERT(1 == hits1->length());

    doc.clear();
}

void testJiebaMatch2(CuTest* tc) {
    RAMDirectory dir;

    auto analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
    analyzer->setLanguage(L"chinese");

    IndexWriter w(&dir, analyzer, true);
    auto field_name = lucene::util::Misc::_charToWide("chinese");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    doc.add(*field);


    const char* field_value_data = "人民可以得到更多实惠";
    auto stringReader =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader);
    w.addDocument(&doc);

    const char* field_value_data1 = "中国人民银行";
    auto stringReader1 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data1), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader1);
    w.addDocument(&doc);

    const char* field_value_data2 = "洛杉矶人，洛杉矶居民";
    auto stringReader2 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data2), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader2);
    w.addDocument(&doc);

    const char* field_value_data3 = "民族，人民";
    auto stringReader3 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data3), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader3);
    w.addDocument(&doc);

    w.close();

    IndexSearcher searcher(&dir);

    lucene::util::Reader* reader = nullptr;
    std::vector<std::wstring> analyse_result;
    const char* value = "人民";
    analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer(L"chinese", false);
    reader = _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(value), lucene::util::SimpleInputStreamReader::UTF8);

    lucene::analysis::TokenStream* token_stream = analyzer->tokenStream(field_name, reader);

    lucene::analysis::Token token;

    while (token_stream->next(&token)) {
        if(token.termLength<TCHAR>() != 0) {
            analyse_result.emplace_back(token.termBuffer<TCHAR>(), token.termLength<TCHAR>());
        }
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    lucene::search::Query* query = _CLNEW lucene::search::BooleanQuery();
    for (const auto& t : analyse_result) {
        //std::wstring token_ws = std::wstring(token.begin(), token.end());
        auto* term =
                _CLNEW lucene::index::Term(field_name, t.c_str());
        dynamic_cast<lucene::search::BooleanQuery*>(query)
                ->add(_CLNEW lucene::search::TermQuery(term), true,
                      lucene::search::BooleanClause::SHOULD);
        _CLDECDELETE(term);
    }

    Hits *hits1 = searcher.search(query);
    CLUCENE_ASSERT(2 == hits1->length());

    doc.clear();
}

void testChineseMatch(CuTest* tc) {
    RAMDirectory dir;

    auto analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
    analyzer->setLanguage(L"cjk");

    IndexWriter w(&dir, analyzer, true);
    auto field_name = lucene::util::Misc::_charToWide("chinese");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    doc.add(*field);


    const char* field_value_data = "人民可以得到更多实惠";
    auto stringReader =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader);
    w.addDocument(&doc);

    const char* field_value_data1 = "中国人民银行";
    auto stringReader1 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data1), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader1);
    w.addDocument(&doc);

    const char* field_value_data2 = "洛杉矶人，洛杉矶居民";
    auto stringReader2 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data2), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader2);
    w.addDocument(&doc);

    const char* field_value_data3 = "民族，人民";
    auto stringReader3 =
            _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(field_value_data3), lucene::util::SimpleInputStreamReader::UTF8);
    field->setValue(stringReader3);
    w.addDocument(&doc);

    w.close();

    IndexSearcher searcher(&dir);

    lucene::util::Reader* reader = nullptr;
    std::vector<std::wstring> analyse_result;
    const char* value = "民族";
    analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer(L"cjk", false);
    reader = _CLNEW lucene::util::SimpleInputStreamReader(new lucene::util::AStringReader(value), lucene::util::SimpleInputStreamReader::UTF8);

    lucene::analysis::TokenStream* token_stream = analyzer->tokenStream(field_name, reader);

    lucene::analysis::Token token;

    while (token_stream->next(&token)) {
        if(token.termLength<TCHAR>() != 0) {
            analyse_result.emplace_back(token.termBuffer<TCHAR>(), token.termLength<TCHAR>());
        }
    }

    if (token_stream != nullptr) {
        token_stream->close();
    }

    lucene::search::Query* query = _CLNEW lucene::search::BooleanQuery();
    for (const auto& t : analyse_result) {
        //std::wstring token_ws = std::wstring(token.begin(), token.end());
        auto* term =
                _CLNEW lucene::index::Term(field_name, t.c_str());
        dynamic_cast<lucene::search::BooleanQuery*>(query)
                ->add(_CLNEW lucene::search::TermQuery(term), true,
                      lucene::search::BooleanClause::SHOULD);
        _CLDECDELETE(term);
    }

    Hits *hits1 = searcher.search(query);
    CLUCENE_ASSERT(1 == hits1->length());

    doc.clear();
}

void testLanguageBasedAnalyzer(CuTest* tc) {
    LanguageBasedAnalyzer a;
    CL_NS(util)::StringReader reader(_T("he abhorred accentueren"));
    reader.mark(50);
    TokenStream* ts;
    Token t;

    //test with english
    a.setLanguage(_T("English"));
    a.setStem(false);
    ts = a.tokenStream(_T("contents"), &reader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("he")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("abhorred")) == 0);
    _CLDELETE(ts);

    //now test with dutch
    reader.reset(0);
    a.setLanguage(_T("Dutch"));
    a.setStem(true);
    ts = a.tokenStream(_T("contents"), &reader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("he")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("abhorred")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("accentuer")) == 0);
    _CLDELETE(ts);
}

CuSuite *testchinese(void) {
    CuSuite *suite = CuSuiteNew(_T("CLucene Analysis Test"));

    SUITE_ADD_TEST(suite, testFile);
    SUITE_ADD_TEST(suite, testCJK);
    SUITE_ADD_TEST(suite, testLanguageBasedAnalyzer);
    SUITE_ADD_TEST(suite, testChineseAnalyzer);
    SUITE_ADD_TEST(suite, testSimpleJiebaTokenizer);
    SUITE_ADD_TEST(suite, testSimpleJiebaTokenizer2);
    SUITE_ADD_TEST(suite, testSimpleJiebaTokenizer3);
    SUITE_ADD_TEST(suite, testSimpleJiebaTokenizer4);
    SUITE_ADD_TEST(suite, testChineseMatch);
    SUITE_ADD_TEST(suite, testJiebaMatch);
    SUITE_ADD_TEST(suite, testJiebaMatch2);

    return suite;
}
// EOF