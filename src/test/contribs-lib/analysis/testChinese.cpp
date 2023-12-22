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

void testSimpleJiebaSearchModeTokenizer2(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "冰咒龙";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::Search);
    a.initDict("./dict");
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("冰咒")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("龙")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaAllModeTokenizer2(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "冰咒龙";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::All);
    a.initDict("./dict");
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("冰")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("咒")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("龙")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaAllModeTokenizer(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "我来到北京清华大学";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::All);
    a.initDict("./dict");
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("来到")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("北京")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("清华")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("清华大学")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("华大")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("大学")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaDefaultModeTokenizer2(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "中国的科技发展在世界上处于领先";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::Default);
    a.initDict("./dict");
    ts = a.tokenStream(_T("contents"), stringReader);

    /*char tmp[255] = {};
    while(ts->next(&t) != nullptr) {
        lucene_wcstoutf8(tmp, t.termBuffer<TCHAR>(), 254);
        std::cout << tmp << std::endl;
    }*/

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("中国")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("科技")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("发展")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("在世界上")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("处于")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("领先")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaDefaultModeTokenizer(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "我来到北京清华大学";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::Default);
    a.initDict("./dict");
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("来到")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("北京")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("清华大学")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaSearchModeTokenizer(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "我来到北京清华大学";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::Search);
    a.initDict("./dict");
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("我")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("来到")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("北京")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("清华")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("华大")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("大学")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("清华大学")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaTokenizer(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "我爱你中国";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::Default);
    a.initDict("./dict");
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("我爱你")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("中国")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaTokenizer2(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "人民可以得到更多实惠";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::Default);
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("人民")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("得到")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("更")) == 0);
    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("实惠")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaTokenizer3(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "中国人民银行";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);

    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    a.setMode(lucene::analysis::AnalyzerMode::Default);
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("中国人民银行")) == 0);
    CLUCENE_ASSERT(ts->next(&t) == NULL);
    _CLDELETE(ts);
}

void testSimpleJiebaTokenizer4(CuTest* tc) {
    LanguageBasedAnalyzer a;
    const char* field_value_data = "人民，银行";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    TokenStream* ts;
    Token t;

    //test with chinese
    a.setLanguage(_T("chinese"));
    a.setStem(false);
    ts = a.tokenStream(_T("contents"), stringReader);

    CLUCENE_ASSERT(ts->next(&t) != NULL);
    CLUCENE_ASSERT(_tcscmp(t.termBuffer<TCHAR>(), _T("人民")) == 0);
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
    analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
    IndexWriter w(&dir, analyzer, true);
    auto field_name = lucene::util::Misc::_charToWide("chinese");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    doc.add(*field);


    const char* field_value_data = "人民可以得到更多实惠";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    field->setValue(stringReader);
    w.addDocument(&doc);

    const char* field_value_data1 = "中国人民银行";
    auto stringReader1 =
            _CLNEW lucene::util::SStringReader<char>(field_value_data1, strlen(field_value_data1), false);
    field->setValue(stringReader1);
    w.addDocument(&doc);

    const char* field_value_data2 = "洛杉矶人，洛杉矶居民";
    auto stringReader2 =
            _CLNEW lucene::util::SStringReader<char>(field_value_data2, strlen(field_value_data2), false);
    field->setValue(stringReader2);
    w.addDocument(&doc);

    const char* field_value_data3 = "民族，人民";
    auto stringReader3 =
            _CLNEW lucene::util::SStringReader<char>(field_value_data3, strlen(field_value_data3), false);
    field->setValue(stringReader3);
    w.addDocument(&doc);

    w.close();

    IndexSearcher searcher(&dir);

    lucene::util::Reader* reader = nullptr;
    std::vector<std::wstring> analyse_result;
    const char* value = "民族";
    analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer(L"chinese", false);
    reader = _CLNEW lucene::util::SStringReader<char>(value, strlen(value), false);

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
    analyzer->setMode(lucene::analysis::AnalyzerMode::Default);

    IndexWriter w(&dir, analyzer, true);
    auto field_name = lucene::util::Misc::_charToWide("chinese");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    doc.add(*field);


    const char* field_value_data = "人民可以得到更多实惠";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    field->setValue(stringReader);
    w.addDocument(&doc);

    const char* field_value_data1 = "中国人民银行";
    auto stringReader1 =
            _CLNEW lucene::util::SStringReader<char>(field_value_data1, strlen(field_value_data1), false);
    field->setValue(stringReader1);
    w.addDocument(&doc);

    const char* field_value_data2 = "洛杉矶人，洛杉矶居民";
    auto stringReader2 =
            _CLNEW lucene::util::SStringReader<char>(field_value_data2, strlen(field_value_data2), false);
    field->setValue(stringReader2);
    w.addDocument(&doc);

    const char* field_value_data3 = "民族，人民";
    auto stringReader3 =
            _CLNEW lucene::util::SStringReader<char>(field_value_data3, strlen(field_value_data3), false);
    field->setValue(stringReader3);
    w.addDocument(&doc);

    w.close();

    IndexSearcher searcher(&dir);

    lucene::util::Reader* reader = nullptr;
    std::vector<std::wstring> analyse_result;
    const char* value = "人民";
    analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer(L"chinese", false);
    reader = _CLNEW lucene::util::SStringReader<char>(value, strlen(value), false);

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

void testJiebaMatchHuge(CuTest* tc) {
    RAMDirectory dir;

    auto analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer();
    analyzer->setLanguage(L"chinese");
    analyzer->setMode(lucene::analysis::AnalyzerMode::Default);
    analyzer->initDict("./dict");

    IndexWriter w(&dir, analyzer, true);
    auto field_name = lucene::util::Misc::_charToWide("chinese");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    doc.add(*field);


    const char* field_value_data = "数据模型\n"
            "本文档主要从逻辑层面，描述 Doris 的数据模型，以帮助用户更好的使用 Doris 应对不同的业务场景。\n"
            "\n"
            "基本概念\n"
            "在 Doris 中，数据以表（Table）的形式进行逻辑上的描述。 一张表包括行（Row）和列（Column）。Row 即用户的一行数据。Column 用于描述一行数据中不同的字段。\n"
            "\n"
            "Column 可以分为两大类：Key 和 Value。从业务角度看，Key 和 Value 可以分别对应维度列和指标列。Doris的key列是建表语句中指定的列，建表语句中的关键字\\'unique key\\'或\\'aggregate key\\'或\\'duplicate key\\'后面的列就是 Key 列，除了 Key 列剩下的就是 Value a列。\n"
            "\n"
            "Doris 的数据模型主要分为3类:\n"
            "\n"
            "Aggregate\n"
            "Unique\n"
            "Duplicate\n"
            "下面我们分别介绍。\n"
            "\n"
            "Aggregate 模型\n"
            "我们以实际的例子来说明什么是聚合模型，以及如何正确的使用聚合模型。\n"
            "\n"
            "示例1：导入数据聚合\n"
            "假设业务有如下数据表模式：\n"
            "\n"
            "ColumnName Type AggregationType Comment\n"
            "user_id LARGEINT 用户id\n"
            "date DATE 数据灌入日期\n"
            "city VARCHAR(20) 用户所在城市\n"
            "age SMALLINT 用户年龄\n"
            "sex TINYINT 用户性别\n"
            "last_visit_date DATETIME REPLACE 用户最后一次访问时间\n"
            "cost BIGINT SUM 用户总消费\n"
            "max_dwell_time INT MAX 用户最大停留时间\n"
            "min_dwell_time INT MIN 用户最小停留时间\n"
            "如果转换成建表语句则如下（省略建表语句中的 Partition 和 Distribution 信息）\n"
            "\n"
            "CREATE TABLE IF NOT EXISTS example_db.example_tbl\n"
            "(\n"
            "user_id LARGEINT NOT NULL COMMENT \"用户id\",\n"
            "date DATE NOT NULL COMMENT \"数据灌入日期时间\",\n"
            "city VARCHAR(20) COMMENT \"用户所在城市\",\n"
            "age SMALLINT COMMENT \"用户年龄\",\n"
            "sex TINYINT COMMENT \"用户性别\",\n"
            "last_visit_date DATETIME REPLACE DEFAULT \"1970-01-01 00:00:00\" COMMENT \"用户最后一次访问时间\",\n"
            "cost BIGINT SUM DEFAULT \"0\" COMMENT \"用户总消费\",\n"
            "max_dwell_time INT MAX DEFAULT \"0\" COMMENT \"用户最大停留时间\",\n"
            "min_dwell_time INT MIN DEFAULT \"99999\" COMMENT \"用户最小停留时间\"\n"
            ")\n"
            "AGGREGATE KEY(user_id, date, city, age, sex)\n"
            "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
            "PROPERTIES (\n"
            "\"replication_allocation\" = \"tag.location.default: 1\"\n"
            ");\n"
            "\n"
            "可以看到，这是一个典型的用户信息和访问行为的事实表。 在一般星型模型中，用户信息和访问行为一般分别存放在维度表和事实表中。这里我们为了更加方便的解释 Doris 的数据模型，将两部分信息统一存放在一张表中。\n"
            "\n"
            "表中的列按照是否设置了 AggregationType，分为 Key (维度列) 和 Value（指标列）。没有设置 AggregationType 的，如 user_id、date、age ... 等称为 Key，而设置了 AggregationType 的称为 Value。\n"
            "\n"
            "当我们导入数据时，对于 Key 列相同的行会聚合成一行，而 Value 列会按照设置的 AggregationType 进行聚合。 AggregationType 目前有以下四种聚合方式：\n"
            "\n"
            "SUM：求和，多行的 Value 进行累加。\n"
            "REPLACE：替代，下一批数据中的 Value 会替换之前导入过的行中的 Value。\n"
            "MAX：保留最大值。\n"
            "MIN：保留最小值。\n"
            "假设我们有以下导入数据（原始数据）：\n"
            "\n"
            "user_id date city age sex last_visit_date cost max_dwell_time min_dwell_time\n"
            "10000 2017-10-01 北京 20 0 2017-10-01 06:00:00 20 10 10\n"
            "10000 2017-10-01 北京 20 0 2017-10-01 07:00:00 15 2 2\n"
            "10001 2017-10-01 北京 30 1 2017-10-01 17:05:45 2 22 22\n"
            "10002 2017-10-02 上海 20 1 2017-10-02 12:59:12 200 5 5\n"
            "10003 2017-10-02 广州 32 0 2017-10-02 11:20:00 30 11 11\n"
            "10004 2017-10-01 深圳 35 0 2017-10-01 10:00:15 100 3 3\n"
            "10004 2017-10-03 深圳 35 0 2017-10-03 10:20:22 11 6 6\n"
            "通过sql导入数据：\n"
            "\n"
            "insert into example_db.example_tbl values\n"
            "(10000,\"2017-10-01\",\"北京\",20,0,\"2017-10-01 06:00:00\",20,10,10),\n"
            "(10000,\"2017-10-01\",\"北京\",20,0,\"2017-10-01 07:00:00\",15,2,2),\n"
            "(10001,\"2017-10-01\",\"北京\",30,1,\"2017-10-01 17:05:45\",2,22,22),\n"
            "(10002,\"2017-10-02\",\"上海\",20,1,\"2017-10-02 12:59:12\",200,5,5),\n"
            "(10003,\"2017-10-02\",\"广州\",32,0,\"2017-10-02 11:20:00\",30,11,11),\n"
            "(10004,\"2017-10-01\",\"深圳\",35,0,\"2017-10-01 10:00:15\",100,3,3),\n"
            "(10004,\"2017-10-03\",\"深圳\",35,0,\"2017-10-03 10:20:22\",11,6,6);\n"
            "\n"
            "我们假设这是一张记录用户访问某商品页面行为的表。我们以第一行数据为例，解释如下：\n"
            "\n"
            "数据 说明\n"
            "10000 用户id，每个用户唯一识别id\n"
            "2017-10-01 数据入库时间，精确到日期\n"
            "北京 用户所在城市\n"
            "20 用户年龄\n"
            "0 性别男（1 代表女性）\n"
            "2017-10-01 06:00:00 用户本次访问该页面的时间，精确到秒\n"
            "20 用户本次访问产生的消费\n"
            "10 用户本次访问，驻留该页面的时间\n"
            "10 用户本次访问，驻留该页面的时间（冗余）\n"
            "那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：\n"
            "\n"
            "user_id date city age sex last_visit_date cost max_dwell_time min_dwell_time\n"
            "10000 2017-10-01 北京 20 0 2017-10-01 07:00:00 35 10 2\n"
            "10001 2017-10-01 北京 30 1 2017-10-01 17:05:45 2 22 22\n"
            "10002 2017-10-02 上海 20 1 2017-10-02 12:59:12 200 5 5\n"
            "10003 2017-10-02 广州 32 0 2017-10-02 11:20:00 30 11 11\n"
            "10004 2017-10-01 深圳 35 0 2017-10-01 10:00:15 100 3 3\n"
            "10004 2017-10-03 深圳 35 0 2017-10-03 10:20:22 11 6 6\n"
            "可以看到，用户 10000 只剩下了一行聚合后的数据。而其余用户的数据和原始数据保持一致。这里先解释下用户 10000 聚合后的数据：\n"
            "\n"
            "前5列没有变化，从第6列 last_visit_date 开始：\n"
            "\n"
            "2017-10-01 07:00:00：因为 last_visit_date 列的聚合方式为 REPLACE，所以 2017-10-01 07:00:00 替换了 2017-10-01 06:00:00 保存了下来。\n"
            "\n"
            "注：在同一个导入批次中的数据，对于 REPLACE 这种聚合方式，替换顺序不做保证。如在这个例子中，最终保存下来的，也有可能是 2017-10-01 06:00:00。而对于不同导入批次中的数据，可以保证，后一批次的数据会替换前一批次。\n"
            "\n"
            "35：因为 cost 列的聚合类型为 SUM，所以由 20 + 15 累加获得 35。\n"
            "\n"
            "10：因为 max_dwell_time 列的聚合类型为 MAX，所以 10 和 2 取最大值，获得 10。\n"
            "\n"
            "2：因为 min_dwell_time 列的聚合类型为 MIN，所以 10 和 2 取最小值，获得 2。\n"
            "\n"
            "经过聚合，Doris 中最终只会存储聚合后的数据。换句话说，即明细数据会丢失，用户不能够再查询到聚合前的明细数据了。\n"
            "\n"
            "示例2：保留明细数据\n"
            "接示例1，我们将表结构修改如下：\n"
            "\n"
            "ColumnName Type AggregationType Comment\n"
            "user_id LARGEINT 用户id\n"
            "date DATE 数据灌入日期\n"
            "timestamp DATETIME 数据灌入时间，精确到秒\n"
            "city VARCHAR(20) 用户所在城市\n"
            "age SMALLINT 用户年龄\n"
            "sex TINYINT 用户性别\n"
            "last_visit_date DATETIME REPLACE 用户最后一次访问时间\n"
            "cost BIGINT SUM 用户总消费\n"
            "max_dwell_time INT MAX 用户最大停留时间\n"
            "min_dwell_time INT MIN 用户最小停留时间\n"
            "即增加了一列 timestamp，记录精确到秒的数据灌入时间。 同时，将AGGREGATE KEY设置为AGGREGATE KEY(user_id, date, timestamp, city, age, sex)\n"
            "\n"
            "导入数据如下：\n"
            "\n"
            "user_id date timestamp city age sex last_visit_date cost max_dwell_time min_dwell_time\n"
            "10000 2017-10-01 2017-10-01 08:00:05 北京 20 0 2017-10-01 06:00:00 20 10 10\n"
            "10000 2017-10-01 2017-10-01 09:00:05 北京 20 0 2017-10-01 07:00:00 15 2 2\n"
            "10001 2017-10-01 2017-10-01 18:12:10 北京 30 1 2017-10-01 17:05:45 2 22 22\n"
            "10002 2017-10-02 2017-10-02 13:10:00 上海 20 1 2017-10-02 12:59:12 200 5 5\n"
            "10003 2017-10-02 2017-10-02 13:15:00 广州 32 0 2017-10-02 11:20:00 30 11 11\n"
            "10004 2017-10-01 2017-10-01 12:12:48 深圳 35 0 2017-10-01 10:00:15 100 3 3\n"
            "10004 2017-10-03 2017-10-03 12:38:20 深圳 35 0 2017-10-03 10:20:22 11 6 6\n"
            "通过sql导入数据：\n"
            "\n"
            "insert into example_db.example_tbl values\n"
            "(10000,\"2017-10-01\",\"2017-10-01 08:00:05\",\"北京\",20,0,\"2017-10-01 06:00:00\",20,10,10),\n"
            "(10000,\"2017-10-01\",\"2017-10-01 09:00:05\",\"北京\",20,0,\"2017-10-01 07:00:00\",15,2,2),\n"
            "(10001,\"2017-10-01\",\"2017-10-01 18:12:10\",\"北京\",30,1,\"2017-10-01 17:05:45\",2,22,22),\n"
            "(10002,\"2017-10-02\",\"2017-10-02 13:10:00\",\"上海\",20,1,\"2017-10-02 12:59:12\",200,5,5),\n"
            "(10003,\"2017-10-02\",\"2017-10-02 13:15:00\",\"广州\",32,0,\"2017-10-02 11:20:00\",30,11,11),\n"
            "(10004,\"2017-10-01\",\"2017-10-01 12:12:48\",\"深圳\",35,0,\"2017-10-01 10:00:15\",100,3,3),\n"
            "(10004,\"2017-10-03\",\"2017-10-03 12:38:20\",\"深圳\",35,0,\"2017-10-03 10:20:22\",11,6,6);\n"
            "\n"
            "那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：\n"
            "\n"
            "user_id date timestamp city age sex last_visit_date cost max_dwell_time min_dwell_time\n"
            "10000 2017-10-01 2017-10-01 08:00:05 北京 20 0 2017-10-01 06:00:00 20 10 10\n"
            "10000 2017-10-01 2017-10-01 09:00:05 北京 20 0 2017-10-01 07:00:00 15 2 2\n"
            "10001 2017-10-01 2017-10-01 18:12:10 北京 30 1 2017-10-01 17:05:45 2 22 22\n"
            "10002 2017-10-02 2017-10-02 13:10:00 上海 20 1 2017-10-02 12:59:12 200 5 5\n"
            "10003 2017-10-02 2017-10-02 13:15:00 广州 32 0 2017-10-02 11:20:00 30 11 11\n"
            "10004 2017-10-01 2017-10-01 12:12:48 深圳 35 0 2017-10-01 10:00:15 100 3 3\n"
            "10004 2017-10-03 2017-10-03 12:38:20 深圳 35 0 2017-10-03 10:20:22 11 6 6\n"
            "我们可以看到，存储的数据，和导入数据完全一样，没有发生任何聚合。这是因为，这批数据中，因为加入了 timestamp 列，所有行的 Key 都不完全相同。也就是说，只要保证导入的数据中，每一行的 Key 都不完全相同，那么即使在聚合模型下，Doris 也可以保存完整的明细数据。\n"
            "\n"
            "示例3：导入数据与已有数据聚合\n"
            "接示例1。假设现在表中已有数据如下：\n"
            "\n"
            "user_id date city age sex last_visit_date cost max_dwell_time min_dwell_time\n"
            "10000 2017-10-01 北京 20 0 2017-10-01 07:00:00 35 10 2\n"
            "10001 2017-10-01 北京 30 1 2017-10-01 17:05:45 2 22 22\n"
            "10002 2017-10-02 上海 20 1 2017-10-02 12:59:12 200 5 5\n"
            "10003 2017-10-02 广州 32 0 2017-10-02 11:20:00 30 11 11\n"
            "10004 2017-10-01 深圳 35 0 2017-10-01 10:00:15 100 3 3\n"
            "10004 2017-10-03 深圳 35 0 2017-10-03 10:20:22 11 6 6\n"
            "我们再导入一批新的数据：\n"
            "\n"
            "user_id date city age sex last_visit_date cost max_dwell_time min_dwell_time\n"
            "10004 2017-10-03 深圳 35 0 2017-10-03 11:22:00 44 19 19\n"
            "10005 2017-10-03 长沙 29 1 2017-10-03 18:11:02 3 1 1\n"
            "通过sql导入数据：\n"
            "\n"
            "insert into example_db.example_tbl values\n"
            "(10004,\"2017-10-03\",\"深圳\",35,0,\"2017-10-03 11:22:00\",44,19,19),\n"
            "(10005,\"2017-10-03\",\"长沙\",29,1,\"2017-10-03 18:11:02\",3,1,1);\n"
            "\n"
            "那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：\n"
            "\n"
            "user_id date city age sex last_visit_date cost max_dwell_time min_dwell_time\n"
            "10000 2017-10-01 北京 20 0 2017-10-01 07:00:00 35 10 2\n"
            "10001 2017-10-01 北京 30 1 2017-10-01 17:05:45 2 22 22\n"
            "10002 2017-10-02 上海 20 1 2017-10-02 12:59:12 200 5 5\n"
            "10003 2017-10-02 广州 32 0 2017-10-02 11:20:00 30 11 11\n"
            "10004 2017-10-01 深圳 35 0 2017-10-01 10:00:15 100 3 3\n"
            "10004 2017-10-03 深圳 35 0 2017-10-03 11:22:00 55 19 6\n"
            "10005 2017-10-03 长沙 29 1 2017-10-03 18:11:02 3 1 1\n"
            "可以看到，用户 10004 的已有数据和新导入的数据发生了聚合。同时新增了 10005 用户的数据。\n"
            "\n"
            "数据的聚合，在 Doris 中有如下三个阶段发生：\n"
            "\n"
            "每一批次数据导入的 ETL 阶段。该阶段会在每一批次导入的数据内部进行聚合。\n"
            "底层 BE 进行数据 Compaction 的阶段。该阶段，BE 会对已导入的不同批次的数据进行进一步的聚合。\n"
            "数据查询阶段。在数据查询时，对于查询涉及到的数据，会进行对应的聚合。\n"
            "数据在不同时间，可能聚合的程度不一致。比如一批数据刚导入时，可能还未与之前已存在的数据进行聚合。但是对于用户而言，用户只能查询到聚合后的数据。即不同的聚合程度对于用户查询而言是透明的。用户需始终认为数据以最终的完成的聚合程度存在，而不应假设某些聚合还未发生。（可参阅聚合模型的局限性一节获得更多详情。）\n"
            "\n"
            "Unique 模型\n"
            "在某些多维分析场景下，用户更关注的是如何保证 Key 的唯一性，即如何获得 Primary Key 唯一性约束。因此，我们引入了 /；·90Unique 数据模型。在1.2版本之前，该模型本质上是聚合模型的一个特例，也是一种简化的表结构表示方式。由于聚合模型的实现方式是读时合并（merge on read)，因此在一些聚合查询上性能不佳（参考后续章节聚合模型的局限性的描述），在1.2版本我们引入了Unique模型新的实现方式，写时合并（merge on write），通过在写入时做一些额外的工作，实现了最优的查询性能。写时合并将在未来替换读时合并成为Unique模型的默认实现方式，两者将会短暂的共存一段时间。下面将对两种实现方式分别举例进行说明。\n"
            "\n"
            "读时合并（与聚合模型相同的实现方式）\n"
            "ColumnName Type IsKey Comment\n"
            "user_id BIGINT Yes 用户id\n"
            "username VARCHAR(50) Yes 用户昵称\n"
            "city VARCHAR(20) No 用户所在城市\n"
            "age SMALLINT No 用户年龄\n"
            "sex TINYINT No 用户性别\n"
            "phone LARGEINT No 用户电话\n"
            "address VARCHAR(500) No 用户住址\n"
            "register_time DATETIME No 用户注册时间\n"
            "这是一个典型的用户基础信息表。这类数据没有聚合需求，只需保证主键唯一性。（这里的主键为 user_id + username）。那么我们的建表语句如下：\n"
            "\n"
            "CREATE TABLE IF NOT EXISTS example_db.example_tbl\n"
            "(\n"
            "user_id LARGEINT NOT NULL COMMENT \"用户id\",\n"
            "username VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n"
            "city VARCHAR(20) COMMENT \"用户所在城市\",\n"
            "age SMALLINT COMMENT \"用户年龄\",\n"
            "sex TINYINT COMMENT \"用户性别\",\n"
            "phone LARGEINT COMMENT \"用户电话\",\n"
            "address VARCHAR(500) COMMENT \"用户地址\",\n"
            "register_time DATETIME COMMENT \"用户注册时间\"\n"
            ")\n"
            "UNIQUE KEY(user_id, username)\n"
            "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
            "PROPERTIES (\n"
            "\"replication_allocation\" = \"tag.location.default: 1\"\n"
            ");\n"
            "\n"
            "而这个表结构，完全同等于以下使用聚合模型描述的表结构：\n"
            "\n"
            "ColumnName Type AggregationType Comment\n"
            "user_id BIGINT 用户id\n"
            "username VARCHAR(50) 用户昵称\n"
            "city VARCHAR(20) REPLACE 用户所在城市\n"
            "age SMALLINT REPLACE 用户年龄\n"
            "sex TINYINT REPLACE 用户性别\n"
            "phone LARGEINT REPLACE 用户电话\n"
            "address VARCHAR(500) REPLACE 用户住址\n"
            "register_time DATETIME REPLACE 用户注册时间\n"
            "及建表语句：\n"
            "\n"
            "CREATE TABLE IF NOT EXISTS example_db.example_tbl\n"
            "(\n"
            "user_id LARGEINT NOT NULL COMMENT \"用户id\",\n"
            "username VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n"
            "city VARCHAR(20) REPLACE COMMENT \"用户所在城市\",\n"
            "age SMALLINT REPLACE COMMENT \"用户年龄\",\n"
            "sex TINYINT REPLACE COMMENT \"用户性别\",\n"
            "phone LARGEINT REPLACE COMMENT \"用户电话\",\n"
            "address VARCHAR(500) REPLACE COMMENT \"用户地址\",\n"
            "register_time DATETIME REPLACE COMMENT \"用户注册时间\"\n"
            ")\n"
            "AGGREGATE KEY(user_id, username)\n"
            "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
            "PROPERTIES (\n"
            "\"replication_allocation\" = \"tag.location.default: 1\"\n"
            ");\n"
            "\n"
            "即Unique 模型的读时合并实现完全可以用聚合模型中的 REPLACE 方式替代。其内部的实现方式和数据存储方式也完全一样。这里不再继续举例说明。\n"
            "\n"
            "SinceVersion 1.2\n"
            "写时合并\n"
            "Unqiue模型的写时合并实现，与聚合模型就是完全不同的两种模型了，查询性能更接近于duplicate模型，在有主键约束需求的场景上相比聚合模型有较大的查询性能优势，尤其是在聚合查询以及需要用索引过滤大量数据的查询中。\n"
            "\n"
            "在 1.2.0 版本中，作为一个新的feature，写时合并默认关闭，用户可以通过添加下面的property来开启\n"
            "\n"
            "\"enable_unique_key_merge_on_write\" = \"true\"\n"
            "\n"
            "仍然以上面的表为例，建表语句为\n"
            "\n"
            "CREATE TABLE IF NOT EXISTS example_db.example_tbl\n"
            "(\n"
            "user_id LARGEINT NOT NULL COMMENT \"用户id\",\n"
            "username VARCHAR(50) NOT NULL COMMENT \"用户昵称\",\n"
            "city VARCHAR(20) COMMENT \"用户所在城市\",\n"
            "age SMALLINT COMMENT \"用户年龄\",\n"
            "sex TINYINT COMMENT \"用户性别\",\n"
            "phone LARGEINT COMMENT \"用户电话\",\n"
            "address VARCHAR(500) COMMENT \"用户地址\",\n"
            "register_time DATETIME COMMENT \"用户注册时间\"\n"
            ")\n"
            "UNIQUE KEY(user_id, username)\n"
            "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
            "PROPERTIES (\n"
            "\"replication_allocation\" = \"tag.location.default: 1\",\n"
            "\"enable_unique_key_merge_on_write\" = \"true\"\n"
            ");\n"
            "\n"
            "使用这种建表语句建出来的表结构，与聚合模型就完全不同了：\n"
            "\n"
            "ColumnName Type AggregationType Comment\n"
            "user_id BIGINT 用户id\n"
            "username VARCHAR(50) 用户昵称\n"
            "city VARCHAR(20) NONE 用户所在城市\n"
            "age SMALLINT NONE 用户年龄\n"
            "sex TINYINT NONE 用户性别\n"
            "phone LARGEINT NONE 用户电话\n"
            "address VARCHAR(500) NONE 用户住址\n"
            "register_time DATETIME NONE 用户注册时间\n"
            "在开启了写时合并选项的Unique表上，数据在导入阶段就会去将被覆盖和被更新的数据进行标记删除，同时将新的数据写入新的文件。在查询的时候，所有被标记删除的数据都会在文件级别被过滤掉，读取出来的数据就都是最新的数据，消除掉了读时合并中的数据聚合过程，并且能够在很多情况下支持多种谓词的下推。因此在许多场景都能带来比较大的性能提升，尤其是在有聚合查询的情况下。\n"
            "\n"
            "【注意】\n"
            "\n"
            "新的Merge-on-write实现默认关闭，且只能在建表时通过指定property的方式打开。\n"
            "旧的Merge-on-read的实现无法无缝升级到新版本的实现（数据组织方式完全不同），如果需要改为使用写时合并的实现版本，需要手动执行insert into unique-mow-table select * from source table.\n"
            "在Unique模型上独有的delete sign 和 sequence col，在写时合并的新版实现中仍可以正常使用，用法没有变化。\n"
            "Duplicate 模型\n"
            "在某些多维分析场景下，数据既没有主键，也没有聚合需求。因此，我们引入 Duplicate 数据模型来满足这类需求。举例说明。\n"
            "\n"
            "ColumnName Type SortKey Comment\n"
            "timestamp DATETIME Yes 日志时间\n"
            "type INT Yes 日志类型\n"
            "error_code INT Yes 错误码\n"
            "error_msg VARCHAR(1024) No 错误详细信息\n"
            "op_id BIGINT No 负责人id\n"
            "op_time DATETIME No 处理时间\n"
            "建表语句如下：\n"
            "\n"
            "CREATE TABLE IF NOT EXISTS example_db.example_tbl\n"
            "(\n"
            "timestamp DATETIME NOT NULL COMMENT \"日志时间\",\n"
            "type INT NOT NULL COMMENT \"日志类型\",\n"
            "error_code INT COMMENT \"错误码\",\n"
            "error_msg VARCHAR(1024) COMMENT \"错误详细信息\",\n"
            "op_id BIGINT COMMENT \"负责人id\",\n"
            "op_time DATETIME COMMENT \"处理时间\"\n"
            ")\n"
            "DUPLICATE KEY(timestamp, type, error_code)\n"
            "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
            "PROPERTIES (\n"
            "\"replication_allocation\" = \"tag.location.default: 1\"\n"
            ");\n"
            "\n"
            "这种数据模型区别于 Aggregate 和 Unique 模型。数据完全按照导入文件中的数据进行存储，不会有任何聚合。即使两行数据完全相同，也都会保留。 而在建表语句中指定的 DUPLICATE KEY，只是用来指明底层数据按照那些列进行排序。（更贴切的名称应该为 “Sorted Column”，这里取名 “DUPLICATE KEY” 只是用以明确表示所用的数据模型。关于 “Sorted Column”的更多解释，可以参阅前缀索引）。在 DUPLICATE KEY 的选择上，我们建议适当的选择前 2-4 列就可以。\n"
            "\n"
            "这种数据模型适用于既没有聚合需求，又没有主键唯一性约束的原始数据的存储。更多使用场景，可参阅聚合模型的局限性小节。\n"
            "\n"
            "聚合模型的局限性\n"
            "这里我们针对 Aggregate 模型，来介绍下聚合模型的局限性。\n"
            "\n"
            "在聚合模型中，模型对外展现的，是最终聚合后的数据。也就是说，任何还未聚合的数据（比如说两个不同导入批次的数据），必须通过某种方式，以保证对外展示的一致性。我们举例说明。\n"
            "\n"
            "假设表结构如下：\n"
            "\n"
            "ColumnName Type AggregationType Comment\n"
            "user_id LARGEINT 用户id\n"
            "date DATE 数据灌入日期\n"
            "cost BIGINT SUM 用户总消费\n"
            "假设存储引擎中有如下两个已经导入完成的批次的数据：\n"
            "\n"
            "batch 1\n"
            "\n"
            "user_id date cost\n"
            "10001 2017-11-20 50\n"
            "10002 2017-11-21 39\n"
            "batch 2\n"
            "\n"
            "user_id date cost\n"
            "10001 2017-11-20 1\n"
            "10001 2017-11-21 5\n"
            "10003 2017-11-22 22\n"
            "可以看到，用户 10001 分属在两个导入批次中的数据还没有聚合。但是为了保证用户只能查询到如下最终聚合后的数据：\n"
            "\n"
            "user_id date cost\n"
            "10001 2017-11-20 51\n"
            "10001 2017-11-21 5\n"
            "10002 2017-11-21 39\n"
            "10003 2017-11-22 22\n"
            "我们在查询引擎中加入了聚合算子，来保证数据对外的一致性。\n"
            "\n"
            "另外，在聚合列（Value）上，执行与聚合类型不一致的聚合类查询时，要注意语意。比如我们在如上示例中执行如下查询：\n"
            "\n"
            "SELECT MIN(cost) FROM table;\n"
            "\n"
            "得到的结果是 5，而不是 1。\n"
            "\n"
            "同时，这种一致性保证，在某些查询中，会极大的降低查询效率。\n"
            "\n"
            "我们以最基本的 count(*) 查询为例：\n"
            "\n"
            "SELECT COUNT(*) FROM table;\n"
            "\n"
            "在其他数据库中，这类查询都会很快的返回结果。因为在实现上，我们可以通过如“导入时对行进行计数，保存 count 的统计信息”，或者在查询时“仅扫描某一列数据，获得 count 值”的方式，只需很小的开销，即可获得查询结果。但是在 Doris 的聚合模型中，这种查询的开销非常大。\n"
            "\n"
            "我们以刚才的数据为例：\n"
            "\n"
            "batch 1\n"
            "\n"
            "user_id date cost\n"
            "10001 2017-11-20 50\n"
            "10002 2017-11-21 39\n"
            "batch 2\n"
            "\n"
            "user_id date cost\n"
            "10001 2017-11-20 1\n"
            "10001 2017-11-21 5\n"
            "10003 2017-11-22 22\n"
            "因为最终的聚合结果为：\n"
            "\n"
            "user_id date cost\n"
            "10001 2017-11-20 51\n"
            "10001 2017-11-21 5\n"
            "10002 2017-11-21 39\n"
            "10003 2017-11-22 22\n"
            "所以，select count(*) from table; 的正确结果应该为 4。但如果我们只扫描 user_id 这一列，如果加上查询时聚合，最终得到的结果是 3（10001, 10002, 10003）。而如果不加查询时聚合，则得到的结果是 5（两批次一共5行数据）。可见这两个结果都是不对的。\n"
            "\n"
            "为了得到正确的结果，我们必须同时读取 user_id 和 date 这两列的数据，再加上查询时聚合，才能返回 4 这个正确的结果。也就是说，在 count() 查询中，Doris 必须扫描所有的 AGGREGATE KEY 列（这里就是 user_id 和 date），并且聚合后，才能得到语意正确的结果。当聚合列非常多时，count() 查询需要扫描大量的数据。\n"
            "\n"
            "因此，当业务上有频繁的 count() 查询时，我们建议用户通过增加一个值恒为 1 的，聚合类型为 SUM 的列来模拟 count()。如刚才的例子中的表结构，我们修改如下：\n"
            "\n"
            "ColumnName Type AggregateType Comment\n"
            "user_id BIGINT 用户id\n"
            "date DATE 数据灌入日期\n"
            "cost BIGINT SUM 用户总消费\n"
            "count BIGINT SUM 用于计算count\n"
            "增加一个 count 列，并且导入数据中，该列值恒为 1。则 select count() from table; 的结果等价于 select sum(count) from table;。而后者的查询效率将远高于前者。不过这种方式也有使用限制，就是用户需要自行保证，不会重复导入 AGGREGATE KEY 列都相同的行。否则，select sum(count) from table; 只能表述原始导入的行数，而不是 select count() from table; 的语义。\n"
            "\n"
            "另一种方式，就是 将如上的 count 列的聚合类型改为 REPLACE，且依然值恒为 1。那么 select sum(count) from table; 和 select count(*) from table; 的结果将是一致的。并且这种方式，没有导入重复行的限制。\n"
            "\n"
            "Unique模型的写时合并实现\n"
            "Unique模型的写时合并实现没有聚合模型的局限性，还是以刚才的数据为例，写时合并为每次导入的rowset增加了对应的delete bitmap，来标记哪些数据被覆盖。第一批数据导入后状态如下\n"
            "\n"
            "batch 1\n"
            "\n"
            "user_id date cost delete bit\n"
            "10001 2017-11-20 50 false\n"
            "10002 2017-11-21 39 false\n"
            "当第二批数据导入完成后，第一批数据中重复的行就会被标记为已删除，此时两批数据状态如下\n"
            "\n"
            "batch 1\n"
            "\n"
            "user_id date cost delete bit\n"
            "10001 2017-11-20 50 true\n"
            "10002 2017-11-21 39 false\n"
            "batch 2\n"
            "\n"
            "user_id date cost delete bit\n"
            "10001 2017-11-20 1 false\n"
            "10001 2017-11-21 5 false\n"
            "10003 2017-11-22 22 false\n"
            "在查询时，所有在delete bitmap中被标记删除的数据都不会读出来，因此也无需进行做任何数据聚合，上述数据中有效的行数为4行，查询出的结果也应该是4行，也就可以采取开销最小的方式来获取结果，即前面提到的“仅扫描某一列数据，获得 count 值”的方式。\n"
            "\n"
            "在测试环境中，count(*) 查询在Unique模型的写时合并实现上的性能，相比聚合模型有10倍以上的提升。\n"
            "\n"
            "Duplicate 模型\n"
            "Duplicate 模型没有聚合模型的这个局限性。因为该模型不涉及聚合语意，在做 count(*) 查询时，任意选择一列查询，即可得到语意正确的结果。\n"
            "\n"
            "key 列\n"
            "Duplicate、Aggregate、Unique 模型，都会在建表指定 key 列，然而实际上是有所区别的：对于 Duplicate 模型，表的key列，可以认为只是 “排序列”，并非起到唯一标识的作用。而 Aggregate、Unique 模型这种聚合类型的表，key 列是兼顾 “排序列” 和 “唯一标识列”，是真正意义上的“ key 列”。\n"
            "\n"
            "数据模型的选择建议\n"
            "因为数据模型在建表时就已经确定，且无法修改。所以，选择一个合适的数据模型非常重要。\n"
            "\n"
            "Aggregate 模型可以通过预聚合，极大地降低聚合查询时所需扫描的数据量和查询的计算量，非常适合有固定模式的报表类查询场景。但是该模型对 count(*) 查询很不友好。同时因为固定了 Value 列上的聚合方式，在进行其他类型的聚合查询时，需要考虑语意正确性。\n"
            "Unique 模型针对需要唯一主键约束的场景，可以保证主键唯一性约束。但是无法利用 ROLLUP 等预聚合带来的查询优势。\n"
            "对于聚合查询有较高性能需求的用户，推荐使用自1.2版本加入的写时合并实现。\n"
            "Unique 模型仅支持整行更新，如果用户既需要唯一主键约束，又需要更新部分列（例如将多张源表导入到一张 doris 表的情形），则可以考虑使用 Aggregate 模型，同时将非主键列的聚合类型设置为 REPLACE_IF_NOT_NULL。具体的用法可以参考语法手册\n"
            "Duplicate 适合任意维度的 Ad-hoc 查询。虽然同样无法利用预聚合的特性，但是不受聚合模型的约束，可以发挥列存模型的优势（只读取相关列，而不需要读取所有 Key 列）。";
    auto stringReader =
            _CLNEW lucene::util::SStringReader<char>(field_value_data, strlen(field_value_data), false);
    field->setValue(stringReader);
    w.addDocument(&doc);

    w.close();

    IndexSearcher searcher(&dir);

    lucene::util::Reader* reader = nullptr;
    std::vector<std::wstring> analyse_result;
    const char* value = "相关";
    analyzer = _CLNEW lucene::analysis::LanguageBasedAnalyzer(L"chinese", false);
    reader = _CLNEW lucene::util::SStringReader<char>(value, strlen(value), false);

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
    CuSuite *suite = CuSuiteNew(_T("CLucene chinese tokenizer Test"));

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
    SUITE_ADD_TEST(suite, testJiebaMatchHuge);
    SUITE_ADD_TEST(suite, testSimpleJiebaAllModeTokenizer);
    SUITE_ADD_TEST(suite, testSimpleJiebaDefaultModeTokenizer);
    SUITE_ADD_TEST(suite, testSimpleJiebaDefaultModeTokenizer2);
    SUITE_ADD_TEST(suite, testSimpleJiebaSearchModeTokenizer);
    SUITE_ADD_TEST(suite, testSimpleJiebaAllModeTokenizer2);
    SUITE_ADD_TEST(suite, testSimpleJiebaSearchModeTokenizer2);

    return suite;
}
// EOF
