/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "test.h"
#include <memory>

void test(CuTest *tc, Reader *reader, bool verbose, int64_t bytes) {
    StandardAnalyzer analyzer;
    TokenStream *stream = analyzer.tokenStream(NULL, reader);

    uint64_t start = Misc::currentTimeMillis();

    int32_t count = 0;
    CL_NS(analysis)::Token t;
    while (stream->next(&t) != NULL) {
        if (verbose) {
            CuMessage(tc, _T("Text=%s start=%d end=%d\n"), t.termBuffer<TCHAR>(), t.startOffset(), t.endOffset());
        }
        count++;
    }

    uint64_t end = Misc::currentTimeMillis();
    int64_t time = end - start;
    CuMessageA(tc, "%d milliseconds to extract ", (int32_t) time);
    CuMessageA(tc, "%d tokens\n", count);
    CuMessageA(tc, "%f microseconds/token\n", (time * 1000.0) / count);
    CuMessageA(tc, "%f megabytes/hour\n", (bytes * 1000.0 * 60.0 * 60.0) / (time * 1000000.0));

    _CLDELETE(stream);
}

/*todo: move this to contribs because we have no filereader
  void _testFile(CuTest *tc,const char* fname, bool verbose) {
    struct cl_stat_t buf;
	  fileStat(fname,&buf);

	  int64_t bytes = buf.st_size;

    CuMessageA(tc," Reading test file containing %d bytes.\n", bytes );
	  FileReader fr (fname);
    const TCHAR *start;
    size_t total = 0;
    do {
      size_t numRead = fr.read(start, numRead);
      total += numRead;
    } while (numRead >= 0);
    test(tc,&fr, verbose, total);
    fr.close();
  }*/

void _testText(CuTest *tc, const TCHAR *text, bool verbose) {
    CuMessage(tc, _T(" Tokenizing string: %s\n"), text);
    StringReader reader(text);
    test(tc, &reader, verbose, _tcslen(text));
}

void testTokenStreamField(CuTest *tc) {
    const char *text = "This is a test 123_test";
    SStringReader<char> reader(text, strlen(text));
    SimpleAnalyzer<char> analyzer;
    TokenStream *stream = analyzer.reusableTokenStream(L"field1", &reader);

    int field_config = lucene::document::Field::STORE_NO | lucene::document::Field::INDEX_TOKENIZED;
    auto field = std::make_unique<Field>(L"field1", field_config);
    field->setValue(stream);
    auto s = field->tokenStreamValue();
    int32_t count = 0;
    CL_NS(analysis)::Token t;
    while (s->next(&t) != NULL) {
        count++;
    }
    CuAssertEquals(tc, count, 6);
    //printf("count = %d\n", count);
    //_CLDELETE(stream);
}

void testChar(CuTest *tc) {
    const char *text = "This is a test 123_test";
    std::vector<string> result{"this","is","a","test","123","test"};
    SStringReader<char> reader(text, strlen(text));
    SimpleAnalyzer<char> analyzer;
    TokenStream *stream = analyzer.tokenStream(NULL, &reader);

    int32_t count = 0;
    CL_NS(analysis)::Token t;
    while (stream->next(&t) != NULL) {
        assertEquals(true, strCompare(t.termBuffer<char>(), result.at(count).c_str()) == 0);
        count++;
    }
    //printf("count = %d\n", count);
    _CLDELETE(stream);
}

void testText(CuTest *tc) {
    _testText(tc, _T("This is a test"), true);
}
/* void testFile(CuTest *tc){
  	CuAssert(tc,_T("words.txt does not exist"),Misc::dir_Exists(CLUCENE_LOCATION "reuters-21578/feldman-cia-worldfactbook-data.txt"));
  	_testFile(tc,CLUCENE_LOCATION "reuters-21578/feldman-cia-worldfactbook-data.txt",false);
  }*/

CuSuite *testanalysis(void) {
    CuSuite *suite = CuSuiteNew(_T("CLucene Analysis Test"));

    SUITE_ADD_TEST(suite, testChar);
    SUITE_ADD_TEST(suite, testText);
    SUITE_ADD_TEST(suite, testTokenStreamField);
    return suite;
}
// EOF
