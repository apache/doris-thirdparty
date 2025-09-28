/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#define ANKERL_NANOBENCH_IMPLEMENT
#include "test.h"

#include "CLucene/document/FieldSelector.h"
#include "CLucene/index/SDocumentWriter.h"
#include <nanobench.h>
#include <memory>

//an in memory input stream for testing binary data
class MemReader : public CL_NS(util)::Reader {
    signed char *value;
    size_t len;
    int64_t pos;

public:
    MemReader(const char *value, const int32_t length = -1) {
        if (length >= 0)
            this->len = length;
        else
            this->len = strlen(value);
        this->pos = 0;
        this->value = _CL_NEWARRAY(signed char, this->len);
        memcpy(this->value, value, this->len);
    }
    virtual ~MemReader() {
        _CLDELETE_ARRAY(this->value);
    }

    int32_t read(const signed char *&start, int32_t min, int32_t max) {
        start = this->value + pos;
        int32_t r = max > min ? max : min;
        if (len - pos < r)
            r = len - pos;
        pos += r;
        return r;
    }
    int64_t position() {
        return pos;
    }
    int64_t skip(int64_t ntoskip) {
        int64_t s = ntoskip;
        if (len - pos < s)
            s = len - pos;

        this->pos += s;
        return s;
    }
    size_t size() {
        return len;
    }
};

void TestReaderValueField(CuTest *tc) {
    RAMDirectory dir;

    SimpleAnalyzer<char> analyzer;
    IndexWriter w(&dir, &analyzer, true);
    w.setUseCompoundFile(false);
    auto field_name = lucene::util::Misc::_charToWide("f3");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);

    auto char_string_reader = std::make_unique<lucene::util::SStringReader<char>>();
    char_string_reader->init("value1", 6, true);
    auto stream = analyzer.tokenStream(field->name(), char_string_reader.get());
    field->setValue(stream);
    doc.add(*field);

    w.addDocument(&doc);
    w.close();

    IndexSearcher searcher(&dir);
    auto t1 = std::make_unique<Term>(_T("f3"), _T("value1"));
    auto query1 =std::make_unique<TermQuery>(t1.get());
    Hits *hits1 = searcher.search(query1.get());
    CLUCENE_ASSERT(1 == hits1->length());
    _CLDELETE(stream)
    _CLDELETE(hits1)
    _CLDELETE_ARRAY(field_name)
}

void TestMultiSetValueField(CuTest *tc) {
    RAMDirectory dir;

    SimpleAnalyzer<char> analyzer;
    IndexWriter w(&dir, &analyzer, true);
    w.setUseCompoundFile(false);
    auto field_name = lucene::util::Misc::_charToWide("f3");

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    auto char_string_reader = std::make_unique<lucene::util::SStringReader<char>>();
    char_string_reader->init("value1", 6, false);
    auto stream = analyzer.tokenStream(field->name(), char_string_reader.get());
    field->setValue(stream);
    char_string_reader->init("value2", 6, false);
    auto stream2 = analyzer.tokenStream(field->name(), char_string_reader.get());
    field->setValue(stream2);

    doc.add(*field);

    w.addDocument(&doc);
    w.close();

    IndexSearcher searcher(&dir);
    auto t1 = std::make_unique<Term>(_T("f3"), _T("value1"));
    auto query1 = std::make_unique<TermQuery>(t1.get());
    Hits *hits1 = searcher.search(query1.get());
    CLUCENE_ASSERT(0 == hits1->length());
    auto t2 = std::make_unique<Term>(_T("f3"), _T("value2"));
    auto query2 = std::make_unique<TermQuery>(t2.get());
    Hits *hits2 = searcher.search(query2.get());
    CLUCENE_ASSERT(1 == hits2->length());

    doc.clear();
    _CLDELETE(stream)
    _CLDELETE(stream2)
    _CLDELETE(hits1)
    _CLDELETE(hits2)
    _CLDELETE_ARRAY(field_name)
}

void TestMultiAddValueField(CuTest *tc) {
    RAMDirectory dir;
    auto field_name = lucene::util::Misc::_charToWide("f3");

    SimpleAnalyzer<char> analyzer;
    IndexWriter w(&dir, &analyzer, true);
    w.setUseCompoundFile(false);

    Document doc;
    auto field1 = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    auto char_string_reader = std::make_unique<lucene::util::SStringReader<char>>();
    char_string_reader->init("value1", 6, false);
    auto stream = analyzer.tokenStream(field1->name(), char_string_reader.get());
    field1->setValue(stream);
    doc.add(*field1);

    auto field2 = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
    auto char_string_reader2 = std::make_unique<lucene::util::SStringReader<char>>();
    char_string_reader2->init("value2", 6, false);
    auto stream2 = analyzer.tokenStream(field2->name(), char_string_reader2.get());
    field2->setValue(stream2);
    doc.add(*field2);

    try {
        w.addDocument(&doc);
        w.close();
    } catch (CLuceneError& ae) {
        std::cout <<ae.what() << std::endl;
        throw ae;
    }

    auto t1 = std::make_unique<Term>(_T("f3"), _T("value1"));
    auto query1 = std::make_unique<TermQuery>(t1.get());
    IndexSearcher searcher(&dir);
    Hits *hits1 = searcher.search(query1.get());

    CLUCENE_ASSERT(1 == hits1->length());
    auto t2 = std::make_unique<Term>(_T("f3"), _T("value2"));
    auto query2 = std::make_unique<TermQuery>(t2.get());
    Hits *hits2 = searcher.search(query2.get());
    CLUCENE_ASSERT(1 == hits2->length());
    doc.removeFields(_T("f3"));
    CLUCENE_ASSERT(doc.getFields()->size() == 0);

    _CLDELETE(hits1);
    _CLDELETE(hits2);
    _CLDELETE(stream);
    _CLDELETE(stream2);
    _CLDELETE_ARRAY(field_name)
}

void TestFields(CuTest *tc) {
    Field *f = _CLNEW Field(_T("test"), _T("value"), Field::INDEX_TOKENIZED);
    CLUCENE_ASSERT(f->isIndexed() && f->isTokenized());
    CLUCENE_ASSERT(!f->isStored() && !f->isBinary() && !f->getOmitNorms());
    _CLDELETE(f);

    f = _CLNEW Field(_T("test"), _T("value"), Field::STORE_YES | Field::INDEX_NONORMS);
    //CLUCENE_ASSERT(f->isIndexed());
    //CLUCENE_ASSERT(!f->isTokenized());
    CLUCENE_ASSERT(f->getOmitNorms());
    CLUCENE_ASSERT(f->isStored() && !f->isBinary());
    _CLDELETE(f);

    Document doc;
    doc.add(*_CLNEW Field(_T("f1"), _T("value"), Field::INDEX_TOKENIZED));
    doc.add(*_CLNEW Field(_T("f2"), _T("value"), Field::INDEX_TOKENIZED));
    doc.add(*_CLNEW Field(_T("f3"), _T("value1"), Field::INDEX_TOKENIZED));
    doc.add(*_CLNEW Field(_T("f3"), _T("value2"), Field::INDEX_TOKENIZED));
    doc.add(*_CLNEW Field(_T("f4"), _T("value"), Field::INDEX_TOKENIZED));
    CLUCENE_ASSERT(doc.getFields()->size() == 5);

    _CLLDELETE(doc.fields());//just fetch the fields (to test deprecated loads)

    doc.removeField(_T("f3"));
    CLUCENE_ASSERT(doc.getFields()->size() == 4);

    //test deprecated enumerator
    DocumentFieldEnumeration *e = doc.fields();
    int count = 0;
    while (e->hasMoreElements()) {
        Field *field4 = e->nextElement();
        CLUCENE_ASSERT(field4 != NULL);
        count++;
    }
    CLUCENE_ASSERT(count == 4);
    _CLDELETE(e);

    doc.add(*_CLNEW Field(_T("f3"), _T("value3"), Field::INDEX_TOKENIZED));
    CLUCENE_ASSERT(doc.getFields()->size() == 5);

    //test deprecated enumerator
    e = doc.fields();
    count = 0;
    while (e->hasMoreElements()) {
        Field *field5 = e->nextElement();
        CLUCENE_ASSERT(field5 != NULL);
        count++;
    }
    CLUCENE_ASSERT(count == 5);
    _CLDELETE(e);

    doc.removeFields(_T("f3"));
    CLUCENE_ASSERT(doc.getFields()->size() == 3);

    doc.removeFields(_T("f4"));
    CLUCENE_ASSERT(doc.getFields()->size() == 2);

    doc.removeFields(_T("f1"));
    CLUCENE_ASSERT(doc.getFields()->size() == 1);

    doc.removeFields(_T("f2"));
    CLUCENE_ASSERT(doc.getFields()->size() == 0);
}

/*
  void TestDateTools(CuTest *tc) {
	  TCHAR* t = CL_NS(document)::DateTools::timeToString( Misc::currentTimeMillis() , CL_NS(document)::DateTools::MILLISECOND_FORMAT);
	  _CLDELETE_ARRAY(t);

	  TCHAR buf[30];
	  const TCHAR* xpt = _T("19700112102054321");
	  int64_t vv = (int64_t)987654321;
	  CL_NS(document)::DateTools::timeToString( vv , CL_NS(document)::DateTools::MILLISECOND_FORMAT, buf, 30);

	  if ( _tcscmp(buf,xpt) != 0 ) {
			CuFail(tc, _T("timeToString failed\n"), buf, xpt);
	  }
	  _CLDELETE_ARRAY(t);
  }
  */

void TestFieldSelectors(CuTest *tc) {
    RAMDirectory dir;
    const TCHAR *longStrValue = _T("too long a field...");
    {
        WhitespaceAnalyzer<TCHAR> a;
        IndexWriter w(&dir, &a, true);
        for (int i = 0; i < 3; i++) {
            Document doc;
            doc.add(*_CLNEW Field(_T("f1"), _T("value1"), Field::STORE_NO | Field::INDEX_TOKENIZED));
            doc.add(*_CLNEW Field(_T("f2"), _T("value2"), Field::STORE_NO | Field::INDEX_TOKENIZED));
            doc.add(*_CLNEW Field(_T("f3"), _T("value3"), Field::STORE_NO | Field::INDEX_TOKENIZED));
            doc.add(*_CLNEW Field(_T("f4"), _T("value4"), Field::STORE_NO | Field::INDEX_TOKENIZED));
            doc.add(*_CLNEW Field(_T("f5"), longStrValue, Field::STORE_NO | Field::INDEX_TOKENIZED));

            w.addDocument(&doc);
        }
        w.flush();
    }

    IndexReader *reader = IndexReader::open(&dir);
    MapFieldSelector fieldsToLoad;
    fieldsToLoad.add(_T("f2"), FieldSelector::LOAD);
    fieldsToLoad.add(_T("f3"), FieldSelector::LAZY_LOAD);
    fieldsToLoad.add(_T("f5"), FieldSelector::SIZE);
    Document doc;
    CLUCENE_ASSERT(reader->document(0, doc, &fieldsToLoad));
    CLUCENE_ASSERT(doc.getFields()->size() == 3);
    CuAssertStrEquals(tc, _T("check f2"), _T("value2"), doc.get(_T("f2")));
    CuAssertStrEquals(tc, _T("check f3"), _T("value3"), doc.get(_T("f3")));

    Field *byteField = doc.getField(_T("f5"));
    const ValueArray<uint8_t> &bytes = *byteField->binaryValue();
    uint32_t shouldBeInt = 2 * _tcslen(longStrValue);
    ValueArray<uint8_t> shouldBe(4);
    shouldBe[0] = (uint8_t) (shouldBeInt >> 24);
    shouldBe[1] = (uint8_t) (shouldBeInt >> 16);
    shouldBe[2] = (uint8_t) (shouldBeInt >> 8);
    shouldBe[3] = (uint8_t) shouldBeInt;
    CLUCENE_ASSERT(byteField != NULL);
    CLUCENE_ASSERT(memcmp(shouldBe.values, bytes.values, 4) == 0);

    _CLDELETE(reader);
    _CL_LDECREF(&dir);//derefence since we are on the stack...
}

void _TestDocumentWithOptions(CuTest *tc, int storeBit, FieldSelector::FieldSelectorResult fieldSelectorBit) {
    char factbook[1024];
    strcpy(factbook, clucene_data_location);
    strcat(factbook, "/reuters-21578/feldman-cia-worldfactbook-data.txt");
    CuAssert(tc, _T("Factbook file does not exist"), Misc::dir_Exists(factbook));

    Document doc;
    Field *f;
    const TCHAR *_ts, *_ts2;
    const ValueArray<uint8_t> *strm;
    RAMDirectory ram;

    const char *areaderString = "a binary field";
    const TCHAR *treaderString = _T("a string reader field");
    size_t readerStringLen = strlen(areaderString);

    SimpleAnalyzer<TCHAR> an;
    IndexWriter writer(&ram, &an, true);//no analyzer needed since we are not indexing...

    ValueArray<uint8_t> b((uint8_t *) strdup(areaderString), strlen(areaderString));
    //use binary utf8
    doc.add(*_CLNEW Field(_T("binaryField"), &b,
                          Field::TERMVECTOR_NO | storeBit | Field::INDEX_NO, true));
    writer.addDocument(&doc);
    doc.clear();

    //use reader
    doc.add(*_CLNEW Field(_T("readerField"), _CLNEW StringReader(treaderString),
                          Field::TERMVECTOR_NO | storeBit | Field::INDEX_NO));
    writer.addDocument(&doc);
    doc.clear();

    //done adding a few documents, now try and add a few more...
    //writer.optimize();

    //use big file
    doc.add(*_CLNEW Field(_T("fileField"),
                          _CLNEW FileReader(factbook, SimpleInputStreamReader::ASCII),
                          Field::TERMVECTOR_NO | storeBit | Field::INDEX_NO));
    writer.addDocument(&doc);
    doc.clear();

    //another optimise...
    //writer.optimize();
    writer.close();

    IndexReader *reader = IndexReader::open(&ram);

    MapFieldSelector fieldsToLoad;
    fieldsToLoad.add(_T("fileField"), fieldSelectorBit);
    fieldsToLoad.add(_T("readerField"), fieldSelectorBit);
    fieldsToLoad.add(_T("binaryField"), fieldSelectorBit);

    //now check binary field
    reader->document(0, doc);
    f = doc.getField(_T("binaryField"));
    strm = f->binaryValue();

    CuAssertIntEquals(tc, _T("Check binary length is correct"), readerStringLen, b.length);
    for (size_t i = 0; i < readerStringLen; i++) {
        CuAssertIntEquals(tc, _T("Check binary values are the same"), (*strm)[i], areaderString[i]);
    }
    doc.clear();

    //and check reader stream
    reader->document(1, doc);
    f = doc.getField(_T("readerField"));
    _ts = f->stringValue();
    CuAssertStrEquals(tc, _T("Check readerField length is correct"), treaderString, _ts);
    doc.clear();

    //now check the large field field
    reader->document(2, doc);
    f = doc.getField(_T("fileField"));
    _ts = f->stringValue();
    FileReader fbStream(factbook, FileReader::ASCII);

    int i = 0;
    _ts2 = NULL;
    do {
        int32_t rd = fbStream.read((const void**)&_ts2, 1, 1);
        if (rd == -1)
            break;
        CLUCENE_ASSERT(rd == 1);
        CLUCENE_ASSERT(_ts[i] == *_ts2);
        i++;
    } while (true);
    CLUCENE_ASSERT(i == _tcslen(_ts));
    doc.clear();

    reader->close();
    _CLDELETE(reader);
    _CL_LDECREF(&ram);//this is in the stack...
}


void TestBinaryDocument(CuTest *tc) {
    _TestDocumentWithOptions(tc, Field::STORE_YES, FieldSelector::LOAD);
}
void TestCompressedDocument(CuTest *tc) {
    _TestDocumentWithOptions(tc, Field::STORE_COMPRESS, FieldSelector::LOAD);
}
void TestLazyBinaryDocument(CuTest *tc) {
    _TestDocumentWithOptions(tc, Field::STORE_YES, FieldSelector::LAZY_LOAD);
}
void TestLazyCompressedDocument(CuTest *tc) {
    _TestDocumentWithOptions(tc, Field::STORE_COMPRESS, FieldSelector::LAZY_LOAD);
}

void TestSetFieldBench(CuTest *tc) {

    ankerl::nanobench::Bench().epochIterations(1).run("TestSetFieldBench", [&] {
        RAMDirectory dir;

        SimpleAnalyzer<TCHAR> analyzer;
        IndexWriter w(&dir, &analyzer, true);
        Document doc;
        auto field_name = lucene::util::Misc::_charToWide("f3");

        auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
        doc.add(*field);

        for (int i = 0; i < 10000; ++i) {
            auto value1 = lucene::util::Misc::_charToWide("value1");
            field->setValue(value1, false);
            w.addDocument(&doc);
        }
        w.close();
        doc.clear();
    });
}

string generateRandomString(int length) {
    static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
    string randomString;
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);
    for (int i = 0; i < length; ++i) {
        randomString += alphanum[dis(gen)];
    }
    return randomString;
}

const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
const int32_t MAX_BUFFER_DOCS = 100000000;
const int32_t MERGE_FACTOR = 100000000;
void TestAddDocument(CuTest *tc) {
    try {
        RAMDirectory dir;
        SimpleAnalyzer<char> sanalyzer;
        IndexWriter w(&dir, &sanalyzer, true);
        w.setUseCompoundFile(false);
        w.setMaxBufferedDocs(MAX_BUFFER_DOCS);
        w.setRAMBufferSizeMB(256);
        w.setMaxFieldLength(MAX_FIELD_LEN);
        w.setMergeFactor(MERGE_FACTOR);
        w.setDocumentWriter(_CLNEW SDocumentsWriter<char>(w.getDirectory(), &w));
        Document doc;
        auto field_name = lucene::util::Misc::_charToWide("f3");
        auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);
        doc.add(*field);

        for (int i = 0; i <= 2000000; i++) {
            std::string value1 = "value1";
            if (i > 0) value1 = generateRandomString(2000);
            auto stringReader = _CLNEW lucene::util::SStringReader<char>(
                    value1.c_str(), strlen(value1.c_str()), false);
            auto stream = sanalyzer.reusableTokenStream(field_name, stringReader);

            field->setValue(stream);
            w.addDocument(&doc, &sanalyzer);
        }
        IndexSearcher searcher(&dir);
        Term* t2 = _CLNEW Term(_T("f3"), _T("value1"));
        auto* query2 = _CLNEW TermQuery(t2);
        Hits* hits2 = searcher.search(query2);
        CLUCENE_ASSERT(1 == hits2->length());
        doc.clear();
        w.close();
    } catch (CLuceneError& ae) {
        std::cout <<ae.what() << std::endl;
        throw ae;
    }
}

void TestNewFieldBench(CuTest *tc) {

    ankerl::nanobench::Bench().epochIterations(1).run("TestNewFieldBench", [&] {
        RAMDirectory dir;
        //std::string value = "value1";
        //std::string field_name = "f3";
        //auto field_name_w = std::wstring(field_name.begin(), field_name.end());
        auto field_name = lucene::util::Misc::_charToWide("f3");

        //auto field_name_w = lucene::util::Misc::_charToWide(field_name.c_str());

        SimpleAnalyzer<TCHAR> analyzer;
        IndexWriter w(&dir, &analyzer, true);
        Document doc;
        for (int i = 0; i < 10000; ++i) {
            //wchar_t value1[value.length()];
            //lucene::util::Misc::_cpycharToWide(value.c_str(), value1, value.length());
            //auto value1 = std::wstring(value).c_str();
            auto value1 = lucene::util::Misc::_charToWide("value1");
            auto field = _CLNEW Field(field_name, value1, Field::INDEX_TOKENIZED | Field::STORE_NO, false);
            doc.add(*field);
            w.addDocument(&doc);
            doc.clear();
            //_CLDELETE_ARRAY(value1);
        }
        w.close();
        //_CLDELETE(field);
        //_CLDELETE_ARRAY(field_name_w);
        //doc.add(*);
    });
}

CuSuite *testdocument(void) {
    CuSuite *suite = CuSuiteNew(_T("CLucene Document Test"));

    SUITE_ADD_TEST(suite, TestFields);
    SUITE_ADD_TEST(suite, TestMultiSetValueField);
    SUITE_ADD_TEST(suite, TestMultiAddValueField);
    SUITE_ADD_TEST(suite, TestReaderValueField);
    //SUITE_ADD_TEST(suite, TestAddDocument);
    return suite;
}
