#include <CLucene.h>

#include <iostream>
#include <memory>
#include <vector>

#include "CLucene/debug/error.h"
#include "CLucene/index/IndexReader.h"
#include "CLucene/index/Term.h"
#include "CLucene/search/MultiPhraseQuery.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/store/RAMDirectory.h"
#include "test.h"

CL_NS_USE(util)
CL_NS_USE(store)
CL_NS_USE(search)
CL_NS_USE(index)

void testSimple1Add(CuTest* tc) {
    RAMDirectory dir;

    SimpleAnalyzer<char> analyzer;
    IndexWriter w(&dir, &analyzer, true);
    w.setUseCompoundFile(false);
    auto field_name = lucene::util::Misc::_charToWide("name");
    std::string value = "value";

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);

    auto char_string_reader = std::make_unique<lucene::util::SStringReader<char>>();
    char_string_reader->init(value.data(), value.size(), true);
    auto stream = analyzer.tokenStream(field->name(), char_string_reader.get());
    field->setValue(stream);
    doc.add(*field);

    w.addDocument(&doc);
    w.close();

    IndexSearcher index_searcher(&dir);
    {
        MultiPhraseQuery query;

        Term* t1 = _CLNEW Term(_T( "name" ), _T( "t1" ));
        query.add(t1);
        _CLLDECDELETE(t1);

        std::vector<int32_t> result;
        index_searcher._search(&query, [&result](const int32_t docid, const float_t /*score*/) {
            result.push_back(docid);
        });
        CLUCENE_ASSERT(result.size() == 0);
    }

    _CLDELETE(stream)
    _CLDELETE_ARRAY(field_name)
}

void testSimple2Add(CuTest* tc) {
    RAMDirectory dir;

    SimpleAnalyzer<char> analyzer;
    IndexWriter w(&dir, &analyzer, true);
    w.setUseCompoundFile(false);
    auto field_name = lucene::util::Misc::_charToWide("name");
    std::string value = "value";

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);

    auto char_string_reader = std::make_unique<lucene::util::SStringReader<char>>();
    char_string_reader->init(value.data(), value.size(), true);
    auto stream = analyzer.tokenStream(field->name(), char_string_reader.get());
    field->setValue(stream);
    doc.add(*field);

    w.addDocument(&doc);
    w.close();

    IndexSearcher index_searcher(&dir);
    {
        MultiPhraseQuery query;

        std::vector<Term*> terms;
        terms.push_back(_CLNEW Term(_T( "name" ), _T( "t2" )));
        terms.push_back(_CLNEW Term(_T( "name" ), _T( "t3" )));
        terms.push_back(_CLNEW Term(_T( "name" ), _T( "t4" )));
        query.add(terms);
        for (int32_t i = 0; i < terms.size(); i++) {
            _CLLDECDELETE(terms[i]);
        }

        std::vector<int32_t> result;
        index_searcher._search(&query, [&result](const int32_t docid, const float_t /*score*/) {
            result.push_back(docid);
        });
        CLUCENE_ASSERT(result.size() == 0);
    }

    _CLDELETE(stream)
    _CLDELETE_ARRAY(field_name)
}

void testMultiAdd(CuTest* tc) {
    RAMDirectory dir;

    SimpleAnalyzer<char> analyzer;
    IndexWriter w(&dir, &analyzer, true);
    w.setUseCompoundFile(false);
    auto field_name = lucene::util::Misc::_charToWide("name");
    std::string value = "value";

    Document doc;
    auto field = _CLNEW Field(field_name, Field::INDEX_TOKENIZED | Field::STORE_NO);

    auto char_string_reader = std::make_unique<lucene::util::SStringReader<char>>();
    char_string_reader->init(value.data(), value.size(), true);
    auto stream = analyzer.tokenStream(field->name(), char_string_reader.get());
    field->setValue(stream);
    doc.add(*field);

    w.addDocument(&doc);
    w.close();

    IndexSearcher index_searcher(&dir);
    {
        MultiPhraseQuery query;

        Term* t1 = _CLNEW Term(_T( "name" ), _T( "t1" ));
        query.add(t1);
        _CLLDECDELETE(t1);

        std::vector<Term*> terms;
        terms.push_back(_CLNEW Term(_T( "name" ), _T( "t2" )));
        terms.push_back(_CLNEW Term(_T( "name" ), _T( "t3" )));
        terms.push_back(_CLNEW Term(_T( "name" ), _T( "t4" )));
        query.add(terms);
        for (int32_t i = 0; i < terms.size(); i++) {
            _CLLDECDELETE(terms[i]);
        }

        std::vector<int32_t> result;
        index_searcher._search(&query, [&result](const int32_t docid, const float_t /*score*/) {
            result.push_back(docid);
        });
        CLUCENE_ASSERT(result.size() == 0);
    }

    _CLDELETE(stream)
    _CLDELETE_ARRAY(field_name)
}

CuSuite* testMultiPhraseQuery(void) {
    CuSuite* suite = CuSuiteNew(_T("CLucene MultiPhraseQuery Test"));

    SUITE_ADD_TEST(suite, testSimple1Add);
    SUITE_ADD_TEST(suite, testSimple2Add);
    SUITE_ADD_TEST(suite, testMultiAdd);

    return suite;
}