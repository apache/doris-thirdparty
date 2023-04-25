#include <CLucene.h>

#include <chrono>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "CLucene/search/TermQuery.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/util/NumericUtils.h"

CL_NS_USE(util)
CL_NS_USE(store)

using namespace lucene::search;
using namespace lucene::index;

using namespace std::chrono;

int32_t main() {
    uint64_t begin = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    IndexReader* reader = IndexReader::open("/mnt/disk2/yangsiyu/clucene/index");
    IndexSearcher index_searcher(reader);

    {
        Term* t = _CLNEW Term(_T("story_author"), _T("joedev"));
        Query* query = _CLNEW TermQuery(t);
        _CLLDECDELETE(t);

        std::vector<int32_t> result;
        index_searcher._search(query, [&result](const int32_t docid, const float_t /*score*/) {
            result.push_back(docid);
        });
        std::cout << "term count: " << result.size() << std::endl;

        _CLLDELETE(query);
    }

    // {
    //     Term* upper = _CLNEW Term(_T("story_time"), _T("1300000000"));
    //     Term* lower = _CLNEW Term(_T("story_time"), _T("1340000000"));
    //     RangeQuery* query = _CLNEW RangeQuery(upper, lower, true);
    //     _CLDECDELETE(upper);
    //     _CLDECDELETE(lower);

    //     std::vector<int32_t> result;
    //     index_searcher._search(query, [&result](const int32_t docid, const float_t /*score*/) {
    //         result.push_back(docid);
    //     });
    //     std::cout << "range count: " << result.size() << std::endl;

    //     _CLLDELETE(query);
    // }

    uint64_t end = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    std::cout << "time: " << end - begin << std::endl;

    return 0;
}