#include "CLucene/util/FutureArrays.h"
#include "CLucene/util/NumericUtils.h"
#include "CLucene/util/bkd/bkd_reader.h"
#include "CLucene/util/bkd/bkd_writer.h"

#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/store/IndexInput.h"
#include "TestBKD.h"

#include "test.h"
#include <utility>

CL_NS_USE(util)
CL_NS_USE(store)

TestVisitor1::TestVisitor1(
        int queryMin, int queryMax,
        shared_ptr<BitSet> &hits) {
    this->queryMin = queryMin;
    this->queryMax = queryMax;
    this->hits = hits;
}

void TestVisitor1::visit(roaring::Roaring &docIDs) {
    for (auto docID: docIDs) {
        //wcout << L"visit docID=" << docID << endl;
        hits->set(docID);
    }
}

void TestVisitor1::visit(int docID) {
    hits->set(docID);
    if (0) {
        wcout << L"visit docID=" << docID << endl;
    }
}

bool TestVisitor1::matches(uint8_t *packedValue) {
    std::vector<uint8_t> result(4);
    std::copy(packedValue, packedValue + 4, result.begin());
    int x = NumericUtils::sortableBytesToInt(result, 0);
    if (x >= queryMin && x <= queryMax) {
        return true;
    }
    return false;
}

void TestVisitor1::visit(roaring::Roaring *docID, std::vector<uint8_t> &packedValue) {
    if (!matches(packedValue.data())) {
        return;
    }
    visit(*docID);
}

void TestVisitor1::visit(bkd::bkd_docid_set_iterator *iter, std::vector<uint8_t> &packedValue) {
    if (!matches(packedValue.data())) {
        return;
    }
    int32_t docID = iter->docid_set->nextDoc();
    while (docID != lucene::util::bkd::bkd_docid_set::NO_MORE_DOCS) {
        hits->set(docID);
        docID = iter->docid_set->nextDoc();
    }
}

void TestVisitor1::visit(
        int docID, std::vector<uint8_t> &packedValue) {
    int x = NumericUtils::sortableBytesToInt(packedValue, 0);
    if (0) {
        wcout << L"visit docID=" << docID << L" x=" << x << endl;
    }
    if (x >= queryMin && x <= queryMax) {
        //wcout << L"visit docID=" << docID << L" x=" << x << endl;
        hits->set(docID);
    }
}

lucene::util::bkd::relation TestVisitor1::compare(
        std::vector<uint8_t> &minPacked, std::vector<uint8_t> &maxPacked) {
    int min = NumericUtils::sortableBytesToInt(minPacked, 0);
    int max = NumericUtils::sortableBytesToInt(maxPacked, 0);
    assert(max >= min);
    if (0) {
        wcout << L"compare: min=" << min << L" max=" << max << L" vs queryMin="
              << queryMin << L" queryMax=" << queryMax << endl;
    }

    if (max < queryMin || min > queryMax) {
        return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
    } else if (min >= queryMin && max <= queryMax) {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    }
}

TestVisitor::TestVisitor(const uint8_t *qMin, const uint8_t *qMax,
                         BitSet *h, predicate p) {
    queryMin = qMin;
    queryMax = qMax;
    hits = h;
    pred = p;
}

bool TestVisitor::matches(uint8_t *packedValue) {
    for (int dim = 0; dim < reader->num_data_dims_; dim++) {
        int offset = dim * reader->bytes_per_dim_;
        if (pred == L) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_, queryMax, offset,
                        offset + reader->bytes_per_dim_) >= 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        } else if (pred == G) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_, queryMin, offset,
                        offset + reader->bytes_per_dim_) <= 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        } else {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_, queryMin, offset,
                        offset + reader->bytes_per_dim_) < 0) {
                // Doc's value is too low, in this dimension
                return false;
            }
            if (lucene::util::FutureArrays::CompareUnsigned(
                        packedValue, offset, offset + reader->bytes_per_dim_, queryMax, offset,
                        offset + reader->bytes_per_dim_) > 0) {
                // Doc's value is too high, in this dimension
                return false;
            }
        }
    }
    return true;
}

void TestVisitor::visit(int rowID) {
    hits->set(rowID);
    if (0) {
        std::wcout << L"visit docID=" << rowID << std::endl;
    }
}

void TestVisitor::visit(int rowID, std::vector<uint8_t> &packedValue) {
    if (0) {
        int x = lucene::util::NumericUtils::sortableBytesToLong(packedValue, 0);
        std::wcout << L"visit docID=" << rowID << L" x=" << x << std::endl;
    }
    if (matches(packedValue.data())) {
        hits->set(rowID);
    }
}

lucene::util::bkd::relation TestVisitor::compare(std::vector<uint8_t> &minPacked,
                                                 std::vector<uint8_t> &maxPacked) {
    bool crosses = false;

    for (int dim = 0; dim < reader->num_data_dims_; dim++) {
        int offset = dim * reader->bytes_per_dim_;

        if (pred == L) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        minPacked.data(), offset, offset + reader->bytes_per_dim_, queryMax, offset,
                        offset + reader->bytes_per_dim_) >= 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        } else if (pred == G) {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        maxPacked.data(), offset, offset + reader->bytes_per_dim_, queryMin, offset,
                        offset + reader->bytes_per_dim_) <= 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        } else {
            if (lucene::util::FutureArrays::CompareUnsigned(
                        minPacked.data(), offset, offset + reader->bytes_per_dim_, queryMax, offset,
                        offset + reader->bytes_per_dim_) > 0 ||
                lucene::util::FutureArrays::CompareUnsigned(
                        maxPacked.data(), offset, offset + reader->bytes_per_dim_, queryMin, offset,
                        offset + reader->bytes_per_dim_) < 0) {
                return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
            }
        }

        crosses |= lucene::util::FutureArrays::CompareUnsigned(
                           minPacked.data(), offset, offset + reader->bytes_per_dim_, queryMin,
                           offset, offset + reader->bytes_per_dim_) <= 0 ||
                   lucene::util::FutureArrays::CompareUnsigned(
                           maxPacked.data(), offset, offset + reader->bytes_per_dim_, queryMax,
                           offset, offset + reader->bytes_per_dim_) >= 0;
    }

    if (crosses) {
        return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
    } else {
        return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
    }
}

Directory *getDirectory(int numPoints) {
    Directory *dir;
    if (numPoints > 100000) {
        //dir = newFSDirectory(createTempDir(L"TestBKDTree"));
    } else {
        dir = FSDirectory::getDirectory("TestBKDTree");
    }
    return dir;
}

void testSameInts1DWrite(CuTest *tc) {
    const int N = 1024 * 1024;
    Directory *dir(FSDirectory::getDirectory("TestBKDTreeSame"));
    shared_ptr<bkd::bkd_writer> w =
            make_shared<bkd::bkd_writer>(N, 1, 1, 4, 512, 100.0f, N, true);
    w->docs_seen_ = N;

    for (int docID = 0; docID < N; docID++) {
        std::vector<uint8_t> scratch(4);

        //auto x = docID / 10000;
        if (docID > 500000) {
            NumericUtils::intToSortableBytes(200, scratch, 0);

        } else {
            NumericUtils::intToSortableBytes(100, scratch, 0);
        }
        //w->Add(scratch, docID);
        w->add(scratch.data(), scratch.size(), docID);
    }

    int64_t indexFP;
    // C++ NOTE: The following 'try with resources' block is replaced by its C++
    // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.IndexOutput out =
    // dir.createOutput("bkd", org.apache.lucene.store.IOContext.DEFAULT))
    {
        std::unique_ptr<IndexOutput> out(dir->createOutput("bkd"));
        std::unique_ptr<IndexOutput> meta_out(dir->createOutput("bkd_meta"));
        std::unique_ptr<IndexOutput> index_out(dir->createOutput("bkd_index"));

        //auto metaOffset = w->MetaInit(out.get());
        try {
            indexFP = w->finish(out.get(), index_out.get());
            w->meta_finish(meta_out.get(), indexFP, 0);
        } catch (...) {
            printf("something wrong\n");
            //printf("clucene error: %s\n",r.what());
        }
    }
    dir->close();
    _CLDECDELETE(dir);
}

void testSameInts1DRead(CuTest *tc) {
    uint64_t str = Misc::currentTimeMillis();
    const int N = 1024 * 1024;
    Directory *dir(FSDirectory::getDirectory("TestBKDTreeSame"));
    //Directory *dir = new FSDirectory("TestBKDTree");
    {
        IndexInput *in_(dir->openInput("bkd"));
        IndexInput *meta_in_(dir->openInput("bkd_meta"));
        IndexInput *index_in_(dir->openInput("bkd_index"));
        shared_ptr<bkd::bkd_reader> r = make_shared<bkd::bkd_reader>(in_);

        // Simple 1D range query:
        constexpr int queryMin = 200;
        constexpr int queryMax = 200;

        //std::shared_ptr<BitSet> hits;
        auto hits = std::make_shared<BitSet>(N);
        auto v = std::make_unique<TestVisitor1>(queryMin, queryMax, hits);
        try {
            r->read_meta(meta_in_);
            //auto type = r->read_type();
            CuAssertEquals(tc, 0, r->type);
            r->read_index(index_in_);
            r->intersect(v.get());
        } catch (CLuceneError &r) {
            //printf("something wrong in read\n");
            printf("clucene error: %s\n", r.what());
        }
        for (int docID = 0; docID < N; docID++) {
            bool expected = docID >= queryMin && docID <= queryMax;
            bool actual = hits->get(N - docID - 1);
            //printf("%d %d\n",expected,actual);
            CuAssertEquals(tc, expected, actual);

            //assertEquals(L"docID=" + to_wstring(docID), expected, actual);
        }
        printf("\nFirst search time taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
        auto hits1 = std::make_shared<BitSet>(N);
        auto v1 = std::make_unique<TestVisitor1>(queryMin, queryMax, hits1);
        str = Misc::currentTimeMillis();

        r->intersect(v1.get());
        for (int docID = 0; docID < N; docID++) {
            bool expected = docID >= queryMin && docID <= queryMax;
            bool actual = hits1->get(N - docID - 1);
            //printf("%d %d\n",expected,actual);
            CuAssertEquals(tc, expected, actual);

            //assertEquals(L"docID=" + to_wstring(docID), expected, actual);
        }
        printf("\nSecond search time taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
    }
    dir->close();
    _CLDECDELETE(dir);
}

void testBug1Write(CuTest *tc) {
    const int N = 8;
    Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
    shared_ptr<bkd::bkd_writer> w =
            make_shared<bkd::bkd_writer>(N, 1, 1, 4, 4, 100.0f, N, true);
    w->docs_seen_ = N;
    std::vector<uint8_t> scratch(4);

    for (int i = 0; i < 6; i++) {
        NumericUtils::intToSortableBytes(0, scratch, 0);
        //w->Add(scratch, docID);
        w->add(scratch.data(), scratch.size(), i);
    }

    for (int i = 6; i < N; i++) {
        NumericUtils::intToSortableBytes(1, scratch, 0);
        //w->Add(scratch, docID);
        w->add(scratch.data(), scratch.size(), i);
    }

    int64_t indexFP;
    {
        std::unique_ptr<IndexOutput> out(dir->createOutput("bkd3"));
        std::unique_ptr<IndexOutput> meta_out(dir->createOutput("bkd3_meta"));
        std::unique_ptr<IndexOutput> index_out(dir->createOutput("bkd3_index"));
        try {
            indexFP = w->finish(out.get(), index_out.get());
            w->meta_finish(meta_out.get(), indexFP, 0);
        } catch (...) {
            printf("something wrong\n");
            //printf("clucene error: %s\n",r.what());
        }
    }
    dir->close();
    _CLDECDELETE(dir);
}

void testBug1Read(CuTest *tc) {
    uint64_t str = Misc::currentTimeMillis();
    Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
    {
        IndexInput *in_(dir->openInput("bkd3"));
        IndexInput *meta_in_(dir->openInput("bkd3_meta"));
        IndexInput *index_in_(dir->openInput("bkd3_index"));

        shared_ptr<bkd::bkd_reader> r = make_shared<bkd::bkd_reader>(in_);
        // Simple 1D range query:
        int value = 0;
        auto result = std::make_unique<BitSet>(10);
        std::vector<uint8_t> value_bytes(4);

        lucene::util::NumericUtils::intToSortableBytes(value, value_bytes, 0);
        const auto *max = reinterpret_cast<const uint8_t *>(value_bytes.data());
        const auto *min = reinterpret_cast<const uint8_t *>(value_bytes.data());

        auto v = std::make_unique<TestVisitor>(min, max, result.get(), EQ);
        try {
            v->setReader(r);
            r->read_meta(meta_in_);
            //auto type = r->read_type();
            CuAssertEquals(tc, 0, r->type);
            r->read_index(index_in_);
            r->intersect(v.get());
        } catch (CLuceneError &r) {
            //printf("something wrong in read\n");
            printf("clucene error: %s\n", r.what());
        }
        //printf("hits count=%d\n", result->count());
        CuAssertEquals(tc, result->count(), 6);
        //printf("\nFirst search time taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
    }
}

void testLowCardinalInts1DWrite(CuTest *tc) {
    const int N = 1024 * 1024;
    Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
    shared_ptr<bkd::bkd_writer> w =
            make_shared<bkd::bkd_writer>(N, 1, 1, 4, 512, 100.0f, N, true);
    w->docs_seen_ = N;

    for (int docID = 0; docID < N; docID++) {
        std::vector<uint8_t> scratch(4);

        NumericUtils::intToSortableBytes(docID % (1024 * 8), scratch, 0);
        //w->Add(scratch, docID);
        w->add(scratch.data(), scratch.size(), docID);
    }

    int64_t indexFP;
    // C++ NOTE: The following 'try with resources' block is replaced by its C++
    // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.IndexOutput out =
    // dir.createOutput("bkd", org.apache.lucene.store.IOContext.DEFAULT))
    {
        std::unique_ptr<IndexOutput> out(dir->createOutput("bkd2"));
        std::unique_ptr<IndexOutput> meta_out(dir->createOutput("bkd2_meta"));
        std::unique_ptr<IndexOutput> index_out(dir->createOutput("bkd2_index"));

        //auto metaOffset = w->MetaInit(out.get());
        try {
            indexFP = w->finish(out.get(), index_out.get());
            w->meta_finish(meta_out.get(), indexFP, 0);
        } catch (...) {
            printf("something wrong\n");
            //printf("clucene error: %s\n",r.what());
        }
    }
    dir->close();
    _CLDECDELETE(dir);
}

void testLowCardinalInts1DRead2(CuTest *tc) {
    uint64_t str = Misc::currentTimeMillis();
    const int N = 1024 * 1024;
    Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
    {
        IndexInput *in_(dir->openInput("bkd2"));
        IndexInput *meta_in_(dir->openInput("bkd2_meta"));
        IndexInput *index_in_(dir->openInput("bkd2_index"));

        shared_ptr<bkd::bkd_reader> r = make_shared<bkd::bkd_reader>(in_);
        // Simple 1D range query:
        constexpr int queryMin = 0;  //std::numeric_limits<int>::min();
        constexpr int queryMax = 100;//std::numeric_limits<int>::max();
        auto hits = std::make_shared<BitSet>(N);
        auto v = std::make_unique<TestVisitor1>(queryMin, queryMax, hits);
        try {
            r->read_meta(meta_in_);
            //auto type = r->read_type();
            CuAssertEquals(tc, 0, r->type);
            r->read_index(index_in_);
            r->intersect(v.get());
        } catch (CLuceneError &r) {
            //printf("something wrong in read\n");
            printf("clucene error: %s\n", r.what());
        }
        //printf("hits count=%d\n", hits->count());
        CuAssertEquals(tc, hits->count(), 12928);
        //printf("\nFirst search time taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
    }
}

void testLowCardinalInts1DRead(CuTest *tc) {
    uint64_t str = Misc::currentTimeMillis();
    const int N = 1024 * 1024;
    Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
    {
        IndexInput *in_(dir->openInput("bkd2"));
        IndexInput *meta_in_(dir->openInput("bkd2_meta"));
        IndexInput *index_in_(dir->openInput("bkd2_index"));

        shared_ptr<bkd::bkd_reader> r = make_shared<bkd::bkd_reader>(in_);
        // Simple 1D range query:
        constexpr int queryMin = 0;//std::numeric_limits<int>::min();
        constexpr int queryMax = 1;//std::numeric_limits<int>::max();
        auto hits = std::make_shared<BitSet>(N);
        auto v = std::make_unique<TestVisitor1>(queryMin, queryMax, hits);
        try {
            r->read_meta(meta_in_);
            //auto type = r->read_type();
            CuAssertEquals(tc, 0, r->type);
            r->read_index(index_in_);
            r->intersect(v.get());
        } catch (CLuceneError &r) {
            //printf("something wrong in read\n");
            printf("clucene error: %s\n", r.what());
        }
        //printf("hits count=%d\n", hits->count());
        CuAssertEquals(tc, hits->count(), 256);
        //printf("\nFirst search time taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
    }
}

void testBasicsInts1DWrite(CuTest *tc) {
    const int N = 1024 * 1024;
    Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
    shared_ptr<bkd::bkd_writer> w =
            make_shared<bkd::bkd_writer>(N, 1, 1, 4, 512, 100.0f, N, true);
    w->docs_seen_ = N;

    for (int docID = 0; docID < N; docID++) {
        std::vector<uint8_t> scratch(4);

        NumericUtils::intToSortableBytes(N - docID - 1, scratch, 0);
        //w->Add(scratch, docID);
        w->add(scratch.data(), scratch.size(), docID);
    }

    int64_t indexFP;
    // C++ NOTE: The following 'try with resources' block is replaced by its C++
    // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.IndexOutput out =
    // dir.createOutput("bkd", org.apache.lucene.store.IOContext.DEFAULT))
    {
        std::unique_ptr<IndexOutput> out(dir->createOutput("bkd"));
        std::unique_ptr<IndexOutput> meta_out(dir->createOutput("bkd_meta"));
        std::unique_ptr<IndexOutput> index_out(dir->createOutput("bkd_index"));
        //auto metaOffset = w->MetaInit(out.get());
        try {
            indexFP = w->finish(out.get(), index_out.get());
            w->meta_finish(meta_out.get(), indexFP, 0);
        } catch (...) {
            printf("something wrong\n");
            //printf("clucene error: %s\n",r.what());
        }
    }
    dir->close();
    _CLDECDELETE(dir);
}

void testBasicsInts1DRead(CuTest *tc) {
    uint64_t str = Misc::currentTimeMillis();
    const int N = 1024 * 1024;
    Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
    {
        IndexInput *in_(dir->openInput("bkd"));
        IndexInput *meta_in_(dir->openInput("bkd_meta"));
        IndexInput *index_in_(dir->openInput("bkd_index"));
        shared_ptr<bkd::bkd_reader> r = make_shared<bkd::bkd_reader>(in_);
        // Simple 1D range query:
        constexpr int queryMin = 1024;
        constexpr int queryMax = std::numeric_limits<int>::max();
        auto hits = std::make_shared<BitSet>(N);
        auto v = std::make_unique<TestVisitor1>(queryMin, queryMax, hits);
        try {
            r->read_meta(meta_in_);
            //auto type = r->read_type();
            CuAssertEquals(tc, 0, r->type);
            r->read_index(index_in_);
            r->intersect(v.get());
        } catch (CLuceneError &r) {
            //printf("something wrong in read\n");
            printf("clucene error: %s\n", r.what());
        }
        for (int docID = 0; docID < N; docID++) {
            bool expected = docID >= queryMin && docID <= queryMax;
            bool actual = hits->get(N - docID - 1);
            if (expected != actual) {
                wcout << docID << " " << expected << " " << actual;
            }
            CuAssertEquals(tc, expected, actual);

            //assertEquals(L"docID=" + to_wstring(docID), expected, actual);
        }
        //printf("\nFirst search time taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
        auto hits1 = std::make_shared<BitSet>(N);
        auto v1 = std::make_unique<TestVisitor1>(queryMin, queryMax, hits1);
        str = Misc::currentTimeMillis();

        r->intersect(v1.get());
        for (int docID = 0; docID < N; docID++) {
            bool expected = docID >= queryMin && docID <= queryMax;
            bool actual = hits1->get(N - docID - 1);
            if (expected != actual) {
                wcout << "failed to equal: " << docID << " " << expected << " " << actual;
            }
            CuAssertEquals(tc, expected, actual);
            //assertEquals(L"docID=" + to_wstring(docID), expected, actual);
        }
        //printf("\nSecond search time taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
    }
    dir->close();
    _CLDECDELETE(dir);
}

void testHttplogsRead(CuTest *tc) {
    uint64_t str = 0;//Misc::currentTimeMillis();
    const int N = 100;
    Directory *dir(FSDirectory::getDirectory("/mnt/disk1/jiangkai/workspace/bin/selectdb/output/be/storage/data/0/10356/151990979/0200000000000003834799d9978e0299cb61196d147f3ba8_0_3"));

    //Directory *dir(FSDirectory::getDirectory("/mnt/disk1/jiangkai/tmp/test_log/bkd"));
    //Directory *dir = new FSDirectory("TestBKDTree");
    {
        IndexInput *in_(dir->openInput("bkd"));
        IndexInput *meta_in_(dir->openInput("bkd_meta"));
        IndexInput *index_in_(dir->openInput("bkd_index"));

        shared_ptr<bkd::bkd_reader> r = make_shared<bkd::bkd_reader>(in_);
        std::vector<uint8_t> scratch(4);
        std::vector<uint8_t> scratch2(4);

        NumericUtils::intToSortableBytes(200, scratch, 0);
        NumericUtils::intToSortableBytes(800, scratch2, 0);
        auto result = std::make_unique<BitSet>(1000000000);

        const auto *max = reinterpret_cast<const uint8_t *>(scratch2.data());
        const auto *min = reinterpret_cast<const uint8_t *>(scratch.data());

        auto v = std::make_unique<TestVisitor>(min, max, result.get(), G);
        v->setReader(r);
        try {
            str = Misc::currentTimeMillis();
            r->read_meta(meta_in_);
            //auto type = r->read_type();
            //CuAssertEquals(tc, 0, type);
            r->read_index(index_in_);
            r->intersect(v.get());
            //printf("\ntry query result:%ld\n", r->estimate_point_count(v.get()));
            //printf("\nsearch time taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
        } catch (CLuceneError &r) {
            //printf("something wrong in read\n");
            printf("clucene error: %s\n", r.what());
        }
        //printf("result size = %d\n", result->count());
        CuAssertEquals(tc, result->count(), 8445);
        //printf("stats=%s\n", r->stats.to_string().c_str());
    }
    dir->close();
    _CLDECDELETE(dir);
}

void testBasicInts1D(CuTest *tc) {

    // C++ NOTE: The following 'try with resources' block is replaced by its C++
    // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.Directory dir =
    // getDirectory(100))
    {
        //std::shared_ptr<Directory> dir{getDirectory(10001)};
        const int N = 1024 * 1024;
        Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
        shared_ptr<bkd::bkd_writer> w =
                make_shared<bkd::bkd_writer>(N, 1, 1, 4, 512, 100.0f, N, true);

        for (int docID = 0; docID < N; docID++) {
            std::vector<uint8_t> scratch(4);

            NumericUtils::intToSortableBytes(docID, scratch, 0);
            //w->Add(scratch, docID);
            w->add(scratch.data(), scratch.size(), docID);
        }

        int64_t indexFP;
        // C++ NOTE: The following 'try with resources' block is replaced by its C++
        // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.IndexOutput out =
        // dir.createOutput("bkd", org.apache.lucene.store.IOContext.DEFAULT))
        {
            std::unique_ptr<IndexOutput> out(dir->createOutput("bkd"));
            std::unique_ptr<IndexOutput> meta_out(dir->createOutput("bkd_meta"));
            std::unique_ptr<IndexOutput> index_out(dir->createOutput("bkd_index"));

            indexFP = w->finish(out.get(), index_out.get());
            w->meta_finish(meta_out.get(), indexFP, 0);
        }

        // C++ NOTE: The following 'try with resources' block is replaced by its C++
        // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.IndexInput in =
        // dir.openInput("bkd", org.apache.lucene.store.IOContext.DEFAULT))
        {
            IndexInput *in_(dir->openInput("bkd"));
            IndexInput *meta_in_(dir->openInput("bkd_meta"));
            IndexInput *index_in_(dir->openInput("bkd_index"));
            //in_->seek(indexFP);
            shared_ptr<bkd::bkd_reader> r = make_shared<bkd::bkd_reader>(in_);

            // Simple 1D range query:
            constexpr int queryMin = 4;
            constexpr int queryMax = 8;

            //std::shared_ptr<BitSet> hits;
            auto hits = std::make_shared<BitSet>(N);
            auto v = std::make_unique<TestVisitor1>(queryMin, queryMax, hits);
            r->read_meta(meta_in_);
            //auto type = r->read_type();
            CuAssertEquals(tc, 0, r->type);
            r->read_index(index_in_);
            r->intersect(v.get());

            for (int docID = 0; docID < N; docID++) {
                bool expected = (docID >= queryMin && docID <= queryMax);
                bool actual = hits->get(docID);
                //printf("%d %d\n",expected,actual);
                CuAssertEquals(tc, expected, actual);

                //assertEquals(L"docID=" + to_wstring(docID), expected, actual);
            }
        }
        dir->close();
        _CLDECDELETE(dir);
    }
}

void testSame(CuTest *tc) {

    // C++ NOTE: The following 'try with resources' block is replaced by its C++
    // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.Directory dir =
    // getDirectory(100))
    {
        //std::shared_ptr<Directory> dir{getDirectory(10001)};
        const int N = 1024 * 1024;
        Directory *dir(FSDirectory::getDirectory("TestBKDTree"));
        shared_ptr<bkd::bkd_writer> w =
                make_shared<bkd::bkd_writer>(N, 1, 1, 4, 512, 100.0f, N, true);

        for (int docID = 0; docID < N; docID++) {
            std::vector<uint8_t> scratch(4);

            NumericUtils::intToSortableBytes(100, scratch, 0);
            //w->Add(scratch, docID);
            w->add(scratch.data(), scratch.size(), docID);
        }

        int64_t indexFP;
        // C++ NOTE: The following 'try with resources' block is replaced by its C++
        // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.IndexOutput out =
        // dir.createOutput("bkd", org.apache.lucene.store.IOContext.DEFAULT))
        {
            std::unique_ptr<IndexOutput> out(dir->createOutput("bkd"));
            std::unique_ptr<IndexOutput> meta_out(dir->createOutput("bkd_meta"));
            std::unique_ptr<IndexOutput> index_out(dir->createOutput("bkd_index"));
            indexFP = w->finish(out.get(), index_out.get());
            w->meta_finish(meta_out.get(), indexFP, 0);
        }

        // C++ NOTE: The following 'try with resources' block is replaced by its C++
        // equivalent: ORIGINAL LINE: try (org.apache.lucene.store.IndexInput in =
        // dir.openInput("bkd", org.apache.lucene.store.IOContext.DEFAULT))
        {
            IndexInput *in_(dir->openInput("bkd"));
            IndexInput *meta_in_(dir->openInput("bkd_meta"));
            IndexInput *index_in_(dir->openInput("bkd_index"));
            //in_->seek(indexFP);
            shared_ptr<bkd::bkd_reader> r = make_shared<bkd::bkd_reader>(in_);

            // Simple 1D range query:
            constexpr int queryMin = 100;
            constexpr int queryMax = 100;

            //std::shared_ptr<BitSet> hits;
            auto hits = std::make_shared<BitSet>(N);
            auto v = std::make_unique<TestVisitor1>(queryMin, queryMax, hits);
            r->read_meta(meta_in_);
            //auto type = r->read_type();
            CuAssertEquals(tc, 0, r->type);
            r->read_index(index_in_);
            r->intersect(v.get());

            for (int docID = 0; docID < N; docID++) {
                bool expected = (100 >= queryMin && 100 <= queryMax);
                bool actual = hits->get(docID);
                if (expected != actual) {
                    wcout << "failed to equal: " << docID << " " << expected << " " << actual;
                }
                CuAssertEquals(tc, expected, actual);
                //assertEquals(L"docID=" + to_wstring(docID), expected, actual);
            }
        }
        dir->close();
        _CLDECDELETE(dir);
    }
}

void equal_predicate(std::shared_ptr<lucene::util::bkd::bkd_reader> r) {
    long value = 20220324090000;
    auto result = std::make_unique<BitSet>(r->point_count_);

    const auto *max = reinterpret_cast<const uint8_t *>(&value);
    const auto *min = reinterpret_cast<const uint8_t *>(&value);

    auto v = std::make_unique<TestVisitor>(min, max, result.get(), EQ);
    v->setReader(r);
    r->intersect(v.get());
    printf("count: %d\n", result->count());
}

void less_equal_predicate(std::shared_ptr<lucene::util::bkd::bkd_reader> r) {
    try {
        long value = 20220427000000;
        auto result = std::make_unique<BitSet>(r->point_count_);
        std::vector<uint8_t> min(r->bytes_per_dim_);

        switch (r->bytes_per_dim_) {
            case 4:
                lucene::util::NumericUtils::intToSortableBytes(std::numeric_limits<int>::min(), min, 0);
            case 8:
                lucene::util::NumericUtils::longToSortableBytes(std::numeric_limits<long>::min(), min, 0);
            case 16:
                //TODO: need longlongToSortableBytes
                lucene::util::NumericUtils::longToSortableBytes(std::numeric_limits<long>::min(), min, 0);
        }
        const auto *max = reinterpret_cast<const uint8_t *>(&value);

        auto v = std::make_unique<TestVisitor>(min.data(), max, result.get(), LE);
        v->setReader(r);
        r->intersect(v.get());
        printf("\ncount: %d\n", result->count());
    } catch (...) {
        printf("something wrong\n");
        //printf("clucene error: %s\n",r.what());
    }
}

void less_predicate(std::shared_ptr<lucene::util::bkd::bkd_reader> r) {
    long value = 20220427000000;
    auto result = std::make_unique<BitSet>(r->point_count_);
    std::vector<uint8_t> min(r->bytes_per_dim_);

    switch (r->bytes_per_dim_) {
        case 4:
            lucene::util::NumericUtils::intToSortableBytes(std::numeric_limits<int>::min(), min, 0);
        case 8:
            lucene::util::NumericUtils::longToSortableBytes(std::numeric_limits<long>::min(), min, 0);
        case 16:
            //TODO: need longlongToSortableBytes
            lucene::util::NumericUtils::longToSortableBytes(std::numeric_limits<long>::min(), min, 0);
    }
    const auto *max = reinterpret_cast<const uint8_t *>(&value);

    auto v = std::make_unique<TestVisitor>(min.data(), max, result.get(), L);
    v->setReader(r);
    r->intersect(v.get());
    printf("count: %d\n", result->count());
}

void greater_equal_predicate(std::shared_ptr<lucene::util::bkd::bkd_reader> r) {
    long value = 20220427000000;
    auto result = std::make_unique<BitSet>(r->point_count_);
    std::vector<uint8_t> max(r->bytes_per_dim_);

    switch (r->bytes_per_dim_) {
        case 4:
            lucene::util::NumericUtils::intToSortableBytes(std::numeric_limits<int>::max(), max, 0);
        case 8:
            lucene::util::NumericUtils::longToSortableBytes(std::numeric_limits<long>::max(), max, 0);
        case 16:
            //TODO: need longlongToSortableBytes
            lucene::util::NumericUtils::longToSortableBytes(std::numeric_limits<long>::max(), max, 0);
    }
    const auto *min = reinterpret_cast<const uint8_t *>(&value);

    auto v = std::make_unique<TestVisitor>(min, max.data(), result.get(), GE);
    v->setReader(r);
    r->intersect(v.get());
    printf("count: %d\n", result->count());
}

void greater_predicate(std::shared_ptr<lucene::util::bkd::bkd_reader> r) {
    long value = 20220324090000;
    auto result = std::make_unique<BitSet>(r->point_count_);
    std::vector<uint8_t> max(r->bytes_per_dim_);

    switch (r->bytes_per_dim_) {
        case 4:
            lucene::util::NumericUtils::intToSortableBytes(std::numeric_limits<int>::max(), max, 0);
        case 8:
            lucene::util::NumericUtils::longToSortableBytes(std::numeric_limits<long>::max(), max, 0);
        case 16:
            //TODO: need longlongToSortableBytes
            lucene::util::NumericUtils::longToSortableBytes(std::numeric_limits<long>::max(), max, 0);
    }
    const auto *min = reinterpret_cast<const uint8_t *>(&value);

    auto v = std::make_unique<TestVisitor>(min, max.data(), result.get(), G);
    v->setReader(r);
    r->intersect(v.get());
    printf("count: %d\n", result->count());
}

std::shared_ptr<lucene::util::bkd::bkd_reader> testBKDReadInit(const std::string &index_full_path) {
    lucene::store::Directory *dir =
            lucene::store::FSDirectory::getDirectory(index_full_path.c_str());

    lucene::store::IndexInput *in_(dir->openInput("bkd"));
    std::unique_ptr<lucene::store::IndexInput> meta_in_(dir->openInput("bkd_meta"));

    auto indexFP = meta_in_->readVLong();
    in_->seek(indexFP);
    std::shared_ptr<lucene::util::bkd::bkd_reader> r =
            make_shared<lucene::util::bkd::bkd_reader>(in_);
    dir->close();
    _CLDECDELETE(dir);
    return r;
}

void testBKDRead(CuTest *tc) {
    try {
        printf("Location of files indexed: ");
        char ndx[250] = "";

        char *tmp = fgets(ndx, 250, stdin);
        if (tmp == nullptr) return;
        ndx[strlen(ndx) - 1] = 0;
        printf("Location to store the clucene index: %s", ndx);

        //IndexFiles(files,ndx,true);
        vector<string> files;
        std::sort(files.begin(), files.end());
        Misc::listDirs(ndx, files, true);
        auto itr = files.begin();
        while (itr != files.end()) {
            if (!IndexReader::indexExists(itr->c_str())) {
                vector<string> in_files;

                Misc::listDirs(itr->c_str(), in_files, true);
                for (auto &file: in_files) {
                    //printf("file %s:%d\n", file.c_str(), IndexReader::indexExists(file.c_str()));
                    if (IndexReader::indexExists(file.c_str())) {
                        auto r = testBKDReadInit(file);
                        //equal_predicate(r);
                        //greater_predicate(r);
                        //greater_equal_predicate(r);
                        less_equal_predicate(r);
                        //less_predicate(r);
                    }
                }

                //printf("directory %s:%d\n", itr->c_str(), IndexReader::indexExists(itr->c_str()));
                itr++;
                continue;
            }
            itr++;
        }
    } catch (...) {
        printf("Exception\n");
    }
}

void testBKDWrite(CuTest *tc) {
    testBasicsInts1DWrite(tc);
    testBasicsInts1DRead(tc);
    //testSameInts1DWrite(tc);
    //testSameInts1DRead(tc);
    //testBasicInts1D(tc);
    //testSame(tc);
}

void testBKDWriteSame(CuTest *tc) {
    //testBasicInts1D(tc);
    testSame(tc);
}

void testBKDWriteLowCardinality(CuTest *tc) {
    testLowCardinalInts1DWrite(tc);
    testLowCardinalInts1DRead(tc);
    testLowCardinalInts1DRead2(tc);
}

void testBKDBug1(CuTest *tc) {
    testBug1Write(tc);
    testBug1Read(tc);
}

CuSuite *testBKD() {
    CuSuite *suite = CuSuiteNew(_T("CLucene BKD Test"));

    SUITE_ADD_TEST(suite, testBKDWrite);
    SUITE_ADD_TEST(suite, testBKDWriteSame);
    //SUITE_ADD_TEST(suite, testHttplogsRead);
    SUITE_ADD_TEST(suite, testBKDWriteLowCardinality);
    SUITE_ADD_TEST(suite, testBKDBug1);
    return suite;
}
