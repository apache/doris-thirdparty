#include "CLucene/util/BitSet.h"
#include "CLucene/util/bkd/bkd_reader.h"
#include <roaring/roaring.hh>

#include <memory>
#include <vector>

class TestVisitor1 : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    int queryMin = 0;
    int queryMax = 0;
    std::shared_ptr<lucene::util::BitSet> hits;

public:
    TestVisitor1(int queryMin, int queryMax,
                 std::shared_ptr<lucene::util::BitSet> &hits);

    void visit(int docID) override;
    void visit(roaring::Roaring &docID) override;
    void visit(roaring::Roaring &&docIDs) override {
        {
            for (auto docID : docIDs) {
                //wcout << L"visit docID=" << docID << endl;
                hits->set(docID);
            }
        }
    }
    void visit(std::vector<char>& docID, std::vector<uint8_t> &packedValue) override {
        if (!matches(packedValue.data())) {
            return;
        }
        visit(roaring::Roaring::read(docID.data(), false));
    }
    void visit(roaring::Roaring *docID, std::vector<uint8_t> &packedValue) override;
    void visit(int docID, std::vector<uint8_t> &packedValue) override;
    void visit(lucene::util::bkd::bkd_docid_set_iterator *iter, std::vector<uint8_t> &packedValue) override;

    bool matches(uint8_t *packedValue);

    lucene::util::bkd::relation compare(std::vector<uint8_t> &minPacked,
                                        std::vector<uint8_t> &maxPacked) override;
};

enum predicate {
    L,
    G,
    LE,
    GE,
    EQ
};

class TestVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    const uint8_t *queryMin;
    const uint8_t *queryMax;
    //int queryMin = 0;
    //int queryMax = 0;
    lucene::util::BitSet *hits;
    //std::shared_ptr<lucene::util::BitSet> hits;
    std::shared_ptr<lucene::util::bkd::bkd_reader> reader;
    predicate pred;

public:
    TestVisitor(const uint8_t *queryMin, const uint8_t *queryMax, lucene::util::BitSet *hits, predicate p);
    virtual ~TestVisitor() = default;

    void setReader(std::shared_ptr<lucene::util::bkd::bkd_reader> &r) { reader = r; };

    void visit(int rowID) override;
    void visit(std::vector<char>& docID, std::vector<uint8_t> &packedValue) override {
        if (!matches(packedValue.data())) {
            return;
        }
        visit(roaring::Roaring::read(docID.data(), false));
    }
    void visit(roaring::Roaring &docIDs) override {
        for (auto docID: docIDs) {
            //std::wcout << L"visit docID=" << docID << endl;
            hits->set(docID);
        }
    };
    void visit(roaring::Roaring &&docIDs) override {
        for (auto docID: docIDs) {
            //std::wcout << L"visit docID=" << docID << endl;
            hits->set(docID);
        }
    };
    void visit(roaring::Roaring *docID, std::vector<uint8_t> &packedValue) override {
        if (!matches(packedValue.data())) {
            return;
        }
        visit(*docID);
    };
    void visit(lucene::util::bkd::bkd_docid_set_iterator *iter, std::vector<uint8_t> &packedValue) override {
        if (!matches(packedValue.data())) {
            return;
        }
        int32_t docID = iter->docid_set->nextDoc();
        while (docID != lucene::util::bkd::bkd_docid_set::NO_MORE_DOCS) {
            hits->set(docID);
            docID = iter->docid_set->nextDoc();
        }
    };
    bool matches(uint8_t *packedValue);

    void visit(int rowID, std::vector<uint8_t> &packedValue) override;

    lucene::util::bkd::relation compare(std::vector<uint8_t> &minPacked,
                                        std::vector<uint8_t> &maxPacked) override;
};
