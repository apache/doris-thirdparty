#include <CLucene.h>

#include <iostream>
#include <unordered_map>
#include <vector>

#include "CLucene/search/TermQuery.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/util/NumericUtils.h"
#include "CLucene/util/bkd/bkd_reader.h"
#include "CLucene/util/bkd/bkd_writer.h"
#include "simdjson.h"

CL_NS_USE(util)
CL_NS_USE(store)

using namespace simdjson;

using namespace lucene::search;
using namespace lucene::index;

class TestVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    int queryMin = 0;
    int queryMax = 0;
    std::shared_ptr<lucene::util::BitSet> hits;

public:
    TestVisitor(int queryMin, int queryMax, std::shared_ptr<lucene::util::BitSet>& hits) {
        this->queryMin = queryMin;
        this->queryMax = queryMax;
        this->hits = hits;
    }

    void visit(int docID) override { hits->set(docID); }
    void visit(Roaring& docID) override {}
    void visit(Roaring&& docIDs) override {
        for (auto docID : docIDs) {
            hits->set(docID);
        }
    }
    void visit(std::vector<char>& docID, std::vector<uint8_t>& packedValue) override {
        if (!matches(packedValue.data())) {
            return;
        }
        visit(Roaring::read(docID.data(), false));
    }
    void visit(Roaring* docID, std::vector<uint8_t>& packedValue) override {
        if (!matches(packedValue.data())) {
            return;
        }
        visit(*docID);
    }
    void visit(int docID, std::vector<uint8_t>& packedValue) override {
        int x = NumericUtils::sortableBytesToInt(packedValue, 0);
        if (x >= queryMin && x <= queryMax) {
            hits->set(docID);
        }
    }
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packedValue) override {
        if (!matches(packedValue.data())) {
            return;
        }
        int32_t docID = iter->docid_set->nextDoc();
        while (docID != lucene::util::bkd::bkd_docid_set::NO_MORE_DOCS) {
            hits->set(docID);
            docID = iter->docid_set->nextDoc();
        }
    }

    bool matches(uint8_t* packedValue) {
        std::vector<uint8_t> result(4);
        std::copy(packedValue, packedValue + 4, result.begin());
        int x = NumericUtils::sortableBytesToInt(result, 0);
        if (x >= queryMin && x <= queryMax) {
            return true;
        }
        return false;
    }

    lucene::util::bkd::relation compare(std::vector<uint8_t>& minPacked,
                                        std::vector<uint8_t>& maxPacked) override {
        int min = NumericUtils::sortableBytesToInt(minPacked, 0);
        int max = NumericUtils::sortableBytesToInt(maxPacked, 0);
        assert(max >= min);
        if (max < queryMin || min > queryMax) {
            return lucene::util::bkd::relation::CELL_OUTSIDE_QUERY;
        } else if (min >= queryMin && max <= queryMax) {
            return lucene::util::bkd::relation::CELL_INSIDE_QUERY;
        } else {
            return lucene::util::bkd::relation::CELL_CROSSES_QUERY;
        }
    }
};

void indexNumeric(const std::string& path, const std::string& name,
                  const std::vector<std::string>& datas, int32_t N) {
    std::cout << "indexNumeric size: " << datas.size() << std::endl;
    std::unique_ptr<Directory> dir(FSDirectory::getDirectory(path.c_str()));
    auto w = std::make_shared<bkd::bkd_writer>(N, 1, 1, 4, 1024, 1024.0f * 10, N, true);
    w->docs_seen_ = N;

    for (int32_t docID = 0; docID < N; docID++) {
        std::vector<uint8_t> scratch(4);
        NumericUtils::intToSortableBytes(std::stoi(datas[docID % datas.size()]), scratch, 0);
        w->add(scratch.data(), scratch.size(), docID);
    }

    int64_t indexFP = 0;
    {
        std::unique_ptr<IndexOutput> out(dir->createOutput("bkd"));
        std::unique_ptr<IndexOutput> meta_out(dir->createOutput("bkd_meta"));
        std::unique_ptr<IndexOutput> index_out(dir->createOutput("bkd_index"));

        try {
            indexFP = w->finish(out.get(), index_out.get());
            w->meta_finish(meta_out.get(), indexFP, 0);
        } catch (...) {
            std::cout << "something wrong" << std::endl;
        }
    }
    dir->close();
}

int32_t main(int32_t argc, char** argv) {
    std::unordered_map<std::string, std::vector<std::string>> datas;
    {
        ondemand::parser parser;
        padded_string json = padded_string::load("/mnt/disk2/yangsiyu/data.json");
        ondemand::document tweets = parser.iterate(json);
        int32_t i = 0;
        for (auto tweet : tweets) {
            for (auto d : tweet.get_object()) {
                std::string key = (std::string)d.unescaped_key().value();
                if (key != "story_id") {
                    continue;
                }
                std::string value = (std::string)d.value().raw_json_token().value();
                datas[key].emplace_back(std::move(value));
            }
            // if (i++ == 5) {
            //     break;
            // }
        }
    }

    std::cout << "getchar" << std::endl;
    getchar();

    {
        const int32_t N = datas["story_id"].size() * 1000;
        std::string path = "/mnt/disk2/yangsiyu/index1/index2";
        indexNumeric(path, "story_id", datas["story_id"], N);

        std::unique_ptr<Directory> dir(FSDirectory::getDirectory(path.c_str()));
        IndexInput* in_(dir->openInput("bkd"));
        IndexInput* meta_in_(dir->openInput("bkd_meta"));
        IndexInput* index_in_(dir->openInput("bkd_index"));

        std::shared_ptr<bkd::bkd_reader> r = std::make_shared<bkd::bkd_reader>(in_);
        r->read_meta(meta_in_);
        r->read_index(index_in_);
        {
            constexpr int queryMin = 6000000;
            constexpr int queryMax = 6302825;
            auto hits = std::make_shared<BitSet>(N);
            auto v = std::make_unique<TestVisitor>(queryMin, queryMax, hits);

            r->intersect(v.get());

            std::cout << "numeric count: " << hits->count() << std::endl;
        }
        dir->close();

        _CLLDELETE(meta_in_);
        _CLLDELETE(index_in_);
    }
}