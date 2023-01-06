#pragma once
#include <cstdint>

#include <memory>
#include <vector>

//#include "CLucene/SharedHeader.h"
#include "CLucene.h"
#include "bkd_reader.h"
#include "CLucene/util/croaring/roaring.hh"

//#include "CLucene/store/IndexInput.h"
//#include "CLucene/store/IndexOutput.h"


CL_NS_DEF2(util, bkd)
class docIds_writer {
private:
    /* data */
public:
    docIds_writer() = default;
    ~docIds_writer() = default;
    static void read_bitmap(store::IndexInput *in, Roaring &r);
    static void read_bitmap(store::IndexInput *in, bkd_reader::intersect_visitor *visitor);
    static void read_low_cardinal_bitmap(store::IndexInput *in,  bkd_docID_set_iterator* iter);
    static void read_low_cardinal_bitmap(store::IndexInput *in, bkd_reader::intersect_visitor *visitor);
    static void read_bitmap_ints(store::IndexInput *in, int32_t count, std::vector<int32_t> &docIDs);
    static void write_doc_ids_bitmap(std::vector<int32_t> &docIds, int32_t start, int32_t count, store::IndexOutput *out);
    static void write_doc_ids(std::vector<int32_t> &docIds, int32_t start, int32_t count, store::IndexOutput *out);
    static void read_ints(store::IndexInput *in, int32_t count, bkd_docID_set_iterator* iter);
    static void read_ints32(store::IndexInput *in, int32_t count, std::vector<int32_t> &docIDs);
    static void read_ints(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor);
    static void read_ints32(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor);
    static void read_ints24(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor);

private:
    static void read_delta_vints(store::IndexInput *in, int32_t count, std::vector<int32_t> &docIDs);
    static void read_ints24(store::IndexInput *in, int32_t count, std::vector<int32_t> &docIDs);
    static void read_delta_vints(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor);
};
CL_NS_END2