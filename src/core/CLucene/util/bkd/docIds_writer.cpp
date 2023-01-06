#include "docIds_writer.h"

#include "CLucene/debug/error.h"

CL_NS_DEF2(util, bkd)

void docIds_writer::write_doc_ids_bitmap(std::vector<int32_t> &docIds, int32_t start, int32_t count, store::IndexOutput *out){
    Roaring r;
    for (int32_t i = 0; i < count; ++i) {
        int32_t doc = docIds[start + i];
        r.add(doc);
    }
    r.runOptimize();
    auto bitmap_size = r.getSizeInBytes(false);
    out->writeVInt(bitmap_size);
    char *bitmap = new char[bitmap_size];
    r.write(bitmap, false);
    out->writeBytes((const uint8_t*)bitmap, bitmap_size);
    delete[] bitmap;
}

void docIds_writer::write_doc_ids(std::vector<int32_t> &docIds, int32_t start, int32_t count, store::IndexOutput *out) {
    bool sorted = true;
    for (int32_t i = 1; i < count; ++i) {
        if (docIds[start + i - 1] > docIds[start + i]) {
            sorted = false;
            break;
        }
    }

    if (sorted) {
        out->writeByte((uint8_t) 0);
        int32_t previous = 0;
        for (int32_t i = 0; i < count; ++i) {
            int32_t doc = docIds[start + i];
            out->writeVInt(doc - previous);
            previous = doc;
        }
    } else {
        int64_t max = 0;
        for (int32_t i = 0; i < count; ++i) {
            max |= (static_cast<uint64_t>(docIds[start + i]));
        }

        if (max <= 0xffffff) {
            out->writeByte((uint8_t) 24);
            for (int32_t i = 0; i < count; ++i) {
                out->writeShort((short) (docIds[start + i] >> 8));
                out->writeByte((uint8_t) (docIds[start + i]));
            }
        } else {
            out->writeByte((uint8_t) 32);
            for (int32_t i = 0; i < count; ++i) {
                out->writeInt(docIds[start + i]);
            }
        }
    }
}

void docIds_writer::read_bitmap_ints(store::IndexInput *in, int32_t count, std::vector<int32_t> &docIDs) {
    auto size = in->readVInt();
    char buf[size];
    in->readBytes((uint8_t *) buf, size);
    Roaring result = Roaring::read(buf, false);
    int c = 0;
    for (auto i: result) {
        if (c <= count) {
            docIDs[c] = i;
        }
        c++;
    }
}

void docIds_writer::read_bitmap(store::IndexInput *in, Roaring &r) {
    auto size = in->readVInt();
    char buf[size];
    in->readBytes((uint8_t *) buf, size);
    r = Roaring::read(buf, false);
}

void docIds_writer::read_bitmap(store::IndexInput *in, bkd_reader::intersect_visitor *visitor){
    auto size = in->readVInt();
    char buf[size];
    in->readBytes((uint8_t *) buf, size);
    visitor->visit(Roaring::read(buf, false));
}

void docIds_writer::read_low_cardinal_bitmap(store::IndexInput *in, bkd_reader::intersect_visitor *visitor){
    auto cardinality = in->readVInt();
    std::vector<char> buf;
    for(int i = 0; i < cardinality; ++i) {
        auto size = in->readVInt();
        buf.resize(size);
        in->readBytes((uint8_t *) buf.data(), size);
        visitor->visit(Roaring::read(buf.data(), false));
    }
}

void docIds_writer::read_low_cardinal_bitmap(store::IndexInput *in,  bkd_docID_set_iterator* iter){
    auto cardinality = in->readVInt();
    //std::vector<char> buf;
    auto offset = iter->bitmap_set->docIDs.size();
    iter->bitmap_set->docIDs.resize(cardinality);

    for(int i = 0; i < cardinality; ++i) {
        auto size = in->readVInt();
        std::vector<char> buf(size);
        //buf.resize(size);
        //buf.resize(size);
        in->readBytes((uint8_t *) buf.data(), size);
        iter->bitmap_set->add(std::move(buf), i);
    }
    iter->bitmap_set->reset(0, cardinality);
}

void docIds_writer::read_ints(store::IndexInput *in, int32_t count, bkd_docID_set_iterator* iter) {
    int32_t bpv = in->readByte();
    switch (bpv) {
        case 0:
            iter->is_bitmap_set = false;
            iter->docID_set->reset(0, count);
            read_delta_vints(in, count, iter->docID_set->docIDs);
            break;
        case 1: {
            iter->is_bitmap_set = true;
            auto size = in->readVInt();
            std::vector<char> buf(size);
            in->readBytes((uint8_t *) buf.data(), size);
            auto offset = 0;//iter->bitmap_set->docIDs.size();
            iter->bitmap_set->docIDs.resize(1);

            iter->bitmap_set->add(std::move(buf), 0);
            iter->bitmap_set->reset(offset, 1);
            break;
        }
        case 2:
            iter->is_bitmap_set = true;
            read_low_cardinal_bitmap(in, iter);
            break;
        case 32:
            iter->is_bitmap_set = false;
            iter->docID_set->reset(0, count);
            read_ints32(in, count, iter->docID_set->docIDs);
            break;
        case 24:
            iter->is_bitmap_set = false;
            iter->docID_set->reset(0, count);
            read_ints24(in, count, iter->docID_set->docIDs);
            break;
        default:
            _CLTHROWA(CL_ERR_IO, ("Unsupported number of bits per value: " + std::to_string(bpv)).c_str());
    }
}

void docIds_writer::read_ints32(store::IndexInput *in, int32_t count, std::vector<int32_t> &docIDs) {
    for (int32_t i = 0; i < count; i++) {
        docIDs[i] = in->readInt();
    }
}

void docIds_writer::read_delta_vints(store::IndexInput *in, int32_t count, std::vector<int32_t> &docIDs) {
    int32_t doc = 0;
    for (int32_t i = 0; i < count; i++) {
        doc += in->readVInt();
        docIDs[i] = doc;
    }
}

void docIds_writer::read_ints24(store::IndexInput *in, int32_t count, std::vector<int32_t> &docIDs) {
    int32_t i = 0;
    for (i = 0; i < count - 7; i += 8) {
        int64_t l1 = in->readLong();
        int64_t l2 = in->readLong();
        int64_t l3 = in->readLong();
        docIDs[i] = (int) (static_cast<uint64_t>(l1) >> 40);
        docIDs[i + 1] = (int) (static_cast<uint64_t>(l1) >> 16) & 0xffffff;
        docIDs[i + 2] = (int) (((static_cast<uint64_t>(l1) & 0xffff) << 8) | (static_cast<uint64_t>(l2) >> 56));
        docIDs[i + 3] = (int) (static_cast<uint64_t>(l2) >> 32) & 0xffffff;
        docIDs[i + 4] = (int) (static_cast<uint64_t>(l2) >> 8) & 0xffffff;
        docIDs[i + 5] = (int) (((l2 & 0xff) << 16) | (static_cast<uint64_t>(l3) >> 48));
        docIDs[i + 6] = (int) (static_cast<uint64_t>(l3) >> 24) & 0xffffff;
        docIDs[i + 7] = (int) l3 & 0xffffff;
    }
    for (; i < count; ++i) {
        docIDs[i] = ((static_cast<int32_t>(in->readShort()) & 0xffff) << 8) | static_cast<uint8_t>(in->readByte());
    }
}

void docIds_writer::read_ints(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor) {
    int32_t bpv = in->readByte();
    switch (bpv) {
        case 0:
            read_delta_vints(in, count, visitor);
            break;
        case 1:
            read_bitmap(in, visitor);
            break;
        case 2:
            read_low_cardinal_bitmap(in, visitor);
            break;
        case 32:
            read_ints32(in, count, visitor);
            break;
        case 24:
            read_ints24(in, count, visitor);
            break;
        default:
            _CLTHROWA(CL_ERR_IO, ("Unsupported number of bits per value: " + std::to_string(bpv)).c_str());
    }
}

void docIds_writer::read_ints32(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor) {
    for (int32_t i = 0; i < count; i++) {
        visitor->visit(in->readInt());
    }
}

void docIds_writer::read_ints24(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor) {
    int32_t i = 0;
    for (i = 0; i < count - 7; i += 8) {
        auto l1 = static_cast<uint64_t>(in->readLong());
        auto l2 = static_cast<uint64_t>(in->readLong());
        auto l3 = static_cast<uint64_t>(in->readLong());
        visitor->visit((int) (l1 >> 40));
        visitor->visit((int) (l1 >> 16) & 0xffffff);
        visitor->visit((int) (((l1 & 0xffff) << 8) | (l2 >> 56)));
        visitor->visit((int) (l2 >> 32) & 0xffffff);
        visitor->visit((int) (l2 >> 8) & 0xffffff);
        visitor->visit((int) (((l2 & 0xff) << 16) | (l3 >> 48)));
        visitor->visit((int) (l3 >> 24) & 0xffffff);
        visitor->visit((int) l3 & 0xffffff);
    }

    for (; i < count; ++i) {
        visitor->visit(((in->readShort() & 0xffff) << 8) | (in->readByte() & 0xff));
    }
}

void docIds_writer::read_delta_vints(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor) {
    int32_t doc = 0;
    for (int32_t i = 0; i < count; i++) {
        doc += in->readVInt();
        visitor->visit(doc);
    }
}

CL_NS_END2