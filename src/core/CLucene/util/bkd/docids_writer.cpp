#include "CLucene/SharedHeader.h"
#include "docids_writer.h"

#include "CLucene/debug/error.h"
#include "CLucene/store/ByteArrayDataInput.h"

CL_NS_DEF2(util, bkd)

void docids_writer::write_doc_ids_bitmap(std::vector<int32_t> &docids, int32_t start, int32_t count, store::IndexOutput *out){
    roaring::Roaring r;
    for (int32_t i = 0; i < count; ++i) {
        int32_t doc = docids[start + i];
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

void docids_writer::write_doc_ids(std::vector<int32_t> &docids, int32_t start, int32_t count, store::IndexOutput *out) {
    bool sorted = true;
    for (int32_t i = 1; i < count; ++i) {
        if (docids[start + i - 1] > docids[start + i]) {
            sorted = false;
            break;
        }
    }

    if (sorted) {
        out->writeByte((uint8_t) 0);
        int32_t previous = 0;
        for (int32_t i = 0; i < count; ++i) {
            int32_t doc = docids[start + i];
            out->writeVInt(doc - previous);
            previous = doc;
        }
    } else {
        int64_t max = 0;
        for (int32_t i = 0; i < count; ++i) {
            max |= (static_cast<uint64_t>(docids[start + i]));
        }

        if (max <= 0xffffff) {
            out->writeByte((uint8_t) 24);
            for (int32_t i = 0; i < count; ++i) {
                out->writeShort((short) (docids[start + i] >> 8));
                out->writeByte((uint8_t) (docids[start + i]));
            }
        } else {
            out->writeByte((uint8_t) 32);
            for (int32_t i = 0; i < count; ++i) {
                out->writeInt(docids[start + i]);
            }
        }
    }
}

void docids_writer::read_bitmap_ints(store::IndexInput *in, int32_t count, std::vector<int32_t> &docids) {
    auto size = in->readVInt();
    char buf[size];
    in->readBytes((uint8_t *) buf, size);
    roaring::Roaring result = roaring::Roaring::read(buf, false);
    int c = 0;
    for (auto i: result) {
        if (c <= count) {
            docids[c] = i;
        }
        c++;
    }
}

void docids_writer::read_bitmap(store::IndexInput *in, roaring::Roaring &r) {
    auto size = in->readVInt();
    char buf[size];
    in->readBytes((uint8_t *) buf, size);
    r = roaring::Roaring::read(buf, false);
}

void docids_writer::read_bitmap(store::IndexInput *in, bkd_reader::intersect_visitor *visitor){
    auto size = in->readVInt();
    char buf[size];
    in->readBytes((uint8_t *) buf, size);
    visitor->visit(roaring::Roaring::read(buf, false));
}

void docids_writer::read_low_cardinal_bitmap(store::IndexInput *in, bkd_reader::intersect_visitor *visitor){
    auto cardinality = in->readVInt();
    std::vector<char> buf;
    for(int i = 0; i < cardinality; ++i) {
        auto size = in->readVInt();
        buf.resize(size);
        in->readBytes((uint8_t *) buf.data(), size);
        visitor->visit(roaring::Roaring::read(buf.data(), false));
    }
}

void docids_writer::read_low_cardinal_bitmap(store::IndexInput *in,  bkd_docid_set_iterator* iter){
    auto cardinality = in->readVInt();
    auto offset = iter->bitmap_set->docids.size();
    iter->bitmap_set->docids.resize(cardinality);

    for(int i = 0; i < cardinality; ++i) {
        auto size = in->readVInt();
        std::vector<char> buf(size);
        in->readBytes((uint8_t *) buf.data(), size);
        iter->bitmap_set->add(std::move(buf), i);
    }
    iter->bitmap_set->reset(0, cardinality);
}

void docids_writer::read_ints(store::IndexInput *in, int32_t count, bkd_docid_set_iterator* iter) {
    int32_t bpv = in->readByte();
    switch (bpv) {
        case 0:
            iter->is_bitmap_set = false;
            iter->docid_set->reset(0, count);
            read_delta_vints(in, count, iter->docid_set->docids);
            break;
        case 1: {
            iter->is_bitmap_set = true;
            auto size = in->readVInt();
            std::vector<char> buf(size);
            in->readBytes((uint8_t *) buf.data(), size);
            auto offset = 0;
            iter->bitmap_set->docids.resize(1);

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
            iter->docid_set->reset(0, count);
            read_ints32(in, count, iter->docid_set->docids);
            break;
        case 24:
            iter->is_bitmap_set = false;
            iter->docid_set->reset(0, count);
            read_ints24(in, count, iter->docid_set->docids);
            break;
        default:
            _CLTHROWA(CL_ERR_IO, ("Unsupported number of bits per value: " + std::to_string(bpv)).c_str());
    }
}

void docids_writer::read_ints32(store::IndexInput *in, int32_t count, std::vector<int32_t> &docids) {
    for (int32_t i = 0; i < count; i++) {
        docids[i] = in->readInt();
    }
}

void docids_writer::read_delta_vints(store::IndexInput *in, int32_t count, std::vector<int32_t> &docids) {
    int32_t doc = 0;
    for (int32_t i = 0; i < count; i++) {
        doc += in->readVInt();
        docids[i] = doc;
    }
}

int32_t calculateTotalBytesForDocIds(int32_t count) {
    int32_t bytesForGroups = (count / 8) * 24; // Bytes for full groups of 8 docids

    int32_t remainingDocIds = count % 8; // Number of remaining docids
    int32_t bytesForRemaining = remainingDocIds * 3; // Bytes for remaining docids, 3 bytes each

    return bytesForGroups + bytesForRemaining;
}

void docids_writer::read_ints24(store::IndexInput *in, int32_t count, std::vector<int32_t> &docids) {
    int32_t i = 0;
    auto data_size = calculateTotalBytesForDocIds(count);
    std::vector<uint8_t> packed_docids(data_size);
    in->readBytes(packed_docids.data(), data_size);
    auto in2= std::make_unique<store::ByteArrayDataInput>(packed_docids);

    for (i = 0; i < count - 7; i += 8) {
        int64_t l1 = in2->readLong();
        int64_t l2 = in2->readLong();
        int64_t l3 = in2->readLong();
        docids[i] = (int) (static_cast<uint64_t>(l1) >> 40);
        docids[i + 1] = (int) (static_cast<uint64_t>(l1) >> 16) & 0xffffff;
        docids[i + 2] = (int) (((static_cast<uint64_t>(l1) & 0xffff) << 8) | (static_cast<uint64_t>(l2) >> 56));
        docids[i + 3] = (int) (static_cast<uint64_t>(l2) >> 32) & 0xffffff;
        docids[i + 4] = (int) (static_cast<uint64_t>(l2) >> 8) & 0xffffff;
        docids[i + 5] = (int) (((l2 & 0xff) << 16) | (static_cast<uint64_t>(l3) >> 48));
        docids[i + 6] = (int) (static_cast<uint64_t>(l3) >> 24) & 0xffffff;
        docids[i + 7] = (int) l3 & 0xffffff;
    }
    for (; i < count; ++i) {
        docids[i] = ((static_cast<int32_t>(in2->readShort()) & 0xffff) << 8) | static_cast<uint8_t>(in2->readByte());
    }
}

void docids_writer::read_ints(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor) {
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

void docids_writer::read_ints32(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor) {
    for (int32_t i = 0; i < count; i++) {
        visitor->visit(in->readInt());
    }
}

void docids_writer::read_ints24(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor) {
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

void docids_writer::read_delta_vints(store::IndexInput *in, int32_t count, bkd_reader::intersect_visitor *visitor) {
    int32_t doc = 0;
    for (int32_t i = 0; i < count; i++) {
        doc += in->readVInt();
        visitor->visit(doc);
    }
}

CL_NS_END2
