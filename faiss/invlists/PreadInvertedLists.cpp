/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/invlists/PreadInvertedLists.h>

#include <faiss/impl/FaissAssert.h>

namespace faiss {

/*******************************************************
 * PreadInvertedListsIterator
 *******************************************************/

namespace {

struct PreadInvertedListsIterator : InvertedListsIterator {
    // --- list-level invariants (set once in ctor) ---
    size_t list_size = 0;
    size_t code_size = 0;

    // --- borrowed data for the entire list ---
    std::unique_ptr<ReadRef> codes_ref_;
    std::unique_ptr<ReadRef> ids_ref_;

    // --- iteration state ---
    size_t i = 0;

    PreadInvertedListsIterator(
            const PreadInvertedLists* parent,
            size_t list_no) {
        const OnDiskOneList& l = parent->lists.at(list_no);
        if (l.size == 0) {
            return;
        }
        list_size = l.size;
        code_size = parent->code_size;
        const auto* reader = &parent->reader();

        const size_t codes_offset = l.offset;
        const size_t ids_offset = l.offset + l.capacity * code_size;

        // Borrow the entire list's codes and ids in one shot.
        // A cache-backed reader can return pinned references here,
        // giving zero-copy access on repeated queries.
        codes_ref_ = reader->borrow(codes_offset, list_size * code_size);
        ids_ref_ = reader->borrow(ids_offset, list_size * sizeof(idx_t));
    }

    // non-copyable
    PreadInvertedListsIterator(const PreadInvertedListsIterator&) = delete;
    PreadInvertedListsIterator& operator=(const PreadInvertedListsIterator&) =
            delete;

    bool is_available() const override {
        return i < list_size;
    }

    void next() override {
        ++i;
    }

    std::pair<idx_t, const uint8_t*> get_id_and_codes() override {
        auto* ids = reinterpret_cast<const idx_t*>(ids_ref_->data());
        return {ids[i], codes_ref_->data() + i * code_size};
    }
};

} // namespace

/*******************************************************
 * PreadInvertedLists
 *******************************************************/

PreadInvertedLists::PreadInvertedLists(const OnDiskInvertedLists& src)
        : ReadOnlyInvertedLists(src.nlist, src.code_size), lists(src.lists) {
    use_iterator = true;
}

PreadInvertedLists::PreadInvertedLists(
        size_t nlist,
        size_t code_size,
        std::vector<OnDiskOneList> lists)
        : ReadOnlyInvertedLists(nlist, code_size),
          lists(std::move(lists)) {
    use_iterator = true;
}

void PreadInvertedLists::set_reader(
        std::unique_ptr<RandomAccessReader> reader) {
    reader_ = std::move(reader);
}

const RandomAccessReader& PreadInvertedLists::reader() const {
    FAISS_THROW_IF_NOT_MSG(
            reader_,
            "PreadInvertedLists: reader not set, call set_reader() first");
    return *reader_;
}

size_t PreadInvertedLists::list_size(size_t list_no) const {
    return lists.at(list_no).size;
}

const uint8_t* PreadInvertedLists::get_codes(size_t list_no) const {
    const OnDiskOneList& l = lists.at(list_no);
    if (l.size == 0) {
        return nullptr;
    }
    auto* out = new uint8_t[l.size * code_size];
    FAISS_THROW_IF_NOT_MSG(
            reader_,
            "PreadInvertedLists: reader not set, call set_reader() first");
    reader_->read_at(l.offset, out, l.size * code_size);
    return out;
}

const idx_t* PreadInvertedLists::get_ids(size_t list_no) const {
    const OnDiskOneList& l = lists.at(list_no);
    if (l.size == 0) {
        return nullptr;
    }
    auto* out = new idx_t[l.size];
    FAISS_THROW_IF_NOT_MSG(
            reader_,
            "PreadInvertedLists: reader not set, call set_reader() first");
    reader_->read_at(
            l.offset + l.capacity * code_size, out, l.size * sizeof(idx_t));
    return out;
}

void PreadInvertedLists::release_codes(
        size_t /*list_no*/,
        const uint8_t* codes) const {
    delete[] codes;
}

void PreadInvertedLists::release_ids(
        size_t /*list_no*/,
        const idx_t* ids) const {
    delete[] ids;
}

bool PreadInvertedLists::is_empty(size_t list_no, void*) const {
    return lists.at(list_no).size == 0;
}

InvertedListsIterator* PreadInvertedLists::get_iterator(
        size_t list_no,
        void*) const {
    return new PreadInvertedListsIterator(this, list_no);
}

void PreadInvertedLists::prefetch_lists(
        const idx_t* /*list_nos*/,
        int /*nlist*/) const {
    // Intentionally empty.  The iterator constructor already borrows
    // the entire list in one shot, so a synchronous prefetch here
    // would just duplicate the same I/O.  If an asynchronous prefetch
    // mechanism becomes available in the future, this is the place to
    // add it.
}

/*******************************************************
 * Helper: replace OnDiskInvertedLists with PreadInvertedLists
 *******************************************************/

PreadInvertedLists* replace_with_pread_invlists(Index* index) {
    auto* ivf = dynamic_cast<IndexIVF*>(index);
    FAISS_THROW_IF_NOT_MSG(
            ivf, "replace_with_pread_invlists expects an IndexIVF");

    auto* od = dynamic_cast<OnDiskInvertedLists*>(ivf->invlists);
    FAISS_THROW_IF_NOT_MSG(
            od,
            "replace_with_pread_invlists expects IndexIVF with "
            "OnDiskInvertedLists");

    auto* pread = new PreadInvertedLists(*od);
    ivf->replace_invlists(pread, true);
    return pread;
}

} // namespace faiss
