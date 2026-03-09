/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include <faiss/IndexIVF.h>
#include <faiss/impl/random_access_reader.h>
#include <faiss/invlists/InvertedLists.h>
#include <faiss/invlists/OnDiskInvertedLists.h>

namespace faiss {

/**
 * Read-only inverted lists backed by a pluggable RandomAccessReader.
 *
 * Unlike OnDiskInvertedLists (which requires mmap), PreadInvertedLists
 * uses positional reads (pread-style) via a RandomAccessReader, making
 * it suitable for environments where:
 * - the data lives inside a compound file (no standalone fd to mmap),
 * - a user-space cache is interposed between FAISS and storage, or
 * - the backing store is remote (object storage, HDFS, etc.).
 *
 * The list metadata layout is identical to OnDiskInvertedLists:
 *   [codes: capacity * code_size bytes] [ids: capacity * sizeof(idx_t) bytes]
 * Only the first `size` entries in each region are valid.
 *
 * The iterator path (enabled by default) borrows entire lists via
 * RandomAccessReader::borrow(), which allows zero-copy access when the
 * reader is backed by a cache or memory map.
 */
struct PreadInvertedLists : ReadOnlyInvertedLists {
    /// Per-list metadata (same layout as OnDiskInvertedLists).
    std::vector<OnDiskOneList> lists;

    /// Construct from an existing OnDiskInvertedLists, copying its metadata.
    explicit PreadInvertedLists(const OnDiskInvertedLists& src);

    /// Construct directly from list metadata.
    PreadInvertedLists(
            size_t nlist,
            size_t code_size,
            std::vector<OnDiskOneList> lists);

    /// Bind the reader that provides random access to the ivfdata content.
    /// Must be called before any read operation.
    void set_reader(std::unique_ptr<RandomAccessReader> reader);

    /// Return the bound reader (must have been set via set_reader()).
    const RandomAccessReader& reader() const;

    // ---------- InvertedLists interface ----------

    size_t list_size(size_t list_no) const override;

    const uint8_t* get_codes(size_t list_no) const override;
    const idx_t* get_ids(size_t list_no) const override;
    void release_codes(size_t list_no, const uint8_t* codes) const override;
    void release_ids(size_t list_no, const idx_t* ids) const override;

    bool is_empty(size_t list_no, void* inverted_list_context = nullptr)
            const override;
    InvertedListsIterator* get_iterator(
            size_t list_no,
            void* inverted_list_context = nullptr) const override;

    void prefetch_lists(const idx_t* list_nos, int nlist) const override;

private:
    std::unique_ptr<RandomAccessReader> reader_;
};

/**
 * Replace an IndexIVF's OnDiskInvertedLists with a PreadInvertedLists.
 *
 * The returned object has no reader yet — the caller must call
 * set_reader() before any search.
 */
FAISS_API PreadInvertedLists* replace_with_pread_invlists(Index* index);

} // namespace faiss
