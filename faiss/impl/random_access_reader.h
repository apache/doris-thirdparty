/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace faiss {

/**
 * Opaque handle returned by RandomAccessReader::borrow().
 *
 * Keeps the underlying data alive (e.g. a pinned cache entry, an mmap
 * region, or a plain heap buffer).  The data() pointer is valid for the
 * lifetime of this object.
 */
struct ReadRef {
    virtual ~ReadRef() = default;

    const uint8_t* data() const {
        return data_;
    }
    size_t size() const {
        return size_;
    }

protected:
    const uint8_t* data_ = nullptr;
    size_t size_ = 0;
};

/**
 * Abstract interface for random-access (pread-like) reads.
 *
 * Unlike IOReader (which is a sequential stream), RandomAccessReader
 * supports positional reads without internal state, making it safe
 * for concurrent use from multiple threads.
 *
 * This is the runtime data-access interface used during search to
 * fetch inverted-list data on demand, as opposed to IOReader which
 * is used for index serialization / deserialization.
 */
struct RandomAccessReader {
    virtual ~RandomAccessReader() = default;

    /**
     * Read exactly @p nbytes starting at byte @p offset into @p ptr.
     * Must throw on short read or I/O error.
     */
    virtual void read_at(size_t offset, void* ptr, size_t nbytes) const = 0;

    /**
     * Borrow a region of data and return a ReadRef that keeps it alive.
     *
     * The default implementation allocates a buffer and calls read_at().
     * Subclasses (e.g. a cache-backed reader) can override this to return
     * a direct pointer into cached / mapped memory without any copy.
     */
    virtual std::unique_ptr<ReadRef> borrow(
            size_t offset,
            size_t nbytes) const;
};

/**
 * Default RandomAccessReader backed by pread(fd) on a local file.
 * Only available on POSIX systems.
 */
struct FileRandomAccessReader : RandomAccessReader {
    explicit FileRandomAccessReader(const std::string& filename);
    ~FileRandomAccessReader() override;

    void read_at(size_t offset, void* ptr, size_t nbytes) const override;

private:
    int fd_ = -1;
};

} // namespace faiss
