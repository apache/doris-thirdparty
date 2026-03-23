/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/impl/random_access_reader.h>

#include <cerrno>
#include <cstring>

#include <faiss/impl/FaissAssert.h>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace faiss {

/*******************************************************
 * ReadRef — default borrow() implementation
 *******************************************************/

namespace {

/// ReadRef that owns a heap-allocated buffer.
struct OwnedReadRef : ReadRef {
    std::unique_ptr<uint8_t[]> buf_;
    OwnedReadRef(std::unique_ptr<uint8_t[]> buf, size_t len)
            : buf_(std::move(buf)) {
        data_ = buf_.get();
        size_ = len;
    }
};

} // namespace

std::unique_ptr<ReadRef> RandomAccessReader::borrow(
        size_t offset,
        size_t nbytes) const {
    auto buf = std::make_unique<uint8_t[]>(nbytes);
    read_at(offset, buf.get(), nbytes);
    return std::make_unique<OwnedReadRef>(std::move(buf), nbytes);
}

/*******************************************************
 * FileRandomAccessReader — default POSIX pread backend
 *******************************************************/

FileRandomAccessReader::FileRandomAccessReader(const std::string& filename) {
#ifndef _WIN32
    fd_ = ::open(filename.c_str(), O_RDONLY);
    FAISS_THROW_IF_NOT_FMT(
            fd_ >= 0,
            "FileRandomAccessReader: cannot open %s: %s",
            filename.c_str(),
            strerror(errno));
#else
    FAISS_THROW_MSG("FileRandomAccessReader is not supported on Windows");
#endif
}

FileRandomAccessReader::~FileRandomAccessReader() {
#ifndef _WIN32
    if (fd_ >= 0) {
        ::close(fd_);
    }
#endif
}

void FileRandomAccessReader::read_at(
        size_t offset,
        void* ptr,
        size_t nbytes) const {
#ifndef _WIN32
    size_t done = 0;
    auto* out = static_cast<uint8_t*>(ptr);
    while (done < nbytes) {
        ssize_t nr = ::pread(fd_, out + done, nbytes - done, offset + done);
        FAISS_THROW_IF_NOT_MSG(
                nr >= 0, "pread failed in FileRandomAccessReader");
        FAISS_THROW_IF_NOT_MSG(
                nr > 0, "unexpected EOF in FileRandomAccessReader");
        done += static_cast<size_t>(nr);
    }
#else
    (void)offset;
    (void)ptr;
    (void)nbytes;
    FAISS_THROW_MSG("FileRandomAccessReader is not supported on Windows");
#endif
}

} // namespace faiss
