#pragma once

#include "CLucene/StdHeader.h"
#include "CLucene/util/croaring/roaring.hh"

#include <limits>
#include <memory>
#include <vector>

CL_NS_DEF2(util, bkd)

class bkd_docID_set{
public:
    static const int NO_MORE_DOCS = std::numeric_limits<int32_t>::max();

    explicit bkd_docID_set(int32_t size) {
        docIDs.resize(size);
    }
    int32_t length() const {
        return _length;
    }
    int32_t nextDoc() {
        if (_idx == _length) {
            _docID = NO_MORE_DOCS;
        } else {
            _docID = docIDs[_offset + _idx];
            _idx++;
        }
        return _docID;
    }
    void reset(int offset, int length) {
        _offset = offset;
        _length = length;
        assert(offset + length <= docIDs.size());
        _docID = -1;
        _idx = 0;
    }
    std::vector<int32_t> docIDs;

private:
    int32_t _idx{};
    int32_t _length{};
    int32_t _offset{};
    int32_t _docID{};
};

class bkd_docID_bitmap_set{
public:
    explicit bkd_docID_bitmap_set(int32_t size) {
        //docIDs.resize(size);
    }
    ~bkd_docID_bitmap_set() = default;
    void add(std::vector<char>&& r, int pos) {
        docIDs[pos] = r;
        _offset++;
    }
    void add(std::vector<char>&& r) {
        docIDs.emplace_back(r);
        _offset++;
    }
    void add(std::vector<char>& r) {
        docIDs.emplace_back(std::move(r));
        _offset++;
    }
    int32_t length() const {
        return _length;
    }
    std::vector<char>& nextDoc() {
        if (_idx == _length) {
            _docID = std::vector<char>(0);
        } else {
            _docID = docIDs[_offset + _idx];
            _idx++;
        }
        return _docID;
    }

    void reset(int length) {
        _length = length;
        assert(_offset + length <= docIDs.size());
        _docID = std::vector<char>(0);
        _idx = 0;
    }

    void reset(int offset, int length) {
        _offset = offset;
        _length = length;
        assert(offset + length <= docIDs.size());
        _docID = std::vector<char>(0);
        _idx = 0;
    }
    std::vector<std::vector<char>> docIDs;

private:
    int32_t _idx{};
    int32_t _length{};
    int32_t _offset{};
    std::vector<char> _docID{};
};

class bkd_docID_set_iterator {
public:
    explicit bkd_docID_set_iterator(int32_t size) {
        bitmap_set = std::make_unique<bkd_docID_bitmap_set>(size);
        docID_set = std::make_unique<bkd_docID_set>(size);
    }
    std::unique_ptr<bkd_docID_bitmap_set> bitmap_set;
    std::unique_ptr<bkd_docID_set> docID_set;
    bool is_bitmap_set;
};

CL_NS_END2