#pragma once

#include "CLucene/StdHeader.h"

#include <limits>
#include <memory>
#include <vector>

CL_NS_DEF2(util, bkd)

class bkd_docid_set{
public:
    static const int NO_MORE_DOCS = std::numeric_limits<int32_t>::max();

    explicit bkd_docid_set(size_t size) {
        docids.resize(size);
    }
    int32_t length() const {
        return _length;
    }
    int32_t nextDoc() {
        if (_idx == _length) {
            _docid = NO_MORE_DOCS;
        } else {
            _docid = docids[size_t(_offset + _idx)];
            _idx++;
        }
        return _docid;
    }
    void reset(int offset, int length) {
        _offset = offset;
        _length = length;
        assert(offset + length <= docids.size());
        _docid = -1;
        _idx = 0;
    }
    std::vector<int32_t> docids;

private:
    int32_t _idx{};
    int32_t _length{};
    int32_t _offset{};
    int32_t _docid{};
};

class bkd_docid_bitmap_set{
public:
    explicit bkd_docid_bitmap_set(int32_t size) {}
    ~bkd_docid_bitmap_set() = default;
    void add(std::vector<char>&& r, int pos) {
        docids[size_t(pos)] = r;
        _offset++;
    }
    void add(std::vector<char>&& r) {
        docids.emplace_back(r);
        _offset++;
    }
    void add(std::vector<char>& r) {
        docids.emplace_back(std::move(r));
        _offset++;
    }
    int32_t length() const {
        return _length;
    }
    std::vector<char>& nextDoc() {
        if (_idx == _length) {
            _docid = std::vector<char>(0);
        } else {
            _docid = docids[size_t(_offset + _idx)];
            _idx++;
        }
        return _docid;
    }

    void reset(int length) {
        _length = length;
        assert(_offset + length <= docids.size());
        _docid = std::vector<char>(0);
        _idx = 0;
    }

    void reset(int offset, int length) {
        _offset = offset;
        _length = length;
        assert(offset + length <= docids.size());
        _docid = std::vector<char>(0);
        _idx = 0;
    }
    std::vector<std::vector<char>> docids;

private:
    int32_t _idx{};
    int32_t _length{};
    int32_t _offset{};
    std::vector<char> _docid{};
};

class bkd_docid_set_iterator {
public:
    explicit bkd_docid_set_iterator(int32_t size) {
        bitmap_set = std::make_unique<bkd_docid_bitmap_set>(size);
        docid_set = std::make_unique<bkd_docid_set>(size);
    }
    std::unique_ptr<bkd_docid_bitmap_set> bitmap_set;
    std::unique_ptr<bkd_docid_set> docid_set;
    bool is_bitmap_set;
};

CL_NS_END2