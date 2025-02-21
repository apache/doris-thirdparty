#ifndef CLUCENE_DICTSEGMENT_H
#define CLUCENE_DICTSEGMENT_H

#include <CLucene.h>

#include <array>
#include <memory>
#include <unordered_map>
#include <vector>

#include "CLucene/analysis/ik/core/CharacterUtil.h"
#include "Hit.h"
#include "phmap.h"

CL_NS_DEF2(analysis, ik)

class CLUCENE_EXPORT DictSegment : public std::enable_shared_from_this<DictSegment> {
private:
    static constexpr size_t ARRAY_LENGTH_LIMIT = 3;
    int32_t key_char_;
    std::vector<std::unique_ptr<DictSegment>> children_array_;
    phmap::flat_hash_map<int32_t, std::unique_ptr<DictSegment>> children_map_;

    DictSegment* lookforSegment(int32_t key_char, bool create_if_missing);
    int node_state_ {0};
    size_t store_size_ {0};

public:
    explicit DictSegment(int32_t key_char);
    ~DictSegment() = default;

    DictSegment(const DictSegment&) = delete;
    DictSegment& operator=(const DictSegment&) = delete;
    DictSegment(DictSegment&&) noexcept = default;
    DictSegment& operator=(DictSegment&&) noexcept = default;

    bool hasNextNode() const;
    Hit match(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
              size_t length);
    void match(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
               size_t length, Hit& search_hit);
    void fillSegment(const char* text);
};

CL_NS_END2
#endif //CLUCENE_DICTSEGMENT_H
