#pragma once

#include <cstdint>

enum class FlagBits : uint32_t {
    DICT_COMPRESS = 1 << 0, // 00000000 00000000 00000000 00000001
};

static inline bool isFlagSet(uint32_t flags, FlagBits flag) {
    return flags & static_cast<uint32_t>(flag);
}