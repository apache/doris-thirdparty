#pragma once
#include <deque>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

CL_NS_DEF(util)

class OfflineSorter : public std::enable_shared_from_this<OfflineSorter> {
    /** Convenience constant for megabytes */
public:
    static constexpr int64_t MB = 1024 * 1024;
    /** Convenience constant for gigabytes */
    static const int64_t GB = MB * 1024;

    /**
   * Minimum recommended buffer size for sorting.
   */
    static constexpr int64_t MIN_BUFFER_SIZE_MB = 32;

    /**
   * Absolute minimum required buffer size for sorting.
   */
    static const int64_t ABSOLUTE_MIN_SORT_BUFFER_SIZE = MB / 2;

private:
    static const std::wstring MIN_BUFFER_SIZE_MSG;

    /**
   * Maximum number of temporary files before doing an intermediate merge.
   */
public:
    static constexpr int MAX_TEMPFILES = 10;

public:
    class BufferSize final : public std::enable_shared_from_this<BufferSize> {
    public:
        const int bytes;

        explicit BufferSize(int64_t bytes) : bytes(static_cast<int>(bytes)){};

        /**
     * Creates a {@link BufferSize} in MB. The given
     * values must be &gt; 0 and &lt; 2048.
     */
    public:
        static std::shared_ptr<BufferSize> MegaBytes(int64_t mb) {
            return std::make_shared<BufferSize>(mb * MB);
        }
    };
};
CL_NS_END