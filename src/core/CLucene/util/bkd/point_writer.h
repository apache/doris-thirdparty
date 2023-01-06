#pragma once

#include <memory>
#include <cstdint>
#include <vector>

CL_NS_DEF2(util,bkd)
class point_reader;

 class point_writer {
    public:
        point_writer(/* args */)= default;;
        ~point_writer() { close(); };

        /** add a new point */
        virtual void append(const std::vector<uint8_t> &packed_value, int64_t ord, int32_t doc_id) = 0;
        virtual void append(const uint8_t* packedValue, uint32_t value_length, int64_t ord, int32_t docID) = 0;
        /** Returns a {@link PointReader} iterator to step through all previously added points */
        virtual std::shared_ptr<point_reader> get_reader(int64_t start_point, int64_t length) = 0;
        /** Returns the single shared reader, used at multiple times during the recursion, to read previously added points */
        virtual std::shared_ptr<point_reader> get_shared_reader(int64_t start_point, int64_t length, const std::vector<std::shared_ptr<point_reader>> &to_close_heroically) = 0;
        /** Removes any temp files behind this writer */
        virtual void destroy() = 0;
        virtual void close(){};
    };

CL_NS_END2