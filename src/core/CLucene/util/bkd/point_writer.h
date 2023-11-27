#pragma once

#include <memory>
#include <cstdint>
#include <vector>

CL_NS_DEF2(util,bkd)
class point_reader;

 class point_writer {
    public:
        point_writer(/* args */)= default;;
        virtual ~point_writer() { close(); };

        virtual void append(const std::vector<uint8_t> &packed_value, int64_t ord, int32_t doc_id) = 0;
        virtual void append(const uint8_t* packedValue, uint32_t value_length, int64_t ord, int32_t docid) = 0;
        virtual std::shared_ptr<point_reader> get_reader(int64_t start_point, int64_t length) = 0;
        virtual std::shared_ptr<point_reader> get_shared_reader(int64_t start_point,
            int64_t length, const std::vector<std::shared_ptr<point_reader>> &to_close_heroically) = 0;
        virtual void destroy() = 0;
        virtual void close(){};
    };

CL_NS_END2