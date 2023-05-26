#pragma once

#include "CLucene/StdHeader.h"
#include "CLucene/util/LongBitSet.h"
#include "point_writer.h"
#include <memory>
#include <vector>


CL_NS_DEF2(util, bkd)
class point_reader {
public:
    virtual ~point_reader() = default;
    virtual bool next() = 0;
    virtual const std::vector<uint8_t> &packed_value() = 0;
    virtual uint8_t* packed_value_raw() = 0;
    virtual int64_t ord() = 0;
    virtual void mark_ords(int64_t count, const std::shared_ptr<LongBitSet> &ordBitSet);
    virtual int docid() = 0;
    virtual int64_t split(int64_t count,
                          const std::shared_ptr<LongBitSet> &rightTree,
                          const std::shared_ptr<point_writer> &left,
                          const std::shared_ptr<point_writer> &right,
                          bool doClearBits);
};
CL_NS_END2
