#include "point_reader.h"

#include "CLucene/debug/error.h"

#include <memory>

CL_NS_DEF2(util, bkd)
void point_reader::mark_ords(int64_t count, const std::shared_ptr<LongBitSet> &ordBitSet) {
    for (int32_t i = 0; i < count; i++) {
        bool result = next();
        if (!result) {
            _CLTHROWA(CL_ERR_InvalidState, "did not see enough points from reader");
        }
        assert(ordBitSet->Get(ord()) == false);
        ordBitSet->Set(ord());
    }
}

int64_t point_reader::split(int64_t count,
                           const std::shared_ptr<LongBitSet> &rightTree,
                           const std::shared_ptr<point_writer> &left,
                           const std::shared_ptr<point_writer> &right,
                           bool doClearBits) {
    int64_t rightCount = 0;
    for (int64_t i = 0; i < count; i++) {
        bool result = next();
        assert(result);
        const std::vector<uint8_t> &packedValue = packed_value();
        int64_t ordinal = ord();
        int32_t doc_id = docid();
        if (rightTree->Get(ordinal)) {
            right->append(packedValue, ordinal, doc_id);
            rightCount++;
            if (doClearBits) {
                rightTree->Clear(ordinal);
            }
        } else {
            left->append(packedValue, ordinal, doc_id);
        }
    }

    return rightCount;
}

CL_NS_END2
