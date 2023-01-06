#pragma once

#include "CLucene/SharedHeader.h"
#include "CLucene/util/MSBRadixSorter.h"
#include "bkd_writer.h"
#include "heap_point_writer.h"

#include <vector>

CL_NS_DEF2(util,bkd)

class bkd_msb_radix_sorter : public MSBRadixSorter
{
private:
    bkd_writer * writer;
    heap_point_writer * heapWriter;
    //std::shared_ptr<BKDWriter> writer;
    //std::shared_ptr<HeapPointWriter> heapWriter;
    int dim = 0;

public:
    bkd_msb_radix_sorter(
            //std::shared_ptr<BKDWriter>&& writer,
            bkd_writer * writer,
            //std::shared_ptr<HeapPointWriter>& heapWriter,
            heap_point_writer * heapWriter,
            int dim, int32_t bytes);

protected:
    int byteAt(int i, int k) override;
    void swap(int i, int j) override;
protected:
    /*std::shared_ptr<BKDMSBRadixSorter> shared_from_this() {
        return std::static_pointer_cast<BKDMSBRadixSorter>(MSBRadixSorter::shared_from_this());
    }*/
};
CL_NS_END2