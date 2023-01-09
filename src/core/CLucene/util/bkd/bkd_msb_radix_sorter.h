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
    heap_point_writer * heap_writer;
    int dim = 0;

public:
    bkd_msb_radix_sorter(
            bkd_writer * writer,
            heap_point_writer * heap_writer,
            int dim, int32_t bytes);

protected:
    int byteAt(int i, int k) override;
    void swap(int i, int j) override;
};
CL_NS_END2
