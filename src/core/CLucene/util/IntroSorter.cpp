#include "IntroSorter.h"

#include <math.h>

CL_NS_DEF(util)

IntroSorter::IntroSorter() {}

void IntroSorter::sort(int from, int to) {
    checkRange(from, to);
    quicksort(from, to, 2 * log2(to - from));
}

void IntroSorter::quicksort(int from, int to, int maxDepth) {
    if (to - from < BINARY_SORT_THRESHOLD) {
        binarySort(from, to);
        return;
    } else if (--maxDepth < 0) {
        heapSort(from, to);
        return;
    }

    int mid =
            static_cast<int>(static_cast<unsigned int>((from + to)) >> 1);

    if (compare(from, mid) > 0) {
        swap(from, mid);
    }

    if (compare(mid, to - 1) > 0) {
        swap(mid, to - 1);
        if (compare(from, mid) > 0) {
            swap(from, mid);
        }
    }

    int left = from + 1;
    int right = to - 2;

    setPivot(mid);
    for (;;) {
        while (comparePivot(right) < 0) {
            --right;
        }

        while (left < right && comparePivot(left) >= 0) {
            ++left;
        }

        if (left < right) {
            swap(left, right);
            --right;
        } else {
            break;
        }
    }

    quicksort(from, left + 1, maxDepth);
    quicksort(left + 1, to, maxDepth);
}

int IntroSorter::compare(int i, int j) {
    setPivot(i);
    return comparePivot(j);
}
CL_NS_END