#include "Sorter.h"

CL_NS_DEF(util)

Sorter::Sorter() {}

void Sorter::setPivot(int i) { pivotIndex = i; }

int Sorter::comparePivot(int j) { return compare(pivotIndex, j); }

void Sorter::checkRange(int from, int to) {
    if (to < from) {
        _CLTHROWA(CL_ERR_IllegalArgument, ("'to' must be >= 'from', got from=" + to_string(from) + " and to=" + to_string(to)).c_str());
    }
}

void Sorter::mergeInPlace(int from, int mid, int to) {
    if (from == mid || mid == to || compare(mid - 1, mid) <= 0) {
        return;
    } else if (to - from == 2) {
        swap(mid - 1, mid);
        return;
    }
    while (compare(from, mid) <= 0) {
        ++from;
    }
    while (compare(mid - 1, to - 1) <= 0) {
        --to;
    }
    int first_cut, second_cut;
    int len11, len22;
    if (mid - from > to - mid) {
        len11 = static_cast<int>(static_cast<unsigned int>((mid - from)) >> 1);
        first_cut = from + len11;
        second_cut = lower(mid, to, first_cut);
        len22 = second_cut - mid;
    } else {
        len22 = static_cast<int>(static_cast<unsigned int>((to - mid)) >> 1);
        second_cut = mid + len22;
        first_cut = upper(from, mid, second_cut);
        len11 = first_cut - from;
    }
    rotate(first_cut, mid, second_cut);
    int new_mid = first_cut + len22;
    mergeInPlace(from, first_cut, new_mid);
    mergeInPlace(new_mid, second_cut, to);
}

int Sorter::lower(int from, int to, int val) {
    int len = to - from;
    while (len > 0) {
        int half = static_cast<int>(static_cast<unsigned int>(len) >> 1);
        int mid = from + half;
        if (compare(mid, val) < 0) {
            from = mid + 1;
            len = len - half - 1;
        } else {
            len = half;
        }
    }
    return from;
}

int Sorter::upper(int from, int to, int val) {
    int len = to - from;
    while (len > 0) {
        int half = static_cast<int>(static_cast<unsigned int>(len) >> 1);
        int mid = from + half;
        if (compare(val, mid) < 0) {
            len = half;
        } else {
            from = mid + 1;
            len = len - half - 1;
        }
    }
    return from;
}

int Sorter::lower2(int from, int to, int val) {
    int f = to - 1, t = to;
    while (f > from) {
        if (compare(f, val) < 0) {
            return lower(f, t, val);
        }
        int delta = t - f;
        t = f;
        f -= delta << 1;
    }
    return lower(from, t, val);
}

int Sorter::upper2(int from, int to, int val) {
    int f = from, t = f + 1;
    while (t < to) {
        if (compare(t, val) > 0) {
            return upper(f, t, val);
        }
        int delta = t - f;
        f = t;
        t += delta << 1;
    }
    return upper(f, to, val);
}

void Sorter::reverse(int from, int to) {
    for (--to; from < to; ++from, --to) {
        swap(from, to);
    }
}

void Sorter::rotate(int lo, int mid, int hi) {
    assert(lo <= mid && mid <= hi);
    if (lo == mid || mid == hi) {
        return;
    }
    doRotate(lo, mid, hi);
}

void Sorter::doRotate(int lo, int mid, int hi) {
    if (mid - lo == hi - mid) {
        // happens rarely but saves n/2 swaps
        while (mid < hi) {
            swap(lo++, mid++);
        }
    } else {
        reverse(lo, mid);
        reverse(mid, hi);
        reverse(lo, hi);
    }
}

void Sorter::binarySort(int from, int to) { binarySort(from, to, from + 1); }

void Sorter::binarySort(int from, int to, int i) {
    for (; i < to; ++i) {
        setPivot(i);
        int l = from;
        int h = i - 1;
        while (l <= h) {
            int mid =
                    static_cast<int>(static_cast<unsigned int>((l + h)) >> 1);
            int cmp = comparePivot(mid);
            if (cmp < 0) {
                h = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        for (int j = i; j > l; --j) {
            swap(j - 1, j);
        }
    }
}

void Sorter::heapSort(int from, int to) {
    if (to - from <= 1) {
        return;
    }
    heapify(from, to);
    for (int end = to - 1; end > from; --end) {
        swap(from, end);
        siftDown(from, from, end);
    }
}

void Sorter::heapify(int from, int to) {
    for (int i = heapParent(from, to - 1); i >= from; --i) {
        siftDown(i, from, to);
    }
}

void Sorter::siftDown(int i, int from, int to) {
    for (int leftChild = heapChild(from, i); leftChild < to;
         leftChild = heapChild(from, i)) {
        int rightChild = leftChild + 1;
        if (compare(i, leftChild) < 0) {
            if (rightChild < to && compare(leftChild, rightChild) < 0) {
                swap(i, rightChild);
                i = rightChild;
            } else {
                swap(i, leftChild);
                i = leftChild;
            }
        } else if (rightChild < to && compare(i, rightChild) < 0) {
            swap(i, rightChild);
            i = rightChild;
        } else {
            break;
        }
    }
}

int Sorter::heapParent(int from, int i) {
    return (static_cast<int>(static_cast<unsigned int>((i - 1 - from)) >> 1)) +
           from;
}

int Sorter::heapChild(int from, int i) { return ((i - from) << 1) + 1 + from; }

CL_NS_END