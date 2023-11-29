#include "FutureArrays.h"

CL_NS_DEF(util)

FutureArrays::FutureArrays() {}// no instance

void FutureArrays::CheckFromToIndex(int fromIndex, int toIndex, int length) {
    if (fromIndex > toIndex) {
        _CLTHROWA(CL_ERR_IllegalArgument, (L"fromIndex " + std::to_wstring(fromIndex) +
                                           L" > toIndex " + std::to_wstring(toIndex))
                                                  .c_str());
    }
    if (fromIndex < 0 || toIndex > length) {
        _CLTHROWA(CL_ERR_IndexOutOfBounds, (L"Range [" + std::to_wstring(fromIndex) + L", " +
                                            std::to_wstring(toIndex) + L") out-of-bounds for length " +
                                            std::to_wstring(length))
                                                   .c_str());
    }
}

int FutureArrays::Mismatch(std::vector<uint8_t> &a, int aFromIndex, int aToIndex,
                           std::vector<uint8_t> &b, int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = std::min(aLen, bLen);
    for (int i = 0; i < len; i++) {
        if (a[i + aFromIndex] != b[i + bFromIndex]) {
            return i;
        }
    }
    return aLen == bLen ? -1 : len;
}

int FutureArrays::CompareUnsigned(const uint8_t *a, int aFromIndex,
                                  int aToIndex, const uint8_t *b,
                                  int bFromIndex, int bToIndex) {
    int len = std::min(aToIndex - aFromIndex, bToIndex - bFromIndex);
    for (int i = 0; i < len; i++) {
        int aByte = a[i + aFromIndex] & 0xFF;
        int bByte = b[i + bFromIndex] & 0xFF;
        int diff = aByte - bByte;
        if (diff != 0) {
            return diff;
        }
    }

    return (aToIndex - aFromIndex) - (bToIndex - bFromIndex);
}

int FutureArrays::CompareNumeric(const uint8_t *a,
                                 int aLen,
                                 const uint8_t *b,
                                 int bLen) {
    if (aLen != bLen) { return aLen - bLen; }
    int len = std::min(aLen, bLen);
    for (int i = len - 1; i >= 0; i--) {
        int aByte = a[i] & 0xFF;
        int bByte = b[i] & 0xFF;
        int diff = aByte - bByte;
        if (diff != 0) {
            return diff;
        }
    }

    // One is a prefix of the other, or, they are equal:
    return 0;
}
int FutureArrays::CompareUnsigned(std::vector<uint8_t> &a, int aFromIndex,
                                  int aToIndex, std::vector<uint8_t> &b,
                                  int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = std::min(aLen, bLen);
    for (int i = 0; i < len; i++) {
        int aByte = a[i + aFromIndex] & 0xFF;
        int bByte = b[i + bFromIndex] & 0xFF;
        int diff = aByte - bByte;
        if (diff != 0) {
            return diff;
        }
    }

    // One is a prefix of the other, or, they are equal:
    return aLen - bLen;
}

bool FutureArrays::Equals(std::vector<uint8_t> &a, int aFromIndex, int aToIndex,
                          std::vector<uint8_t> &b, int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
        return false;
    }
    for (int i = 0; i < aLen; i++) {
        if (a[i + aFromIndex] != b[i + bFromIndex]) {
            return false;
        }
    }
    return true;
}

int FutureArrays::Mismatch(std::vector<wchar_t> &a, int aFromIndex,
                           int aToIndex, std::vector<wchar_t> &b,
                           int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = std::min(aLen, bLen);
    for (int i = 0; i < len; i++) {
        if (a[i + aFromIndex] != b[i + bFromIndex]) {
            return i;
        }
    }
    return aLen == bLen ? -1 : len;
}

int FutureArrays::Compare(std::vector<wchar_t> &a, int aFromIndex, int aToIndex,
                          std::vector<wchar_t> &b, int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = std::min(aLen, bLen);
    for (int i = 0; i < len; i++) {
        int aInt = a[i + aFromIndex];
        int bInt = b[i + bFromIndex];
        if (aInt > bInt) {
            return 1;
        } else if (aInt < bInt) {
            return -1;
        }
    }

    // One is a prefix of the other, or, they are equal:
    return aLen - bLen;
}

bool FutureArrays::Equals(std::vector<wchar_t> &a, int aFromIndex, int aToIndex,
                          std::vector<wchar_t> &b, int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
        return false;
    }
    for (int i = 0; i < aLen; i++) {
        if (a[i + aFromIndex] != b[i + bFromIndex]) {
            return false;
        }
    }
    return true;
}

int FutureArrays::Compare(std::vector<int> &a, int aFromIndex, int aToIndex,
                          std::vector<int> &b, int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = std::min(aLen, bLen);
    for (int i = 0; i < len; i++) {
        int aInt = a[i + aFromIndex];
        int bInt = b[i + bFromIndex];
        if (aInt > bInt) {
            return 1;
        } else if (aInt < bInt) {
            return -1;
        }
    }

    // One is a prefix of the other, or, they are equal:
    return aLen - bLen;
}

bool FutureArrays::Equals(std::vector<int> &a, int aFromIndex, int aToIndex,
                          std::vector<int> &b, int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
        return false;
    }
    for (int i = 0; i < aLen; i++) {
        if (a[i + aFromIndex] != b[i + bFromIndex]) {
            return false;
        }
    }
    return true;
}

int FutureArrays::Compare(std::vector<int64_t> &a, int aFromIndex,
                          int aToIndex, std::vector<int64_t> &b,
                          int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    int len = std::min(aLen, bLen);
    for (int i = 0; i < len; i++) {
        int64_t aInt = a[i + aFromIndex];
        int64_t bInt = b[i + bFromIndex];
        if (aInt > bInt) {
            return 1;
        } else if (aInt < bInt) {
            return -1;
        }
    }

    // One is a prefix of the other, or, they are equal:
    return aLen - bLen;
}

bool FutureArrays::Equals(std::vector<int64_t> &a, int aFromIndex,
                          int aToIndex, std::vector<int64_t> &b,
                          int bFromIndex, int bToIndex) {
    CheckFromToIndex(aFromIndex, aToIndex, a.size());
    CheckFromToIndex(bFromIndex, bToIndex, b.size());
    int aLen = aToIndex - aFromIndex;
    int bLen = bToIndex - bFromIndex;
    // lengths differ: cannot be equal
    if (aLen != bLen) {
        return false;
    }
    for (int i = 0; i < aLen; i++) {
        if (a[i + aFromIndex] != b[i + bFromIndex]) {
            return false;
        }
    }
    return true;
}
CL_NS_END
