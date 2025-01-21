#ifndef CLUCENE_IKMEMORYPOOL_H
#define CLUCENE_IKMEMORYPOOL_H

#include <memory>
#include <stdexcept>
#include <vector>

#include "CLucene/_ApiHeader.h"
#include "CLucene/analysis/ik/core/QuickSortSet.h"

CL_NS_DEF2(analysis, ik)

template <typename T>
class IKMemoryPool {
public:
    IKMemoryPool(size_t poolSize) : poolSize_(poolSize), allocatedCount_(0) {
        if (poolSize_ == 0) {
            throw std::invalid_argument("Pool size must be greater than 0");
        }

        memoryBlock_ = std::make_unique<char[]>(sizeof(T) * poolSize_);

        for (size_t i = 0; i < poolSize_; ++i) {
            char* block = memoryBlock_.get() + (i * sizeof(T));
            FreeNode* node = reinterpret_cast<FreeNode*>(block);
            node->next = freeList_;
            freeList_ = node;
        }
    }

    T* allocate() {
        if (allocatedCount_ >= poolSize_) {
            throw std::bad_alloc();
        }

        FreeNode* node = freeList_;
        freeList_ = freeList_->next;
        ++allocatedCount_;
        return reinterpret_cast<T*>(node);
    }

    void deallocate(T* ptr) {
        if (!ptr) {
            return;
        }

        FreeNode* node = reinterpret_cast<FreeNode*>(ptr);
        node->next = freeList_;
        freeList_ = node;
        --allocatedCount_;
    }

    void mergeFreeList(T* externalHead, T* externalTail, size_t count) {
        if (!externalHead || !externalTail) {
            return;
        } else if (count == 1) {
            deallocate(externalHead);
            return;
        }

        FreeNode* tailNode = reinterpret_cast<FreeNode*>(externalTail);
        tailNode->next = freeList_;
        freeList_ = reinterpret_cast<FreeNode*>(externalHead);
        allocatedCount_ -= count;
    }

    size_t available() const { return poolSize_ - allocatedCount_; }

    size_t poolSize() const { return poolSize_; }

private:
    struct FreeNode {
        FreeNode* next = nullptr;
    };

    size_t poolSize_;
    size_t allocatedCount_;
    std::unique_ptr<char[]> memoryBlock_;
    FreeNode* freeList_ = nullptr;
};
CL_NS_END2
#endif //CLUCENE_IKMEMORYPOOL_H
