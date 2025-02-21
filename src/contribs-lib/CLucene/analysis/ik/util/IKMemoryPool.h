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
    // IKMemoryPool: A layered memory pool for QuickSortSet::Cell objects, dynamically expanding by adding new layers.
    IKMemoryPool(size_t layerSize) : layerSize_(layerSize), allocatedCount_(0) {
        if (layerSize_ == 0) {
            throw std::invalid_argument("Layer size must be greater than 0");
        }
        addLayer();
    }

    T* allocate() {
        if (freeList_ == nullptr) {
            addLayer();
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

    size_t available() const {
        return (layers_.size() * layerSize_) - allocatedCount_;
    }

    size_t poolSize() const { return layers_.size() * layerSize_; }

private:
    struct FreeNode {
        FreeNode* next = nullptr;
    };

    size_t layerSize_;
    size_t allocatedCount_;
    std::vector<std::unique_ptr<char[]>> layers_;
    FreeNode* freeList_ = nullptr;
    void addLayer() {
        auto newLayer = std::make_unique<char[]>(sizeof(T) * layerSize_);
        layers_.push_back(std::move(newLayer));

        char* layerStart = layers_.back().get();
        for (size_t i = 0; i < layerSize_; ++i) {
            char* block = layerStart + (i * sizeof(T));
            FreeNode* node = reinterpret_cast<FreeNode*>(block);
            node->next = freeList_;
            freeList_ = node;
        }
    }
};
CL_NS_END2
#endif //CLUCENE_IKMEMORYPOOL_H
