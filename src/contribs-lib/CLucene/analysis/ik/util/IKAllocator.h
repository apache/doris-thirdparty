#ifndef CLUCENE_IKALLOCATOR_H
#define CLUCENE_IKALLOCATOR_H
#include <cstddef>
#include <new>
#include <vector>

#include "CLucene/_ApiHeader.h"

CL_NS_DEF2(analysis, ik)

// Custom memory allocator that manages memory in blocks for improved performance
template <typename T, size_t BlockSize = 16>
class IKAllocator {
public:
    typedef T value_type;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    // Rebind allocator to another type
    template <typename U>
    struct rebind {
        typedef IKAllocator<U, BlockSize> other;
    };

private:
    // Node structure for linked list of free nodes
    struct alignas(std::max_align_t) Node {
        alignas(T) char data[sizeof(T)];
        Node* next;
    };

    // Pre-allocated memory pool size
    static constexpr size_t POOL_SIZE = BlockSize;
    alignas(Node) char initial_pool_[sizeof(Node) * POOL_SIZE];
    bool initialized_ = false;
    // Head of the free list
    Node* free_list_head_;
    // Vector to keep track of allocated blocks
    std::vector<Node*> blocks_;

public:
    // Constructor initializes the allocator and the memory pool
    IKAllocator() : free_list_head_(nullptr) {
        blocks_.reserve(64);
        initializePool();
    }

    // Copy constructor
    IKAllocator(const IKAllocator&) : free_list_head_(nullptr) { initializePool(); }

    // Template copy constructor for different types
    template <typename U>
    IKAllocator(const IKAllocator<U, BlockSize>&) : free_list_head_(nullptr) {
        initializePool();
    }

    // Destructor to free allocated memory blocks
    ~IKAllocator() {
        for (Node* block : blocks_) {
            if (block >= reinterpret_cast<Node*>(initial_pool_) &&
                block < reinterpret_cast<Node*>(initial_pool_ + sizeof(initial_pool_))) {
                // Skip the initial pool memory
                continue;
            }
            // Free the allocated block
            ::operator delete(block);
        }
    }

private:
    // Initialize the memory pool with free nodes
    void initializePool() {
        if (initialized_) return;

        Node* nodes = reinterpret_cast<Node*>(initial_pool_);
        for (size_t i = 0; i < POOL_SIZE - 1; ++i) {
            nodes[i].next = &nodes[i + 1]; // Link nodes together
        }
        nodes[POOL_SIZE - 1].next = nullptr; // Last node points to null
        free_list_head_ = nodes;             // Set the head of the free list

        blocks_.push_back(nodes); // Add the initial pool to the blocks
        initialized_ = true;      // Mark the pool as initialized
    }

    // Allocate a new block of memory
    Node* allocateBlock() {
        Node* block = static_cast<Node*>(::operator new(sizeof(Node) * BlockSize));
        blocks_.push_back(block); // Keep track of the allocated block

        for (size_t i = 0; i < BlockSize - 1; ++i) {
            block[i].next = &block[i + 1]; // Link nodes in the new block
        }
        block[BlockSize - 1].next = nullptr; // Last node points to null

        return block; // Return the new block
    }

public:
    // Allocate memory for n objects of type T
    pointer allocate(size_type n) {
        if (n != 1) {
            return static_cast<pointer>(::operator new(
                    n * sizeof(T))); // Use default allocation for more than one object
        }

        if (!free_list_head_) {
            try {
                free_list_head_ = allocateBlock(); // Allocate a new block if free list is empty
            } catch (const std::bad_alloc& e) {
                return static_cast<pointer>(::operator new(sizeof(T)));
            }
        }

        Node* result = free_list_head_;
        free_list_head_ = free_list_head_->next;
        return reinterpret_cast<pointer>(&result->data);
    }

    // Deallocate memory for a single object
    void deallocate(pointer p, size_type n) {
        if (!p) return; // Do nothing if pointer is null
        if (n != 1) {
            ::operator delete(p); // Use default deallocation for more than one object
            return;
        }

        Node* node = reinterpret_cast<Node*>(reinterpret_cast<char*>(p) -
                                             offsetof(Node, data)); // Get the node from the pointer
        node->next = free_list_head_; // Link the node back to the free list
        free_list_head_ = node;       // Update the head of the free list
    }

    // Construct an object in allocated memory
    template <typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        new (p) U(std::forward<Args>(args)...); // Placement new to construct the object
    }

    // Destroy an object in allocated memory
    template <typename U>
    void destroy(U* p) {
        p->~U(); // Call the destructor
    }

    // Comparison operators for allocator
    bool operator==(const IKAllocator&) const { return true; }
    bool operator!=(const IKAllocator&) const { return false; }

    // Return the maximum size of allocatable memory
    size_t max_size() const noexcept { return std::numeric_limits<size_type>::max() / sizeof(T); }

    // Return the count of allocated blocks
    size_t get_allocated_block_count() const { return blocks_.size(); }
};

CL_NS_END2

#endif //CLUCENE_IKALLOCATOR_H
