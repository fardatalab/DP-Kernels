#pragma once
#include <iterator>

template <typename T>
struct NodeT {
    T Value;
    NodeT* Next;
};


/*
 *
 * Iterator for LinkedList to be used in DDSPosix.cpp
 *
 *
 */
template <typename T>
class LinkedListIterator {
public:
    // Iterator typedefs required for compatibility with STL
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = T*;
    using reference = T&;

private:
    NodeT<T>* current;

public:
    // Constructor
    LinkedListIterator(NodeT<T>* node = nullptr) : current(node) {}

    // Copy constructor
    LinkedListIterator(const LinkedListIterator& other) : current(other.current) {}

    // Assignment operator
    LinkedListIterator& operator=(const LinkedListIterator& other) {
        current = other.current;
        return *this;
    }

    // Dereference operator
    T& operator*() const {
        return current->Value;
    }

    // Arrow operator
    T* operator->() const {
        return &(current->Value);
    }

    // Pre-increment
    LinkedListIterator& operator++() {
        current = current->Next;
        return *this;
    }

    // Post-increment
    LinkedListIterator operator++(int) {
        LinkedListIterator temp = *this;
        current = current->Next;
        return temp;
    }

    // Equality operator
    bool operator==(const LinkedListIterator& other) const {
        return current == other.current;
    }

    // Inequality operator
    bool operator!=(const LinkedListIterator& other) const {
        return current != other.current;
    }

    // Get the current node
    NodeT<T>* GetNode() const {
        return current;
    }

    // Get the current value (for compatibility with your existing code)
    T GetValue() const {
        return current->Value;
    }

    // Check if the iterator is valid (points to a node)
    bool IsValid() const {
        return current != nullptr;
    }

    // Move to the next node (for compatibility with your existing code)
    void Next() {
        if (current) {
            current = current->Next;
        }
    }
};





template <typename T>
class LinkedList {
private:
    NodeT<T>* Head;
    NodeT<T>* Tail;

public:
    // Define iterator types
    using iterator = LinkedListIterator<T>;
    using const_iterator = const LinkedListIterator<T>;

    //
    // template class implementation must
    // be put in the header file
    //
    //
    LinkedList();
    ~LinkedList();
    void Insert(T NodeValue);
    void Delete(T NodeValue);
    void DeleteAll();
    NodeT<T>* Begin();
    NodeT<T>* End();

    // Add iterator methods
    iterator begin() {
        return iterator(Head);
    }

    iterator end() {
        return iterator(nullptr);
    }

    const_iterator cbegin() const {
        return const_iterator(Head);
    }

    const_iterator cend() const {
        return const_iterator(nullptr);
    }

};

// NEW: add specialization declarations to build
template<> void LinkedList<int>::DeleteAll();
template<> void LinkedList<uint32_t>::DeleteAll();
template<> void LinkedList<unsigned long>::DeleteAll();
template<> void LinkedList<unsigned short>::DeleteAll();