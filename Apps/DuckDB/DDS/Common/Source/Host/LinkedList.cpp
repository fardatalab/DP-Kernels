#include <stdint.h>

#include "LinkedList.h"

//
// int instantiation
//
//
template<>
LinkedList<int>::LinkedList() {
    Head = nullptr;
    Tail = nullptr;
}

template<>
LinkedList<int>::~LinkedList() {
    DeleteAll();
}

template<>
void
LinkedList<int>::Insert(int NodeValue) {
    NodeT<int>* newNode = new NodeT<int>();
    newNode->Value = NodeValue;
    newNode->Next = nullptr;

    if (Tail) {
        Tail->Next = newNode;
        Tail = newNode;
    }
    else {
        Head = Tail = newNode;
    }
}

template<>
void
LinkedList<int>::Delete(int NodeValue) {
    NodeT<int>* cur = Head;
    NodeT<int>* prev = nullptr;
    while (cur != nullptr) {
        if (cur->Value == NodeValue) {
            if (prev) {
                prev->Next = cur->Next;
            }
            if (cur == Head) {
                Head = cur->Next;
            }
            break;
        }
        prev = cur;
        cur = cur->Next;
    }

    if (cur) {
        delete cur;
    }
}

template <>
void
LinkedList<int>::DeleteAll() {
    if (Head) {
        while (Head->Next) {
            NodeT<int>* next = Head->Next;
            Head->Next = next->Next;
            delete next;
        }
        delete Head;
    }
    Head = Tail = nullptr;
}

template<>
NodeT<int>*
LinkedList<int>::Begin() {
    return Head;
}

template<>
NodeT<int>*
LinkedList<int>::End() {
    return Tail;
}

//
// unsigned int instantiation
//
//
template<>
LinkedList<uint32_t>::LinkedList() {
    Head = nullptr;
    Tail = nullptr;
}

template<>
LinkedList<uint32_t>::~LinkedList() {
    DeleteAll();
}

template<>
void
LinkedList<uint32_t>::Insert(uint32_t NodeValue) {
    NodeT<uint32_t>* newNode = new NodeT<uint32_t>();
    newNode->Value = NodeValue;
    newNode->Next = nullptr;

    if (Tail) {
        Tail->Next = newNode;
        Tail = newNode;
    }
    else {
        Head = Tail = newNode;
    }
}

template<>
void
LinkedList<uint32_t>::Delete(uint32_t NodeValue) {
    NodeT<uint32_t>* cur = Head;
    NodeT<uint32_t>* prev = nullptr;
    while (cur != nullptr) {
        if (cur->Value == NodeValue) {
            if (prev) {
                prev->Next = cur->Next;
            }
            if (cur == Head) {
                Head = cur->Next;
            }
            break;
        }
        prev = cur;
        cur = cur->Next;
    }

    if (cur) {
        delete cur;
    }
}

template <>
void
LinkedList<uint32_t>::DeleteAll() {
    if (Head) {
        while (Head->Next) {
            NodeT<uint32_t>* next = Head->Next;
            Head->Next = next->Next;
            delete next;
        }
        delete Head;
    }
    Head = Tail = nullptr;
}

template<>
NodeT<uint32_t>*
LinkedList<uint32_t>::Begin() {
    return Head;
}

template<>
NodeT<uint32_t>*
LinkedList<uint32_t>::End() {
    return Tail;
}

//
// unsigned long instantiation
//
//
template<>
LinkedList<unsigned long>::LinkedList() {
    Head = nullptr;
    Tail = nullptr;
}

template<>
LinkedList<unsigned long>::~LinkedList() {
    DeleteAll();
}

template<>
void
LinkedList<unsigned long>::Insert(unsigned long NodeValue) {
    NodeT<unsigned long>* newNode = new NodeT<unsigned long>();
    newNode->Value = NodeValue;
    newNode->Next = nullptr;

    if (Tail) {
        Tail->Next = newNode;
        Tail = newNode;
    }
    else {
        Head = Tail = newNode;
    }
}

template<>
void
LinkedList<unsigned long>::Delete(unsigned long NodeValue) {
    NodeT<unsigned long>* cur = Head;
    NodeT<unsigned long>* prev = nullptr;
    while (cur != nullptr) {
        if (cur->Value == NodeValue) {
            if (prev) {
                prev->Next = cur->Next;
            }
            if (cur == Head) {
                Head = cur->Next;
            }
            break;
        }
        prev = cur;
        cur = cur->Next;
    }

    if (cur) {
        delete cur;
    }
}

template <>
void
LinkedList<unsigned long>::DeleteAll() {
    if (Head) {
        while (Head->Next) {
            NodeT<unsigned long>* next = Head->Next;
            Head->Next = next->Next;
            delete next;
        }
        delete Head;
    }
    Head = Tail = nullptr;
}

template<>
NodeT<unsigned long>*
LinkedList<unsigned long>::Begin() {
    return Head;
}

template<>
NodeT<unsigned long>*
LinkedList<unsigned long>::End() {
    return Tail;
}

//
// unsigned short instantiation
//
//

template<>
LinkedList<unsigned short>::LinkedList() {
    Head = nullptr;
    Tail = nullptr;
}

template<>
LinkedList<unsigned short>::~LinkedList() {
    DeleteAll();
}

template<>
void
LinkedList<unsigned short>::Insert(unsigned short NodeValue) {
    NodeT<unsigned short>* newNode = new NodeT<unsigned short>();
    newNode->Value = NodeValue;
    newNode->Next = nullptr;

    if (Tail) {
        Tail->Next = newNode;
        Tail = newNode;
    }
    else {
        Head = Tail = newNode;
    }
}

template<>
void
LinkedList<unsigned short>::Delete(unsigned short NodeValue) {
    NodeT<unsigned short>* cur = Head;
    NodeT<unsigned short>* prev = nullptr;
    while (cur != nullptr) {
        if (cur->Value == NodeValue) {
            if (prev) {
                prev->Next = cur->Next;
            }
            if (cur == Head) {
                Head = cur->Next;
            }
            break;
        }
        prev = cur;
        cur = cur->Next;
    }

    if (cur) {
        delete cur;
    }
}

template <>
void
LinkedList<unsigned short>::DeleteAll() {
    if (Head) {
        while (Head->Next) {
            NodeT<unsigned short>* next = Head->Next;
            Head->Next = next->Next;
            delete next;
        }
        delete Head;
    }
    Head = Tail = nullptr;
}

template<>
NodeT<unsigned short>*
LinkedList<unsigned short>::Begin() {
    return Head;
}

template<>
NodeT<unsigned short>*
LinkedList<unsigned short>::End() {
    return Tail;
}