#ifndef BOUNDED_QUEUE_HPP
#define BOUNDED_QUEUE_HPP
#include <cstdio>
#pragma once

#include <array>

template <typename T, std::size_t N> class BoundedQueue
{
    std::size_t head = 0;
    std::size_t tail = 0;
    std::size_t count = 0;
    std::array<T, N> buffer{};

  public:
    BoundedQueue() = default;

    bool push(const T &value)
    {
        // printf("queue push\n");
        if (count == N)
        {
            return false;
        }
        buffer[tail] = value;
        tail = (tail + 1) % N;
        ++count;
        return true;
    }

    bool push_front(const T &value)
    {
        if (count == N)
        {
            return false;
        }
        head = (head - 1 + N) % N;
        buffer[head] = value;
        ++count;
        return true;
    }

    bool pop_back(T &value)
    {
        if (count == 0)
        {
            return false;
        }
        tail = (tail - 1 + N) % N;
        value = buffer[tail];
        --count;
        return true;
    }

    bool pop(T &value)
    {
        if (count == 0)
        {
            return false;
        }
        value = buffer[head];
        head = (head + 1) % N;
        --count;
        return true;
    }

    bool empty() const
    {
        return count == 0;
    }
    bool full() const
    {
        return count == N;
    }
    std::size_t size() const
    {
        return count;
    }
    static constexpr std::size_t capacity()
    {
        return N;
    }
};

#endif // BOUNDED_QUEUE_HPP
