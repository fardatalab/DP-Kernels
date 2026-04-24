#include "queue.hpp"
#include "dp_manager.hpp"

void initializeQueue(dpkernel_manager_queue *queue)
{
    queue->front.store(0, std::memory_order_relaxed);
    queue->rear.store(0, std::memory_order_relaxed);
    queue->tasks = (dpkernel_task_base **)malloc(MAX_MANAGER_QUEUE_SIZE * sizeof(*queue->tasks));
}

int isQueueEmpty(dpkernel_manager_queue *queue)
{
    return (queue->rear.load(std::memory_order_acquire) == queue->front.load(std::memory_order_acquire));
}

int isQueueFull(dpkernel_manager_queue *queue)
{
    int r = queue->rear.load(std::memory_order_relaxed);
    int f = queue->front.load(std::memory_order_acquire);
    return (r - f) == MAX_MANAGER_QUEUE_SIZE;
}

int enqueue(dpkernel_manager_queue *queue, dpkernel_task_base *task)
{
    while (true)
    {
        int r = queue->rear.load(std::memory_order_relaxed);
        int f = queue->front.load(std::memory_order_acquire);
        if ((r - f) == MAX_MANAGER_QUEUE_SIZE)
            return -1;
        if (queue->rear.compare_exchange_weak(r, r + 1, std::memory_order_release, std::memory_order_relaxed))
        {
            queue->tasks[r % MAX_MANAGER_QUEUE_SIZE] = task;
            return 0;
        }
    }
}

dpkernel_task_base *dequeue(dpkernel_manager_queue *queue)
{
    while (true)
    {
        int f = queue->front.load(std::memory_order_relaxed);
        int r = queue->rear.load(std::memory_order_acquire);
        if (f == r)
            return nullptr;
        if (queue->front.compare_exchange_weak(f, f + 1, std::memory_order_release, std::memory_order_relaxed))
        {
            return queue->tasks[f % MAX_MANAGER_QUEUE_SIZE];
        }
    }
}
void initializeQueue(dpkernel_manager_queue *queue)
{
    queue->front.store(0, std::memory_order_relaxed);
    queue->rear.store(0, std::memory_order_relaxed);
    queue->tasks = (dpkernel_task_base **)malloc(MAX_MANAGER_QUEUE_SIZE * sizeof(*queue->tasks));
}

int isQueueEmpty(dpkernel_manager_queue *queue)
{
    return (queue->rear.load(std::memory_order_acquire) == queue->front.load(std::memory_order_acquire));
}

int isQueueFull(dpkernel_manager_queue *queue)
{
    int r = queue->rear.load(std::memory_order_relaxed);
    int f = queue->front.load(std::memory_order_acquire);
    return (r - f) == MAX_MANAGER_QUEUE_SIZE;
}

int enqueue(dpkernel_manager_queue *queue, dpkernel_task_base *task)
{
    while (true)
    {
        int r = queue->rear.load(std::memory_order_relaxed);
        int f = queue->front.load(std::memory_order_acquire);
        if ((r - f) == MAX_MANAGER_QUEUE_SIZE)
            return -1;
        if (queue->rear.compare_exchange_weak(r, r + 1, std::memory_order_release, std::memory_order_relaxed))
        {
            queue->tasks[r % MAX_MANAGER_QUEUE_SIZE] = task;
            return 0;
        }
    }
}

dpkernel_task_base *dequeue(dpkernel_manager_queue *queue)
{
    while (true)
    {
        int f = queue->front.load(std::memory_order_relaxed);
        int r = queue->rear.load(std::memory_order_acquire);
        if (f == r)
            return nullptr;
        if (queue->front.compare_exchange_weak(f, f + 1, std::memory_order_release, std::memory_order_relaxed))
        {
            return queue->tasks[f % MAX_MANAGER_QUEUE_SIZE];
        }
    }
}