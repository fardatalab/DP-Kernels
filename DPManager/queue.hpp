#include <atomic>
#include <cstdlib>

typedef struct dpkernel_manager_queue
{
    struct dpkernel_task_base **tasks; // array of ptrs to task_base
    std::atomic<int> front;
    std::atomic<int> rear;
} dpkernel_manager_queue;
// dpkernel_manager_queue queue;

void initializeQueue(dpkernel_manager_queue *queue);

int isQueueEmpty(dpkernel_manager_queue *queue);

int isQueueFull(dpkernel_manager_queue *queue);

int enqueue(dpkernel_manager_queue *queue, struct dpkernel_task_base *task);

struct dpkernel_task_base *dequeue(dpkernel_manager_queue *queue);