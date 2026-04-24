#ifndef DDS_EVENT_H
#define DDS_EVENT_H

#include <pthread.h>

struct Event {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool signaled;
};

void Event_Init(Event* ev);
void Event_Set(Event* ev);
void Event_Wait(Event* ev);
void Event_Reset(Event* ev);
void Event_Destroy(Event* ev);

#endif