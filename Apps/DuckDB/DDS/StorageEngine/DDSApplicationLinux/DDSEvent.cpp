#include <pthread.h>
#include <DDSEvent.h>

void Event_Init(Event* ev) {
    pthread_mutex_init(&ev->mutex, NULL);
    pthread_cond_init(&ev->cond, NULL);
    ev->signaled = false;
}

void Event_Set(Event* ev) {
    pthread_mutex_lock(&ev->mutex);
    ev->signaled = true;
    pthread_cond_broadcast(&ev->cond);
    pthread_mutex_unlock(&ev->mutex);
}

void Event_Wait(Event* ev) {
    pthread_mutex_lock(&ev->mutex);
    while (!ev->signaled) {
        pthread_cond_wait(&ev->cond, &ev->mutex);
    }
    pthread_mutex_unlock(&ev->mutex);
}

void Event_Reset(Event* ev) {
    pthread_mutex_lock(&ev->mutex);
    ev->signaled = false;
    pthread_mutex_unlock(&ev->mutex);
}

void Event_Destroy(Event* ev) {
    pthread_mutex_destroy(&ev->mutex);
    pthread_cond_destroy(&ev->cond);
}
