#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

#include "MPSC.h"
#include "Profiler.h"

#define NUM_PRODUCERS 4

using namespace std;
 
class Message {
public:
       int Size;
       int* Data;
};
 
class SmallMessage : Message {
public:
       SmallMessage() {
              Size = 8; // 8 B
              Data = new int[3];
       }
       ~SmallMessage() {
              delete[] Data;
       }
};
 
struct MediumMessage : Message {
public:
       MediumMessage() {
              Size = 8192; // 8 KB
              Data = new int[2049];
       }
       ~MediumMessage() {
              delete[] Data;
       }
};
 
struct LargeMessage : Message {
public:
       LargeMessage() {
              Size = 1048576; // 1 MB
              Data = new int[262145];
       }
};
 
void MessageProducer(
       FarRingMPSC* FarRingMPSC,
       Message** Messages,
       size_t NumMessages
) {
       for (size_t r = 0; r != NumMessages; r++) {
              while (InsertMPSC(FarRingMPSC, (BufferT)Messages[r]->Data, Messages[r]->Size + sizeof(int)) == false) {
                     this_thread::yield();
              }
       }
}
 
void MessageConsumer(
       FarRingMPSC* FarRingMPSC,
       size_t TotalNumMessages
) {
       size_t numMsgProcessed = 0;
       size_t sum = 0;
       char* pagesOfMessages = new char[RING_SIZE];
       BufferT messagePointer = NULL;
       MessageSizeT messageSize = 0;
       BufferT startOfNext = NULL;
       MessageSizeT remainingSize = 0;
       Profiler profiler(TotalNumMessages);
      
       profiler.Start();
       while (numMsgProcessed != TotalNumMessages) {
              while(FetchMPSC(FarRingMPSC, pagesOfMessages, &remainingSize) == false) {
                     this_thread::yield();
              }
 
              startOfNext = pagesOfMessages;
 
              while (true) {
                     ParseNextMessageMPSC(
                           startOfNext,
                           remainingSize,
                           &messagePointer,
                           &messageSize,
                           &startOfNext,
                           &remainingSize);
 
                     int* message = (int*)messagePointer;
 
                     int numInts = message[0] / (int)sizeof(int);
                     for (int i = 1; i != numInts + 1; i++) {
                           sum += message[i];
                     }
 
                     numMsgProcessed++;
 
                     if (remainingSize == 0) {
                           break;
                     }
              }
       }
 
       profiler.Stop();
 
       delete[] pagesOfMessages;
 
       cout << "Microbenchmark completed" << endl;
       cout << "-- Result: sum = " << sum << endl;
       cout << "-- Ring progress tail = " << FarRingMPSC->Progress[0] << endl;
       cout << "-- Ring safe tail = " << FarRingMPSC->Tail[0] << endl;
       cout << "-- Ring head = " << FarRingMPSC->Head[0] << endl;
       profiler.Report();
}
 
void EvaluateFarRingMPSC() {
       char* Buffer = new char[134217728]; // 128 MB
       FarRingMPSC* ringBuffer = NULL;
       const size_t messagesPerProducer = 10000000;
       size_t totalMessages = messagesPerProducer * NUM_PRODUCERS;
 
       //
       // Allocate a ring buffer
       //
       //
       cout << "Allocating a ring buffer..." << endl;
       memset(Buffer, 0, RING_SIZE);
       ringBuffer = AllocateMPSC(Buffer);
 
       //
       // Prepare messages
       //
       //
       cout << "Preparing messages..." << endl;
       unsigned int randomSeed = 0;
       srand(randomSeed);
       Message** allMessages = new Message*[totalMessages];
       size_t sum = 0;
       for (size_t r = 0; r != totalMessages; r++) {
              Message* curReq = NULL;
              switch (rand() % 1)
              {
              case 2:
              {
                     curReq = (Message*)(new LargeMessage());
                     break;
              }
              case 1:
              {
                     curReq = (Message*)(new MediumMessage());
                     break;
              }
              default:
              {
                     curReq = (Message*)(new SmallMessage());
                     break;
              }
              }
 
              int numInts = curReq->Size / (int)sizeof(int);
              curReq->Data[0] = curReq->Size;
              for (int i = 1; i != numInts + 1; i++) {
                     curReq->Data[i] = rand();
                     sum += curReq->Data[i];
              }
 
              allMessages[r] = curReq;
       }
       cout << "Messages have been prepared: sum = " << sum << endl;
 
       //
       // Start producers
       //
       //
       cout << "Starting producers..." << endl;
       thread* producerThreads[NUM_PRODUCERS];
 
       for (size_t t = 0; t != NUM_PRODUCERS; t++) {
              producerThreads[t] = new thread(
                     [t, ringBuffer, allMessages] {
                           MessageProducer(ringBuffer,
                                  &allMessages[messagesPerProducer * t],
                                  messagesPerProducer);
                     }
              );
       }
 
       //
       // Start the consumer
       //
       //
       cout << "Starting the consumer..." << endl;
       thread* consumerThread = new thread(
              [ringBuffer, totalMessages] {
                     MessageConsumer(ringBuffer, totalMessages);
              }
       );
 
       //
       // Wait for all threads to join
       //
       //
       cout << "Waiting for all threads to complete..." << endl;
       for (size_t t = 0; t != NUM_PRODUCERS; t++) {
              producerThreads[t]->join();
       }
       consumerThread->join();
 
       cout << "Release all of the memory..." << endl;
       DeallocateMPSC(ringBuffer);
 
       for (size_t r = 0; r != totalMessages; r++) {
              delete allMessages[r];
       }
       delete[] allMessages;
 
       for (size_t t = 0; t != NUM_PRODUCERS; t++) {
              delete producerThreads[t];
       }
       delete consumerThread;
 
       delete[] Buffer;
}

int main() {
       EvaluateFarRingMPSC();

       return 0;
}
