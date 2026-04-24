#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

#include "SPMC.h"
#include "Profiler.h"

#define NUM_CONSUMERS 4

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
       FarRingSPMC* FarRingSPMC,
       Message** Messages,
       size_t NumMessages
) {
       Profiler profiler(NumMessages);
      
       profiler.Start();
       for (size_t r = 0; r != NumMessages; r++) {
              while (InsertSPMC(FarRingSPMC, (BufferT)Messages[r]->Data, Messages[r]->Size + sizeof(int)) == false) {
                     this_thread::yield();
              }
       }
       while(!CheckForCompletionSPMC(FarRingSPMC)) {
              this_thread::yield();
       }
       profiler.Stop();

       cout << "Microbenchmark completed" << endl;
       cout << "-- Ring progress tail = " << FarRingSPMC->Progress[0] << endl;
       cout << "-- Ring safe tail = " << FarRingSPMC->Tail[0] << endl;
       cout << "-- Ring head = " << FarRingSPMC->Head[0] << endl;
       profiler.Report();
}
 
void MessageConsumer(
       FarRingSPMC* FarRingSPMC,
       size_t TotalNumMessages
) {
       size_t numMsgProcessed = 0;
       char* pagesOfMessages = new char[RING_SIZE];
       MessageSizeT messageSize = 0;
       
       while (numMsgProcessed != TotalNumMessages) {
              while(FetchSPMC(FarRingSPMC, pagesOfMessages, &messageSize) == false) {
                     this_thread::yield();
              }
              numMsgProcessed++;
       }
 
       delete[] pagesOfMessages;
}
 
void EvaluateFarRingSPMC() {
       char* Buffer = new char[134217728]; // 128 MB
       FarRingSPMC* ringBuffer = NULL;
       const size_t messagesPerConsumer = 10000000;
       size_t totalMessages = messagesPerConsumer * NUM_CONSUMERS;
 
       //
       // Allocate a ring buffer
       //
       //
       cout << "Allocating a ring buffer..." << endl;
       memset(Buffer, 0, RING_SIZE);
       ringBuffer = AllocateSPMC(Buffer);
 
       //
       // Prepare messages
       //
       //
       cout << "Preparing messages..." << endl;
       unsigned int randomSeed = 0;
       srand(randomSeed);
       Message** allMessages = new Message*[totalMessages];
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
              }
 
              allMessages[r] = curReq;
       }
       cout << "Messages have been prepared" << endl;
 
       //
       // Start the producer
       //
       //
       cout << "Starting producers..." << endl;
       thread* producerThread;

       producerThread = new thread(
              [ringBuffer, allMessages, totalMessages] {
                     MessageProducer(ringBuffer,
                            allMessages,
                            totalMessages);
              }
       );
 
       //
       // Start the consumer
       //
       //
       cout << "Starting the consumers..." << endl;
       thread* consumerThreads[NUM_CONSUMERS];

       for (size_t t = 0; t != NUM_CONSUMERS; t++) {
              consumerThreads[t] = new thread(
                     [ringBuffer] {
                            MessageConsumer(ringBuffer, messagesPerConsumer);
                     }
              );
       }
 
       //
       // Wait for all threads to join
       //
       //
       cout << "Waiting for all threads to complete..." << endl;
       producerThread->join();
       for (size_t t = 0; t != NUM_CONSUMERS; t++) {
              consumerThreads[t]->join();
       }
 
       cout << "Release all of the memory..." << endl;
       DeallocateSPMC(ringBuffer);
 
       for (size_t r = 0; r != totalMessages; r++) {
              delete allMessages[r];
       }
       delete[] allMessages;
 
       delete producerThread;
       for (size_t t = 0; t != NUM_CONSUMERS; t++) {
              delete consumerThreads[t];
       }
 
       delete[] Buffer;
}

int main() {
       EvaluateFarRingSPMC();

       return 0;
}
