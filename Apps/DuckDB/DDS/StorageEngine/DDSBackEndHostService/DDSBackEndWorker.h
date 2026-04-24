#pragma once
#include <atomic>

namespace DDS_BackEnd {

//
// A back end worker that performs DMA to read requests and write responses
//
//
class DDSBackEndWorker {
private:
	std::atomic_ref<int>* AtomicTailPointerRef;
	int* TailPointer;

public:
	DDSBackEndWorker();
};

}