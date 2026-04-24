#include "DDSBackEndWorker.h"

namespace DDS_BackEnd {

DDSBackEndWorker::DDSBackEndWorker() {
	TailPointer = new int();
	*TailPointer = 42;

	AtomicTailPointerRef = new std::atomic_ref<int>(*TailPointer);
}

}