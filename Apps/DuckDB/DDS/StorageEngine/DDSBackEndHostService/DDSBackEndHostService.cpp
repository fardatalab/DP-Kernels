#include <iostream>
#include <thread>
#include <string.h>

#include "DDSBackEndHostService.h"

namespace DDS_BackEnd {
    
//
// Constructor
// 
//
    BackEndHostService::BackEndHostService() {
}

//
// Destructor
// 
//
BackEndHostService::~BackEndHostService() {
}

//
// Initialize the backend service
//
//
ErrorCodeT
BackEndHostService::Initialize() {
    Storage.Initialize();
}

}