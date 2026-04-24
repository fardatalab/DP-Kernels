#pragma once

#include "DDSBackEndStorage.h"

namespace DDS_BackEnd {

class BackEndHostService {
private:
    BackEndStorage Storage;

public:
    BackEndHostService();

    ~BackEndHostService();
    
    //
    // Initialize the backend service
    //
    //
    ErrorCodeT
    Initialize();
};

}