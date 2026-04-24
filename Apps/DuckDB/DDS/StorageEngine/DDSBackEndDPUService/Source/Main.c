#include "FileBackEnd.h"

int
main(
    int Argc,
    char** Argv
) {
     int ret;

     ret = RunFileBackEnd(
        DDS_BACKEND_ADDR,
        DDS_BACKEND_PORT,
        1,
        // One buffer connection per channel for each client.
        1 * DDS_NUM_IO_CHANNELS,
        Argc,
        Argv
    );

    return ret;
}
