#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <unistd.h>

#include "PEPOLinuxTCP.h"

const char* DPU_IP_ADDRESS = "192.168.200.32";
const int DPU_PORT = 3232;
const char* HOST_IP_ADDRESS = "192.168.200.132";
const int HOST_PORT = 3232;
const int MAX_QUEUE_SIZE = 1000;

struct MessageHeader {
    uint8_t OffloadedToDPU;
    uint8_t LastMessage;
    int MessageSize;
    char Timestamp[10];
};

int ServerFd = -1;

//
// Initialize the PEPO based on Linux TCP/IP
//
//
int
PEPOLinuxTCPInit(void) {
    return 0;
}

//
// Run the PEPO
//
//
int
PEPOLinuxTCPRun(void) {
    int ServerFd, hostServerFd, clientFd;
    struct sockaddr_in serverAddr, hostServerAddr, clientAddr;
    socklen_t clientLen;
    
    //
    // create DPU server socket
    //
    //
    ServerFd = socket(AF_INET, SOCK_STREAM, 0);
    if (ServerFd < 0) {
        perror("ERROR opening socket");
        return 1;
    }
    
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = inet_addr(DPU_IP_ADDRESS);
    serverAddr.sin_port = htons(DPU_PORT);
    if (bind(ServerFd, (struct sockaddr *) &serverAddr, sizeof(serverAddr)) < 0) {
        perror("ERROR on binding");
        return 1;
    }
    
    //
    // listen for connections
    //
    //
    if (listen(ServerFd, MAX_QUEUE_SIZE) < 0) {
        perror("ERROR on listening");
        return 1;
    }
    
    printf("Server is listening on %s:%d\n", inet_ntoa(serverAddr.sin_addr), DPU_PORT);
    
    while (1) {
        //
        // accept connection
        //
        //
        clientLen = sizeof(clientAddr);
        clientFd = accept(ServerFd, (struct sockaddr *) &clientAddr, &clientLen);
        if (clientFd < 0) {
            perror("ERROR on accepting connection");
            return 1;
        }

        //
        // Connect to host
        //
        //
        hostServerFd = socket(AF_INET, SOCK_STREAM, 0);
        if (hostServerFd < 0) {
            perror("ERROR opening socket");
            return 1;
        }

        memset(&hostServerAddr, 0, sizeof(hostServerAddr));
        hostServerAddr.sin_family = AF_INET;
        hostServerAddr.sin_addr.s_addr = inet_addr(HOST_IP_ADDRESS);
        hostServerAddr.sin_port = htons(HOST_PORT);
        if (connect(hostServerFd, (struct sockaddr *) &hostServerAddr, sizeof(hostServerAddr)) < 0) {
            perror("ERROR on connecting to host");
            close(clientFd);
            return 1;
        }

        //
        // Receive the first message
        // Forward this message to the host
        //
        //
        int msgSize = 0;
        int iResult = read(clientFd, (char*)&msgSize, sizeof(int));
        if (iResult < 0 || iResult != sizeof(int)) {
            perror("ERROR on reading from client");
            close(clientFd);
            close(hostServerFd);
            return 1;
        }

        iResult = write(hostServerFd, (const char*)&msgSize, sizeof(int));
        if (iResult < 0 || iResult != sizeof(int)) {
            perror("ERROR on writing to host");
            close(clientFd);
            close(hostServerFd);
            return 1;
        }

        printf("Starting measurement (%d-byte messages)...\n", msgSize);

        char* recvBuffer = (char*)malloc(msgSize);
        memset(recvBuffer, 0, msgSize);

        int oneReceive = 0;
        int remainingBytesToReceive = msgSize;

        int oneSend = 0;
        int remainingBytesToSend = msgSize;
        
        printf("Client connected from %s:%d\n", inet_ntoa(clientAddr.sin_addr), ntohs(clientAddr.sin_port));

        while (1) {
            //
            // Receive message from client and send response
            //
            //
            while (oneReceive < msgSize) {
                iResult = read(clientFd, recvBuffer + oneReceive, remainingBytesToReceive);

                if (iResult < 0) {
                    perror("ERROR on reading from client");
                    close(clientFd);
                    close(hostServerFd);
                    return 1;
                }

                oneReceive += iResult;
                remainingBytesToReceive = msgSize - oneReceive;
            }

            oneReceive = 0;
            remainingBytesToReceive = msgSize;

            //
            // Check if this is the last message
            //
            //
            if (((struct MessageHeader*)recvBuffer)->LastMessage) {
                //
                // Forward this message to the host
                //
                //
                while (oneSend < msgSize) {
                    iResult = write(hostServerFd, recvBuffer + oneSend, remainingBytesToSend);
                    if (iResult < 0) {
                        perror("ERROR on writing to host");
                        close(hostServerFd);
                        close(clientFd);
                        return 1;
                    }

                    oneSend += iResult;
                    remainingBytesToSend = msgSize - oneSend;
                }

                oneSend = 0;
                remainingBytesToSend = msgSize;

                break;
            }

            //
            // Check if this message is for the host
            //
            //
            if (!((struct MessageHeader*)recvBuffer)->OffloadedToDPU) {
                //
                // Send message to host
                //
                //
                while (oneSend < msgSize) {
                    iResult = write(hostServerFd, recvBuffer + oneSend, remainingBytesToSend);
                    if (iResult < 0) {
                        perror("ERROR on writing to host");
                        close(hostServerFd);
                        close(clientFd);
                        return 1;
                    }

                    oneSend += iResult;
                    remainingBytesToSend = msgSize - oneSend;
                }

                oneSend = 0;
                remainingBytesToSend = msgSize;

                //
                // Receive response from host
                //
                //
                while (oneReceive < msgSize) {
                    iResult = read(hostServerFd, recvBuffer + oneReceive, remainingBytesToReceive);

                    if (iResult < 0) {
                        perror("ERROR on reading from host");
                        close(hostServerFd);
                        close(clientFd);
                        return 1;
                    }

                    oneReceive += iResult;
                    remainingBytesToReceive = msgSize - oneReceive;
                }

                oneReceive = 0;
                remainingBytesToReceive = msgSize;
            }

            //
            // Send response to client
            //
            //
            while (oneSend < msgSize) {
                iResult = write(clientFd, recvBuffer + oneSend, remainingBytesToSend);
                if (iResult < 0) {
                    perror("ERROR on writing to client");
                    close(hostServerFd);
                    close(clientFd);
                    return 1;
                }

                oneSend += iResult;
                remainingBytesToSend = msgSize - oneSend;
            }

            oneSend = 0;
            remainingBytesToSend = msgSize;
        }

        printf("Measurement completes\n");

    //
        // Close the socket for this client and host server and handle the next connection
    //
    //
        close(clientFd);
        close(hostServerFd);

        free(recvBuffer);
    }

    // Close the server socket
    close(ServerFd);
    
    return 0;
}

//
// Stop the PEPO
//
//
int
PEPOLinuxTCPStop(void) {
    if (ServerFd < 0) {
        return 0;
    }
    else {
        close(ServerFd);
        return 0;
    }
}

//
// Destroy the PEPO
//
//
int
PEPOLinuxTCPDestroy(void) {
    return PEPOLinuxTCPStop();
}
