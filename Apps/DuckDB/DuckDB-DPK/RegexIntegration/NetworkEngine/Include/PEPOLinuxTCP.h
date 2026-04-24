#ifndef PEPO_LINUX_TCP_H
#define PEPO_LINUX_TCP_H

//
// Initialize the PEPO based on Linux TCP/IP
//
//
int
PEPOLinuxTCPInit(void);

//
// Run the PEPO
//
//
int
PEPOLinuxTCPRun(void);

//
// Stop the PEPO
//
//
int
PEPOLinuxTCPStop(void);

//
// Destroy the PEPO
//
//
int
PEPOLinuxTCPDestroy(void);

#endif /* PEPO_LINUX_TCP_H */
