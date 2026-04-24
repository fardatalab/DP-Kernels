#ifndef APP_ECHO_TCP_H_
#define APP_ECHO_TCP_H_

#include <rte_mbuf.h>

//
// Echo TCP packets back to the sender
//
//
struct rte_mbuf*
EchoTCPDirect(
    struct rte_mbuf *mbuf
);


#endif /* APP_ECHO_TCP_H_ */