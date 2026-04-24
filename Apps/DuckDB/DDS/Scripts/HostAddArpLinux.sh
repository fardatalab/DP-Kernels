#!/bin/bash

# Check the number of arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 [DPU IP address] [SF representor's MAC]"
    exit 1
fi

sudo arp -s $1 $2
