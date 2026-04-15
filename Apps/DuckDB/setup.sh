#!/bin/bash

# Set the system huge pages
sudo sh -c "echo 4096 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"

# ping the host to get ARP entry
HOST_IPV4=$(grep '"host-ipv4"' DDSPipeline.json | grep -oP '(?<=: ")[^"]+')
if ping -c 1 "$HOST_IPV4"; then
    exit 0
else
    echo "ping host failed"
fi
