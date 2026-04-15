# RegEx App with DuckDB
## Overview
This directory contains three implementations for the scenario where a client makes a request (i.e., a RegEx matching query `SELECT COUNT(*) FROM comment WHERE l_comment LIKE '%regular%';`) to a server that hosts DuckDB. Specifically,
* A client sends the request over TCP to a server that runs the vanilla DuckDB using its C/C++ interface. For details, check `client.cpp` and `server.cpp` (possibly along with `socket.cpp` where all socket-related helpers are implemented).
* A client does the same, except that the request is intercepted by a DPU-optimized network engine that executes the query using Google's RE2 regular expression library on DPU's SoC cores, and the result is sent back to the client. For details, check `re2_regex_app.cpp`.
* A client does the same, execpt that the request is intercepted by a DPU-optimized network engine that invokes DPKernel's RegEx kernel and sends back the result to the client. For details, check `bf3_regex_app.cpp`.
## Setup
* Run `git submodule update --init --recursive` to have all the submodules in place.
* Run `setup.sh` to set up the hugepages the network engine needs to run.
* Run `config.sh` to configure various dependencies `server.cpp` needs to run (run `source config.sh` to perserve the changes to the current shell process rather than its children).
## Build
* In `DP-Kernels/`, build DPKernels to get a static library the app can link with.
* Back to the current directory, and run `meson build` and `meson compile -C build` to have the app built. An executable `network_engine` will be created under `build`.
## Run
* To run the basic client and server, simply run `build/client` and `build/server`.
* Otherwise, run `build/client` first, and then
	* Run `dp_manager` under `DP-Kernels/build` on the DPU  with `sudo` access to have shared memory set up.
	* Run `network_engine` under `DP-Kernels/build/Apps/DuckDB/build` on the DPU with `sudo` access and the network configuration file `NetworkEngine/DDSPipeline.json` to run the network engine, which will eventually drive one of `re2_regex_app` and `bf3_regex_app`.
