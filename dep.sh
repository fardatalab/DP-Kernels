# This script installs the dependencies for the project.
#!/bin/bash


# download latest cmake first
cd ~/
wget https://github.com/Kitware/CMake/releases/download/v4.0.1/cmake-4.0.1-linux-aarch64.sh
sudo chmod +x cmake-4.0.1-linux-aarch64.sh
sudo ./cmake-4.0.1-linux-aarch64.sh --skip-license --prefix=/usr/local


# install mimalloc v1.9.2
cd ~/
git clone https://github.com/microsoft/mimalloc.git
cd mimalloc
git checkout v1.9.2
mkdir -p out/release
cd out/release
cmake -DMI_OVERRIDE=OFF ../..
make -j4
sudo make install

# install libdeflate
cd ~/
git clone https://github.com/ebiggers/libdeflate.git
cd libdeflate
cmake -B build && cmake --build build
cd build
sudo make install