cd ../../ThirdParty/spdk
cp ../../StorageEngine/DDSBackEndDPUService/Patches/spdk_dpdk_init.c lib/env_dpdk/init.c
CFLAGS="$(pkg-config --cflags libdpdk)" make -j
make install

cd -
rm -r build; meson setup build -Dbuildtype=release; ninja -v -C build
