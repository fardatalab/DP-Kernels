INSTALL_PATH=/opt/dds/spdk
mkdir -p $INSTALL_PATH
cd ../ThirdParty/spdk

# Avoid duplicated DPDK init
cp ../../StorageEngine/DDSBackEndDPUService/Patches/spdk_dpdk_init.c lib/env_dpdk/init.c
# Avoid SPDK reactors taking all the Arm cores
cp ../../StorageEngine/DDSBackEndDPUService/Patches/spdk_dpdk_threads.c lib/env_dpdk/threads.c
cp ../../StorageEngine/DDSBackEndDPUService/Patches/spdk_reactor.c lib/event/reactor.c

pip3 install testresources
pip3 install --upgrade setuptools
./scripts/pkgdep.sh --all
git submodule update --init
./configure --prefix=$INSTALL_PATH --with-shared --with-dpdk=/opt/mellanox/dpdk/
CFLAGS="$(pkg-config --cflags libdpdk) -DDPDK_INIT_SUPPRESSED -DSINGLE_REACTOR" make -j
make install
cp isa-l/.libs/libisal.a $INSTALL_PATH/lib/
cp isa-l/.libs/libisal.lai $INSTALL_PATH/lib/
cp isa-l/libisal.la $INSTALL_PATH/lib/
