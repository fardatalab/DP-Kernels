# make a copy of the original dpdk_utils.c
sudo cp /opt/mellanox/doca/applications/common/src/dpdk_utils.c /opt/mellanox/doca/applications/common/src/dpdk_utils.c.orig
# Apply patches
sudo cp Patches/dpdk_utils.c /opt/mellanox/doca/applications/common/src/dpdk_utils.c

BUILD_DIR="./build/"
if [ -d "$BUILD_DIR" ]; then
	rm -r $BUILD_DIR
fi
meson build
ninja -C build
