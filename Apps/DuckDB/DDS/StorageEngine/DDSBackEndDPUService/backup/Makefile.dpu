CC=gcc

SPDK_DIR=/opt/mellanox/spdk
SPDK_HEADER_DIR=$(SPDK_DIR)/include
SPDK_LIB_DIR=$(SPDK_DIR)/lib
DPDK_LIB_DIR=/opt/mellanox/dpdk/lib/aarch64-linux-gnu

Include=-I../../Common/Include -I../../Common/Include/DPU -I../../StorageEngine/DDSBackEndDPUService/Include

COMMON_CFLAGS=-I$(SPDK_HEADER_DIR)
COMMON_CFLAGS+=$(Include)
COMMON_CFLAGS+=-L$(SPDK_LIB_DIR)
COMMON_CFLAGS+=-L$(DPDK_LIB_DIR)

# PKG_CONFIG_PATH = $(SPDK_LIB_DIR)/pkgconfig
# $(info PKG_CONFIG_PATH: $(PKG_CONFIG_PATH))

### SPDK_LIB := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --libs spdk_bdev)
### DPDK_LIB := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --libs spdk_env_dpdk)

SPDK_LIB = -lspdk_bdev -lspdk_notify -lspdk_bdev_malloc -lspdk_bdev_null -lspdk_bdev_nvme -lspdk_bdev_passthru -lspdk_bdev_lvol -lspdk_bdev_raid -lspdk_accel -lspdk_accel_ioat -lspdk_ioat -lspdk_bdev_error -lspdk_bdev_gpt -lspdk_bdev_split -lspdk_bdev_delay -lspdk_bdev_zone_block -lspdk_blobfs_bdev -lspdk_blobfs -lspdk_blob_bdev -lspdk_lvol -lspdk_blob -lspdk_dma -lspdk_vmd -lspdk_nvme -lspdk_sock -lspdk_sock_posix -lspdk_bdev_aio -lspdk_bdev_ftl -lspdk_ftl -lspdk_bdev_virtio -lspdk_virtio -lspdk_thread -lspdk_trace -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_util -lspdk_vfio_user -lspdk_log 
SPDK_EVENT_LIB = -lspdk_event -lspdk_env_dpdk_rpc -lspdk_event_bdev -lspdk_bdev -lspdk_notify -lspdk_bdev_malloc -lspdk_bdev_null -lspdk_bdev_nvme -lspdk_bdev_passthru -lspdk_bdev_lvol -lspdk_bdev_raid -lspdk_bdev_error -lspdk_bdev_gpt -lspdk_bdev_split -lspdk_bdev_delay -lspdk_bdev_zone_block -lspdk_blobfs_bdev -lspdk_blobfs -lspdk_blob_bdev -lspdk_lvol -lspdk_blob -lspdk_nvme -lspdk_bdev_aio -lspdk_bdev_ftl -lspdk_ftl -lspdk_bdev_virtio -lspdk_virtio -lspdk_vfio_user -lspdk_event_accel -lspdk_accel -lspdk_dma -lspdk_accel_ioat -lspdk_ioat -lspdk_event_vmd -lspdk_vmd -lspdk_event_sock -lspdk_sock -lspdk_sock_posix -lspdk_event_iobuf -lspdk_init -lspdk_thread -lspdk_trace -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_util -lspdk_log


DPDK_LIB := -lspdk_env_dpdk -lspdk_util -lspdk_log -L/home/jason/spdk/dpdk/build/lib -lrte_eal -lrte_mempool -lrte_ring -lrte_mbuf -lrte_bus_pci -lrte_pci -lrte_mempool_ring -lrte_telemetry -lrte_kvargs -lrte_rcu -lrte_power -lrte_ethdev -lrte_vhost -lrte_net -lrte_dmadev -lrte_cryptodev -lrte_hash
# SPDK_EVENT_LIB := $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_event spdk_event_bdev)
# SPDK_DPDK_LIB := $(shell PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" pkg-config --libs spdk_event spdk_event_bdev spdk_env_dpdk)
SYS_LIB :=-pthread -lrt -luuid -lssl -lcrypto -lm  -lbsd -lnuma -ldl  -lrdmacm -libverbs


$(info SPDK_LIB: $(SPDK_LIB))
$(info DPDK_LIB: $(DPDK_LIB))


LDFLAGS=-L$(SPDK_LIB_DIR) -L$(DPDK_LIB_DIR) -lspdk_bdev_malloc -lspdk_bdev -lspdk_notify  -lspdk_bdev_error -lspdk_bdev_gpt -lspdk_bdev_split -lspdk_bdev_delay -lspdk_bdev_zone_block -lspdk_accel -lspdk_accel_ioat -lspdk_thread -lspdk_trace -lspdk_rpc -lspdk_jsonrpc -lspdk_json -lspdk_util -lspdk_ioat -lspdk_dma -lspdk_log -lspdk_event -lspdk_env_dpdk_rpc -lspdk_event_bdev 
LDLIBS=-pthread -Wl,--no-as-needed -lspdk_env_dpdk $(SPDK_LIB) $(SPDK_EVENT_LIB)  $(DPDK_LIB) -lrdmacm -libverbs

# SPDK_LIB := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --libs spdk_bdev)
# DPDK_LIB := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --libs spdk_env_dpdk)
# SYS_LIB := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --libs --static spdk_syslibs)

# LDLIBS=-pthread -Wl,--whole-archive $(SPDK_LIB) $(DPDK_LIB) $(SYS_LIB)

# LDLIBS=-pthread -lspdk -lspdk_env_dpdk -ldpdk

CFLAGS=-std=gnu11 -O3 -g

PROG=TestDDS
BUILD_DIR=./build
SRC_COMMON=../../Common/Source

SRC_DIR = Source
SRCS = $(wildcard $(SRC_DIR)/*.c)
SRCS += $(SRC_COMMON)/DPU/FileService.c
SRCS += $(SRC_COMMON)/DPU/RingBufferPolling.c
$(info SRCS: $(SRCS))

OBJ = $(SRCS:%.c=$(BUILD_DIR)/%.o)
$(info OBJ: $(OBJ))
DEP = $(OBJ:%.o=%.d)


all: $(PROG)

$(PROG) : $(BUILD_DIR)/$(PROG)

# Actual target of the binary - depends on all .o files.
# $(CC) $(Include) $(CFLAGS) $^ -o $@ $(LDLIBS)
$(BUILD_DIR)/$(PROG) : $(OBJ)
	mkdir -p $(@D)
	$(CC) $(CFLAGS) $(COMMON_CFLAGS) -Wl,-rpath=$(SPDK_LIB_DIR),--no-as-needed $^ -o $@ $(DPDK_LIB) $(SYS_LIB) $(LDFLAGS) $(LDLIB)

-include $(DEP)

# Build target for every single object file.
# The potential dependency on header files is covered
# by calling `-include $(DEP)`.
# The -MMD flags additionaly creates a .d file with
# the same name as the .o file.
# $(CC) $(Include) $(CFLAGS) -MMD -c $< -o $@ $(LDLIBS)
$(BUILD_DIR)/%.o : %.c
	$(info with -MMD: $<)
	mkdir -p $(@D)
	$(CC) $(CFLAGS) $(COMMON_CFLAGS)  -Wl,-rpath=$(SPDK_LIB_DIR),--no-as-needed -MMD -c $< -o $@ $(LDFLAGS) $(LDLIB)

.PHONY: clean
clean:
	# This should remove all generated files.
	-rm $(BUILD_DIR)/$(BIN) $(OBJ) $(DEP)
