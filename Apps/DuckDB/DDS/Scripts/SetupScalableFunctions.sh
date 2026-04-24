/opt/mellanox/iproute2/sbin/mlxdevm port add pci/0000:03:00.0 flavour pcisf pfnum 0 sfnum 1
/opt/mellanox/iproute2/sbin/mlxdevm port function set pci/0000:03:00.0/229377 hw_addr 00:00:00:00:01:00 trust on state active
echo mlx5_core.sf.3 > /sys/bus/auxiliary/drivers/mlx5_core.sf_cfg/unbind
echo mlx5_core.sf.3 > /sys/bus/auxiliary/drivers/mlx5_core.sf/bind
devlink dev show
