ovs-vsctl del-br ovsbr1
ovs-vsctl add-br sf_bridge1
ovs-vsctl add-port sf_bridge1 p0
ovs-vsctl add-port sf_bridge1 en3f0pf0sf1
ovs-vsctl add-port sf_bridge1 pf0hpf
ovs-vsctl show
