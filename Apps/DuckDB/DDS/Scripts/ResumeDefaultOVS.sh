ovs-vsctl del-br sf_bridge1
ovs-vsctl add-br ovsbr1
ovs-vsctl add-port ovsbr1 p0
ovs-vsctl add-port ovsbr1 pf0hpf
ovs-vsctl add-port ovsbr1 en3f0pf0sf0
ovs-vsctl show
