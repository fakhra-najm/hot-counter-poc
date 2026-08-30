# DPDK deployment contract

`counterd_dpdk` is a separate poll-mode, UDP data-plane executable. It owns
one dedicated NIC, configures RSS when more than one queue is requested, pins
one receive/transmit queue to each enabled DPDK lcore, and returns an
`INCRNCHK` acknowledgement in a fixed-size UDP frame (`ACCEPT`, `REJECT`, or
`MOVED`). The standard `counterd`
process remains the kernel-TCP baseline.

Do not bind the management ENI to DPDK: SSH and control traffic must remain on
the primary ENI. Use a dedicated ENA secondary ENI and bind only that PCI
device to `vfio-pci`.

```sh
make -C cpp dpdk dpdk-client

# EAL options select CPU cores, huge pages, and the dedicated NIC.
./cpp/build/counterd_dpdk -l 1-4 -a PCI_ADDRESS -- \
  --dpdk-port=0 --queues=4 --udp-port=9091 --limit=1000000

# From another host on the same data network.
./cpp/build/counterd_udp_client SERVER_IPV4 9091 7 25
```

Before calling a result “kernel bypass” or “RSS scaling”, record: IOMMU/vfio
state; DPDK version; ENA PMD; huge pages; NIC PCI address; queue count; enabled
lcores; CPU affinity; `testpmd` forwarding result; and matching TCP baseline.
DPDK is not enabled merely because `libdpdk` is installed.
