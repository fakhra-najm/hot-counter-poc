# DPDK deployment contract

`counterd_dpdk` is a poll-mode UDP adapter over the shared `CounterRuntime`
product. It owns one dedicated NIC, configures RSS when more than one queue is
requested, pins one receive/transmit queue to each enabled DPDK lcore, and
returns an `INCRNCHK` acknowledgement in a fixed 40-byte UDP frame (`ACCEPT`,
`REJECT`, or `MOVED`). The same runtime is used by the kernel-TCP `counterd`
adapter, including peak/danger transitions, replication, and authenticated
distributed handoff.

Do not bind the management ENI to DPDK: SSH and control traffic must remain on
the primary ENI. Use a dedicated ENA secondary ENI and bind only that PCI
device to `vfio-pci`.

```sh
make -C cpp dpdk dpdk-client

# EAL options select CPU cores, huge pages, and the dedicated NIC.
COUNTER_TRANSPORT=dpdk ./cpp/build/counterd -l 1-4 -a PCI_ADDRESS -- \
  --dpdk-port=0 --queues=4 --udp-port=9091 --limit=1000000

# From another host on the same data network.
./cpp/build/counterd_udp_client SERVER_IPV4 9091 7 25
```

Before calling a result “kernel bypass” or “RSS scaling”, record: IOMMU/vfio
state; DPDK version; ENA PMD; huge pages; NIC PCI address; queue count; enabled
lcores; CPU affinity; `testpmd` forwarding result; and matching TCP baseline.
DPDK is not enabled merely because `libdpdk` is installed.

`--udp-bind-port` / `--udp-peer` and `--handoff-bind-port` /
`--handoff-peer` configure the shared kernel-UDP control plane, not the DPDK
data port. Bind those control sockets to the management interface and reserve
the vfio-bound NIC for DPDK data traffic. The full comparison instructions are
in [the transport runbook](../../docs/TRANSPORT_RUNBOOK.md).
