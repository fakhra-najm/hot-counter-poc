# TCP and DPDK transport runbook

`CounterRuntime` is the transport-independent product. It owns the counter
engine, peak/danger transitions, optional UDP replication, and authenticated
distributed handoff. The data-plane adapter is selected with
`COUNTER_TRANSPORT`:

- `tcp` (or unset): `counterd` runs the TCP line protocol.
- `dpdk`: `counterd` launches its sibling `counterd_dpdk` binary. Build that
  binary first on the target Linux host with `make -C cpp dpdk dpdk-client`.

The two modes use different application protocols:

- TCP accepts decimal `delta\n` and returns `A`, `R`, or `M endpoint`.
- DPDK accepts the fixed 40-byte `UdpCounterProtocol` frame and returns its
  fixed 40-byte acknowledgement. `counterd_udp_client` is a smoke client.

They share `CounterRuntime`; different framing is intentional so the TCP
baseline retains normal kernel networking while DPDK owns the dedicated NIC.

## Machine A: TCP baseline

```sh
make -C cpp clean all

COUNTER_TRANSPORT=tcp ./cpp/build/counterd 9090 1000000000 4 100000 50000
```

For peak-mode comparison, use the same engine flags and variable-delta request
distribution that will be used for DPDK:

```sh
COUNTER_TRANSPORT=tcp ./cpp/build/counterd 9090 1000000000 4 100000 50000 peak
```

## Machine B: DPDK data plane

Use a Linux machine with DPDK, huge pages, IOMMU/vfio, enabled lcores, and a
dedicated data-plane NIC. Keep the SSH/management ENI under its kernel driver.

```sh
make -C cpp dpdk dpdk-client

COUNTER_TRANSPORT=dpdk ./cpp/build/counterd -l 1-4 -a PCI_ADDRESS -- \
  --dpdk-port=0 --queues=4 --udp-port=9091 \
  --limit=1000000000 --shards=4 --danger-threshold=100000 --hot-tps=50000 \
  --start-peak
```

The shared flags after `--` configure the same `CounterRuntime` as TCP. The
DPDK-specific flags are `--dpdk-port`, `--queues`, and `--udp-port`.

## Fair comparison rules

- Use the same AMI, compiler flags, limit, shard count, danger threshold,
  hot-key threshold, client concurrency, duration, and variable-delta trace.
- Run TCP and DPDK as separate matched experiments. A TCP client cannot retry
  a DPDK UDP `MOVED` endpoint, and vice versa; do not mix them in one live
  client-routing experiment.
- Kernel UDP control traffic for replication/handoff remains on the management
  interface. Set `--udp-bind-host` and `--handoff-bind-host` to that interface
  when the DPDK NIC is bound to `vfio-pci`.
- Record both completed operations/second and latency percentiles. Label the
  result as TCP or DPDK because their framing and kernel paths differ.

## Shared multi-node control-plane configuration

Both adapters accept the same reservation, replication, and handoff options:

```text
--component-id=N
--cluster-reservation=component:capacity
--udp-bind-port=N --udp-peer=IPv4:port
--handoff-bind-port=N --handoff-peer=component:IPv4:port
--handoff-leader=N --strict-owner=N
--handoff-auth-key-file=PATH --handoff-journal=PATH
```

The control plane therefore behaves the same for TCP and DPDK runs. A
multi-node test must use a data-plane-aware router when it wants to follow a
`MOVED` reply across transports.
