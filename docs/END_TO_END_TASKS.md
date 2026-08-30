# End-to-end execution backlog

This is the operational task list for proving—not assuming—that the hot-counter
design is safe and useful. A checked item is implemented in this repository;
an external gate requires the EC2/DPDK environment or a measured result.

## A. Correctness and safety

- [x] Atomic variable-delta `INCRNCHK` with a hard local limit.
- [x] Per-lane escrow capacity: peak accepts cannot overshoot the global limit.
- [x] Automatic local strict → peak → danger transition with admission draining.
- [x] Exact `LIMIT_REACHED` event only after an accepting operation.
- [x] Fixed-membership distributed `prepare → commit` handoff and `MOVED` result.
- [x] Missing-member pre-commit timeout aborts and reopens peak safely.
- [x] Authenticate every distributed control packet with a configured key.
      `HCN1` frames use a 128-bit truncated HMAC-SHA-256 tag; the daemon only
      reads raw key material from a protected file.
- [x] Persist a pre-commit fence and committed handoff record; fence startup after an
      interrupted handoff instead of accepting work from an unknown state.
- [x] Store strict-owner endpoint in TCP `MOVED` so a router can retry without external mapping.
- [ ] Add property/fuzz tests for malformed, duplicate, stale, reordered, and forged packets.

## B. Replication and recovery

- [x] Cumulative component snapshots, duplicate-safe max merge, UDP ACK/retry,
      bounded pending work, and loopback integration test.
- [ ] Add packet authentication for replication when it crosses an untrusted network.
- [ ] Persist a component snapshot/checkpoint and recover it before reopening a node.
- [ ] Test process restart, control-leader restart, and owner failure after commit.

## C. Data plane and observability

- [x] Kernel-TCP baseline daemon and fixed-frame DPDK UDP daemon.
- [x] RSS queue-per-lcore DPDK setup code and UDP smoke client.
- [ ] Export accepted/rejected/moved, mode, handoff, replication, queue-drop, and
      latency metrics in a machine-readable endpoint/file.
- [x] Add a C++ microbenchmark that emits comparable CSV for strict, local
      peak, and danger modes. Distributed figures remain an EC2 execution gate.
- [ ] Capture CPU, memory, NIC queue, RSS, and packet-drop evidence per run.

## D. Deployment and experiment execution

- [ ] Provide deterministic Linux/EC2 provisioning for compiler, DPDK, huge pages,
      vfio/IOMMU, secondary ENI, security groups, and CPU pinning.
- [ ] Validate the dedicated ENI with `testpmd`; retain the primary ENI for SSH.
- [ ] Run one-variable-at-a-time saturation tests using identical delta distributions.
- [ ] Run failure injection: packet loss/reorder, delayed peer, killed component,
      leader restart, and owner restart.
- [ ] Publish result CSVs and a final comparison table only after matched runs.

## E. Release gate

- [x] Unit/integration tests and an ASan/UBSan CI job are defined. Linux CI
      must pass for a specific release candidate.
- [ ] DPDK binary builds against the target DPDK version.
- [ ] Two-node and multi-node EC2 handoff tests pass with authenticated traffic.
- [ ] Baseline and peak performance claims cite committed CSV evidence.
- [ ] Security review covers keys, process privileges, ENI binding, and recovery procedures.

## Current implementation order

1. Finish A/B: fuzz the authenticated protocols, authenticate replication,
   and add checkpoint/restart tests.
2. Finish C: add non-intrusive daemon metrics and collect matching latency,
   CPU, and NIC evidence.
3. Finish D: provision and validate the selected EC2 data-plane host.
4. Execute E only on the target EC2 hardware; do not substitute local results.
