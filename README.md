# Hot Counter PoC

A C++20 proof of concept for one question:

> Can one **hot logical financial counter** scale toward millions of
> **variable-delta operations per second** while preserving a hard limit?

**Current boundary:** the small-instance experiment demonstrated roughly 70K
completed variable-delta operations/second with a hard limit. It does **not**
prove million-operations/second scale, DPDK, kernel bypass, RSS pinning, or
distributed multi-node scaling.

## 1. Terms to know first

| Term | Plain meaning | Why it matters here | Reference |
| --- | --- | --- | --- |
| Atomic operation | An update appears to happen as one indivisible step. | A request must not read, check, and update as three separately raceable actions. | [Compare-and-swap](https://en.wikipedia.org/wiki/Compare-and-swap) |
| Hot key | One logical key receives much more traffic than other keys. | Many transactions contend for the same merchant or account limit. | No standalone Wikipedia article; defined in section 2. |
| Data plane | The path that decides `ACCEPT` or `REJECT`. | It must not wait for sampling or reconfiguration. | [Data plane](https://en.wikipedia.org/wiki/Data_plane) |
| Control plane | Background work that changes how later requests should be routed. | It detects a hot key and requests a safe mode change. | [Data plane](https://en.wikipedia.org/wiki/Data_plane) |
| DPDK | A userspace packet-processing toolkit with NIC poll-mode drivers. | Possible future transport optimisation; not enabled in this PoC. | [Data Plane Development Kit](https://en.wikipedia.org/wiki/Data_Plane_Development_Kit) |
| Kernel bypass | A userspace process accesses a NIC more directly than the normal kernel networking path. | It can reduce packet-path overhead at high packet rates; it does not remove counter-state contention. | No standalone Wikipedia article; see [operating-system kernel](https://en.wikipedia.org/wiki/Kernel_(operating_system)). |
| CRDT | A replicated type whose replicas can merge concurrent state and converge. | Useful for convergence; insufficient alone for a hard financial cap. | [Conflict-free replicated data type](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) |
| Eventual consistency | Replicas may temporarily differ and later converge after updates are exchanged. | Convergence is not the same as safe financial admission. | [Eventual consistency](https://en.wikipedia.org/wiki/Eventual_consistency) |
| RSS | Hardware distribution of received flows across NIC receive queues and CPU cores. | It can spread network work; it cannot split one shared atomic counter. | [Receive-side scaling](https://en.wikipedia.org/wiki/Receive_Side_Scaling) |
| NIC | Network interface controller: hardware, or a virtual function, that connects a machine to the network. | It receives/transmits packets and commonly uses queues and DMA. | [Network interface controller](https://en.wikipedia.org/wiki/Network_interface_controller) |
| DMA | A device moves data to/from memory without the CPU copying every byte. | NICs use it to place packet data in memory. | [Direct memory access](https://en.wikipedia.org/wiki/Direct_memory_access) |

## 2. Problem: a variable-delta counter becomes a hot key

A payment limit is not `counter += 1`. Every request has its own amount.

```text
INCRNCHK(counter, delta, limit)

if current + delta <= limit:
    atomically store current + delta
    return ACCEPT
else:
    leave the counter unchanged
    return REJECT
```

| Current | Delta | Limit | Correct result | Reason |
| ---: | ---: | ---: | --- | --- |
| 95 | 5 | 100 | Accept; value becomes 100 | The request reaches the limit exactly. |
| 95 | 6 | 100 | Reject; value remains 95 | Accepting would make the value 101. |
| 90 | +8 at A and +8 at B concurrently | 100 | Unsafe without extra coordination | Each node can accept from a stale local view; total becomes 106. |

A hot key is one merchant/account counter receiving a very high request rate.
Changing the names of unrelated keys does not remove contention on this one
logical state.

## 3. What the networking terms can — and cannot — solve

| Technology | Helps with | Does **not** solve |
| --- | --- | --- |
| NIC + DMA | Packet movement between network and memory. | Counter correctness. |
| RSS | Distribution of separate network flows across receive queues/cores. | Contention on one shared counter cache line. |
| Kernel bypass / DPDK | Some kernel socket, interrupt, and copy overhead at high packet rates. | Hard-limit correctness, replication, or unsafe transition logic. |
| Atomic CAS | Safe local read-check-update. | Cross-node coordination by itself. |
| CRDT | Eventual merge/convergence of replicated state. | A hard global financial limit by itself. |

The current implementation deliberately starts with kernel TCP. A performance
limit could come from client generation, the network path, server scheduling,
or the hot atomic state. DPDK only becomes meaningful after those baselines are
measured.

## 4. How a request travels on the current t3.micro PoC

**Measured topology:** a benchmark client on one small EC2 instance sends
persistent TCP traffic across the private VPC to `counterd` on another small EC2
instance. Redis was measured separately over loopback; its latency is not
directly comparable to the C++ result.

| Step | Current PoC behaviour | Source / certainty |
| ---: | --- | --- |
| 1 | Client writes one decimal `delta` plus newline on a persistent TCP connection. | `benchmark/offered_benchmark.c` |
| 2 | Client uses the normal operating-system TCP socket path. | DPDK is not enabled. |
| 3 | Guest network interface and AWS virtual network carry the packet to the server. | Physical NIC chip, driver, and RSS queue count were **not recorded**. |
| 4 | Server kernel delivers TCP data to a socket. | `accept`, `read`, and `write` are used. |
| 5 | `ConnectionSession` parses the line and creates a request. | `cpp/server/main.cpp` |
| 6 | `CounterEngine` selects strict or reserved-peak handling. | `cpp/src/engine.cpp` |
| 7 | A counter runs an atomic acceptance check, then replies `A` or `R`. | `StrictCounter` or `ReservedCounter` |
| 8 | Reply returns through normal kernel TCP and the network path. | Kernel bypass is not used. |

Do not claim an exact “NIC chip” for this run without evidence. A future
network experiment must record `ethtool -i`, `ethtool -l`, `lspci -nnk`, CPU
affinity, and ENA queue configuration before claiming an RSS or NIC bottleneck.

## 5. Design idea: separate fast requests from background decisions

| Plane | Job | Can it block a transaction? | Current status |
| --- | --- | --- | --- |
| Data plane | Parse request, select mode, execute `INCRNCHK`, reply. | No. | Implemented. |
| Control plane | Sample traffic, detect a hot key, request a transition, coordinate it later. | No. | Detector and flags exist; no distributed coordinator. |

### Current design vocabulary

| Discussion name | Actual repository name | Meaning | Source / status |
| --- | --- | --- | --- |
| HotKeyRouter | No class with this name; closest is `CounterEngine` + `RoutingMap`. | Chooses strict or reserved-peak handling. | [engine.hpp](cpp/include/counter_poc/engine.hpp), [routing.hpp](cpp/include/counter_poc/routing.hpp) |
| FastMap | No structure with this name. | `RoutingMap` is a simple atomic mode publisher plus route hash; it is **not** a general lock-free map. | [routing.hpp](cpp/include/counter_poc/routing.hpp) |
| Hot-key detector | `HotKeyDetector` | Sends best-effort samples through a bounded MPMC queue and raises a hot-key request. | [hotkey_detector.hpp](cpp/include/counter_poc/hotkey_detector.hpp) |
| Fast path | `CounterEngine::apply` and selected counter | Bounded counter work with no lock or allocation in the counter update path. | [engine.cpp](cpp/src/engine.cpp) |
| Strict counter | `StrictCounter` | One aligned atomic value protected by a CAS loop. | [strict_counter.hpp](cpp/include/counter_poc/strict_counter.hpp) |
| Reserved counter | `ReservedCounter` | Each internal component owns a fixed, non-overlapping portion of the limit. | [reserved_counter.hpp](cpp/include/counter_poc/reserved_counter.hpp) |
| Routing mode | `RoutingMode` | `Strict`, `ReservedPeak`, or `DangerStrict`. | [types.hpp](cpp/include/counter_poc/types.hpp) |
| Danger zone | `danger_transition_requested()` | Signals that the remaining capacity is near the configured threshold. | [engine.hpp](cpp/include/counter_poc/engine.hpp) |
| Transport seam | `ITransport` | Future place for epoll, io_uring, or DPDK transport. | [transport.hpp](cpp/include/counter_poc/transport.hpp) |

## 6. Accuracy versus eventual consistency

The safe-peak implementation is **not** “eventually consistent admission.”
Before peak requests are admitted, capacity is divided into reservations.

| Global limit | Reservation A | Reservation B | Per-component rule | Safety result |
| ---: | ---: | ---: | --- | --- |
| 100 | 50 | 50 | Accept only if local use + `delta` is at most its own reservation. | Accepted total cannot exceed 100. |

No capacity is owned twice, so a component cannot spend capacity assigned to
another component. The trade-off is a **false rejection**: A may be empty while
B still has spare capacity. Moving capacity safely needs future control-plane
work.

| Component | Eventual convergence? | Can it authorize a hard limit alone? | Current use |
| --- | --- | --- | --- |
| `GCounter` | Yes, through per-component maximum merge. | No. Separate replicas can each accept from incomplete knowledge. | Demonstration type and merge tests; no replication network path. |
| `ReservedCounter` | Does not rely on eventual convergence to accept a request. | Yes, when reservations do not overlap and the transition is quiescent. | Current safe-peak implementation inside one process. |
| `StrictCounter` | Not applicable; one authority has the value. | Yes. | Current normal and danger handling. |

**Accurate statement:** the PoC gives hard-limit decisions through strict CAS
or pre-reserved capacity. It does **not** yet implement an eventually
consistent, replicated financial admission system.

## 7. Safe transition rules

| From | To | Coordinator must do | Automatic in `counterd`? |
| --- | --- | --- | --- |
| Strict | Reserved peak | Stop admissions for that counter; wait for old requests; seed reservations with exact strict value; publish route. | No. Tests call the transition after quiescence. |
| Reserved peak | Danger strict | Stop admissions; wait for peak requests; total reservations; seed strict value; publish route. | No. Engine can raise a danger request; no server coordinator consumes it. |

## 8. Codebase structure

```text
counter-poc/
├── cpp/                         canonical C++20 implementation
│   ├── include/counter_poc/      interfaces and domain types
│   ├── src/                     counter, routing, detector, engine
│   ├── server/                  kernel-TCP reference daemon
│   ├── tests/                   correctness tests
│   └── dpdk/                    requirements, not a DPDK transport
├── benchmark/                   earlier C benchmark programs
├── src/                         earlier C harness retained for comparison
├── tests/                       earlier C correctness test
├── scripts/                     C-harness helper scripts
└── .github/workflows/ci.yml     build-and-test workflow
```

| Area | Purpose |
| --- | --- |
| `cpp/include/counter_poc/` | Interfaces and domain types. |
| `cpp/src/` | Strict counter, reservations, routing, detector, engine. |
| `cpp/server/` | Transparent POSIX/TCP reference server; not a production network stack. |
| `cpp/tests/` | Hard-limit, CAS, reservation, merge, and transition tests. |
| `cpp/dpdk/` | Future ENA, huge-page, RSS, and DPDK checklist. |
| `benchmark/`, `src/`, `tests/` | Earlier C baseline tools; not the canonical C++ design. |

## 9. Build and current scope

```sh
# Canonical C++ implementation
make clean all test

# Earlier C benchmark harness
make legacy-clean legacy-c legacy-test
```

| Claim | Status |
| --- | --- |
| Local atomic variable-delta limit enforcement | Tested. |
| Safe local reservation-based peak mode | Tested. |
| Roughly 70K completed TPS in the small-instance offered-load experiment | Demonstrated with the topology caveat above. |
| Automatic hot-key migration and danger return | Not end-to-end implemented. |
| Async replication, failure recovery, distributed CRDT admission | Not implemented. |
| RSS pinning, huge pages, DPDK, kernel bypass | Not implemented or validated. |
| NIC/kernel bottleneck proof | Not done; needs matched instrumentation and saturation tests. |
