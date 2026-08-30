# Hot Counter PoC

A C++20 proof of concept for one question:

> Can one **hot logical financial counter** scale toward millions of
> **variable-delta operations per second** while preserving a hard limit?

**Current boundary:** the small-instance experiment demonstrated roughly 70K
completed variable-delta operations/second with a hard limit. The repository
now contains automatic local peak/danger transitions, escrow plans for
multi-node peak capacity, a fixed-membership UDP handoff barrier, asynchronous
UDP snapshot replication with ACKs, and an optional DPDK/RSS UDP server. It does **not** prove million-operations/second
scale, or validate DPDK/RSS on an EC2 NIC, until those experiments are run.

## 1. Terms to know first

| Term | Plain meaning | Why it matters here | Reference |
| --- | --- | --- | --- |
| Atomic operation | An update appears to happen as one indivisible step. | A request must not read, check, and update as three separately raceable actions. | [Compare-and-swap](https://en.wikipedia.org/wiki/Compare-and-swap) |
| Hot key | One logical key receives much more traffic than other keys. | Many transactions contend for the same merchant or account limit. | No standalone Wikipedia article; defined in section 2. |
| Data plane | The path that decides `ACCEPT` or `REJECT`. | It must not wait for sampling or reconfiguration. | [Data plane](https://en.wikipedia.org/wiki/Data_plane) |
| Control plane | Background work that changes how later requests should be routed. | It detects a hot key and safely changes local mode after admissions drain. | [Data plane](https://en.wikipedia.org/wiki/Data_plane) |
| DPDK | A userspace packet-processing toolkit with NIC poll-mode drivers. | Optional UDP/RSS backend; requires a Linux host, huge pages, and a dedicated NIC. | [Data Plane Development Kit](https://en.wikipedia.org/wiki/Data_Plane_Development_Kit) |
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
| 2 | Client uses the normal operating-system TCP socket path. | This is the measured baseline. |
| 3 | Guest network interface and AWS virtual network carry the packet to the server. | Physical NIC chip, driver, and RSS queue count were **not recorded**. |
| 4 | Server kernel delivers TCP data to a socket. | `accept`, `read`, and `write` are used. |
| 5 | `ConnectionSession` parses the line and creates a request. | `cpp/server/main.cpp` |
| 6 | `CounterEngine` selects strict or reserved-peak handling. | `cpp/src/engine.cpp` |
| 7 | A counter runs an atomic acceptance check, then replies `A` or `R`. | `StrictCounter` or `ReservedCounter` |
| 8 | Reply returns through normal kernel TCP and the network path. | Kernel bypass is not used by this baseline. |

Do not claim an exact “NIC chip” for this run without evidence. A future
network experiment must record `ethtool -i`, `ethtool -l`, `lspci -nnk`, CPU
affinity, and ENA queue configuration before claiming an RSS or NIC bottleneck.

## 5. Design idea: separate fast requests from background decisions

[![](https://mermaid.ink/img/pako:eNp1Vm1v2zYQ_isHfc6bkzquja5AYndd0CX17GwDNu8DI51tLhKpUlQaL81_3_FISpbtGEHAl3t9nrujXpJUZ5iMkmWuv6drYSzcXy8U0C_NJSr79yIZ8wJO4VctMviMCo2w2iySf7YF72Tayt7djHeuv6BRmLcSfg_34-npzZRlt6Xh-Phja3dr59WicIXmCY33POe180yR_iGNrUUOd2i_a_MIN8qiWYoUm6ge2dB9WpJqJxaY6_QRLUyFXX94MKcfx7UxLqSpbnOqsKqkVi4drRSmljZzf8Y6U2EqhAxzK8Dgtxoru5dj8Opya_LgXRNauGOzTcr1w8qIcg2ZsGKaC4Vt7hM6Aj6DY7gpyhwL8oNZE7b7oVpJVhrr2qHyifcdEaNrK9WKZGZ-dSvKDhRBAAqqna28OD5rZOqqZs6L4IS1r6wuKMXx1Ry0oj8EUdu1NtIKK58QnkRe7wSCjEzmIgnLbYM3KsMS6Z8LKdxDqouSbCtb7UQm0hRLy8au4Ce44u2Ou3-JSu-OJGa83c3P08HUeCh3oeWrgNC2ZgSNrn9EeEA71tQKjd8vkh8Bwbf0Ig5TFI9OOua9zwHHEZM-eBnz3Qf8Dd3OdVebaIghkBQxUOFeZ83CBfdWg3x0E4x6kWjJuzh4FWy1s2GvRVKtrNF57JKx3zYdci3SxxWhqzL4k4ZEpxQq4drntxprp3nthCiM2-ntGPiwI5yhpTC1IclfNPX1ZhIOOlIlMTbzs4AEHX9gjVCVdMMjTomdZk21NplUwhu_b8XH7QW3ws-1rQ2CjE0vnFTH1LdaYpXytLC6BJEVkiu5AkEAZEZIBVIdL3O5WtsYTtUxweEusQmFloDPgqop9V1JxSVsF5uyfshltXYZ-xUofLbQDpZDQyQ20gkskgcK4xiXS01Pk2dlkcCJG4wtRQd548qI1OyTxddbnBwkyldXi_VBZlgowMvriJN34dPeQ6Q7JnZaqCnhJfManjECMfAc37WvpZXEI7NNFX2n7Ztz31SVm2vzOYVK6FRQiLKkmrYaSklvmJuc1FTdwi4z53QynXyhRzU08_WmFFUFXAElsbI3IJu3zNHnE4CSO555o0A6UXmeyFPXb0c9lOO2meZdfAs6g2UuU4amBW7WHga8fq8odaouuIod0YFgFUqbTHwObw9BMZ5N7rnvPj0R0u5Lg2YNpb1CRTWgVb7ZeVjYK9n4atdkIOy32mXfIXzwQ48lD83YMIkdSlupEq0b0A98G-0zXtFucpSsjMySkTU1HiUFmkK4bfLiLC8SCrAgnREtM1yKOieCF-qV1Eqh_tK6iJpUu6t13NQlfY_gRArCniSWIq-ciHucDcOWjHq9IdtIRi_JczI6v7g4ed8fnvfPh-fDy_eD_lGyIaGzy5Nev3f27nLgL4avR8l_7PXsZPBuMOhfnA0uh4Pzfu-yd5RgJqkBb_0nLH_Jvv4PS6eJOg?type=png)](https://mermaid.live/edit#pako:eNp1Vllv4zYQ_isDPeeEj8TGdoHE3m6DbbKunbZA6z4w0thmI5Faisomzea_dzgkJct2jCDgcO5vDuo1SXWGyThZ5fp7uhHGwv31UgH90lyisn8vkwkf4BR-1SKDz6jQCKvNMvlnW_BOpq3s3c1kh_0FjcK8lfA03E9mpzczlt2WhuPjj63dLcqrReEKzRMa73nBZ-eZIv1DGluLHO7QftfmEW6URbMSKTZRPbKh-7Qk1U4ssNDpI1qYCbv58GBOP05qY1xIM93mVGFVSa1cOlopTC0RC3_HOjNhKoQMcyvA4LcaK7uXY_DqcmvyYKoJLfDYbJNy_bA2otxAJqyY5UJhm_uUroDv4BhuijLHgvxg1oTtfqjWkpUmunaofGK6I2J0baVak8zcn25F2YEiCEBBvbOVF8dnjUxd1yz4EJyw9pXVBaU4uVqAVvSHIGq70UZaYeUTwpPI651AkJHJXCThuG3wRmVYIv1zIQU-pLooybay1U5kIk2xtGzsCn6CKyZ33P1LpfTuSGLO5G5-vhxcGg_lLrTMCghta0bQiP0jwgPaVU2t0Xh6mfwICL6nF3GYoXh00jHv_RpwHDHpg8yY7z7g7-h22F1tKkMMgaSoAhXuTdY8MHi2GuSjm2DUi0RL3sVBVrDV7oa9EUm1skbncUomnmwm5Fqkj2tCV2XwJy2JTitUwo3PbzXWTvPaCVEYt7PbCfBlRzhDS2FqQ5K_aJrrl2m46EiVVLG53wUk6OoH1ghVSbc84pbYGdZUa5NJJbzx-1Z80jJ4FH6ubW0QZBx64aQ6pr7VEquUt4XVJYiskNzJFQgCIDNCKpDqeJXL9cbGcKqOCQ53hU0odAR8FtRNqZ9Kai5hu9iU9UMuq43L2J9A4bOFdrEcWiJxkE5gmTxQGMe4Wml6mnxVlgmcuMXYluhg3bgzYmn2i8XsrZocLJTvrhbrg5VhoQAvnyNO3oVPew-R7prYGaGmhVdc1_CMEYihzvFd-1paSXXkalNH32n77t43VeX22mJBoRI6FRSiLKmnrYZS0hvmNicNVbexy8w5nc6mX-hRDcN8_VKKqgLugJKqsrcgm7fMlc8nACVPPNeNAulE5etEnrp-O-qhHbfNNO_ie9AZLHOZMjQtcPP2MuD1e0WpU3fBVZyIDgTr0Npk4nN4ewiKyXx6z3P36YmQdl8atGso7TUq6gGt8pedh4W9ko2vdkMGAr01LvsO4YNfeix5aMeGTexQ2kqVyvoC-oG50T7jFe0mR8nayCwZW1PjUVKgKYQjk1dneZlQgAXpjOmY4UrUORV4qd5IrRTqL62LqEm9u95Eoi7pewSnUhD2JLESeeVE3ONsGLZkfH42ZBvJ-DV5JvJicDIanvWH_f7FxaA3ctyXZDzon5xdjnoXl73BcNTrDUb9t6PkP3Z7djIietDvXfaGl-fDs_OLowQzSRN4679h-VP27X_Sh4l3)

| Plane | Job | Can it block a transaction? | Current status |
| --- | --- | --- | --- |
| Data plane | Parse request, select mode, execute `INCRNCHK`, reply. | No. | Implemented. |
| Control plane | Sample traffic, detect a hot key, and close/drain admissions before publishing a local mode change. | No. | Implemented for one process. |

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
| Admission gate | `AdmissionGate` | Rejects new local work while a mode change waits for in-flight operations to finish. | [admission_gate.hpp](cpp/include/counter_poc/admission_gate.hpp) |
| Reservation plan | `ReservationPlan` | Validates non-overlapping capacity ownership across peak components. | [reservation_plan.hpp](cpp/include/counter_poc/reservation_plan.hpp) |
| UDP replicator | `ReliableUdpReplicator` | Sends monotonic component snapshots; a receiver max-merges them and replies with an ACK. | [replication.hpp](cpp/include/counter_poc/replication.hpp) |
| Limit-reached cache | `LimitReachedCache` | Caches only an exact exhausted state and emits the event only for the accepting operation. | [limit_reached.hpp](cpp/include/counter_poc/limit_reached.hpp) |
| Distributed handoff | `DistributedHandoffController` | Collects drained component totals, installs one strict owner, and makes other nodes return `MOVED`. | [distributed_handoff.hpp](cpp/include/counter_poc/distributed_handoff.hpp) |
| Transport seam | `ITransport` | Interface boundary for alternative transports; the DPDK executable currently has its own fixed UDP protocol. | [transport.hpp](cpp/include/counter_poc/transport.hpp) |

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
| `GCounter` / UDP component snapshots | Yes, through per-component maximum merge. | No. Separate replicas can each accept from incomplete knowledge. | `ReliableUdpReplicator` sends cumulative snapshots and bounds retry work with ACKs. |
| `ReservedCounter` | Does not rely on eventual convergence to accept a request. | Yes, when reservations do not overlap and the transition is quiescent. | Safe local execution lanes and statically allocated multi-node peak capacity. |
| `StrictCounter` | Not applicable; one authority has the value. | Yes. | Current normal and danger handling. |

**Accurate statement:** the PoC gives hard-limit decisions through strict CAS
or pre-reserved capacity. UDP/CRDT state is asynchronous observation and
recovery data, not the authority that admits a financial operation.

## 7. Safe transition rules

| From | To | Coordinator must do | Automatic in `counterd`? |
| --- | --- | --- | --- |
| Strict | Reserved peak | Close the local admission gate; wait for old requests; seed reservations with exact strict value; publish route. | Yes, when the detector threshold is crossed. |
| Reserved peak | Danger strict | Close the local admission gate; wait for peak requests; total reservations; seed strict value; publish route. | Yes for one process that owns the full limit. |
| Distributed reserved peak | One strict owner | Fixed-membership UDP `prepare`, drained-total collection, `commit`, and commit acknowledgement. | Yes, when every member is configured with the same plan and control peers. |

A process using only a component reservation starts directly in peak mode. Its
cluster reservation plan must sum to at most the global limit. This prevents
false allows, but can create false rejects when one component has no capacity
while another component still has some.

## 8. Codebase structure

```text
counter-poc/
├── cpp/                         canonical C++20 implementation
│   ├── include/counter_poc/      interfaces and domain types
│   ├── src/                     counter, routing, detector, engine, replication
│   ├── server/                  kernel-TCP reference daemon
│   ├── tests/                   correctness tests
│   └── dpdk/                    optional DPDK/RSS UDP daemon and smoke client
├── benchmark/                   earlier C benchmark programs
├── src/                         earlier C harness retained for comparison
├── tests/                       earlier C correctness test
├── scripts/                     C-harness helper scripts
└── .github/workflows/ci.yml     build-and-test workflow
```

| Area | Purpose |
| --- | --- |
| `cpp/include/counter_poc/` | Interfaces and domain types. |
| `cpp/src/` | Strict counter, reservations, routing, detector, mode controller, and UDP replication. |
| `cpp/server/` | Transparent POSIX/TCP reference server; not a production network stack. |
| `cpp/tests/` | Hard-limit, CAS, reservation, merge, and transition tests. |
| `cpp/dpdk/` | DPDK poll-mode/RSS UDP daemon, kernel UDP smoke client, and deployment checklist. |
| `benchmark/`, `src/`, `tests/` | Earlier C baseline tools; not the canonical C++ design. |

## 9. Build and current scope

```sh
# Canonical C++ implementation
make clean all test

# Direct-engine microbenchmark. It writes CSV to stdout; redirect each run to
# a separate evidence file rather than combining unlike environments.
./cpp/build/counter_bench --mode=strict --threads=4 --shards=4 \
  --duration-ms=15000 --limit=1000000000000 --min-delta=1 --max-delta=100

# Earlier C benchmark harness
make legacy-clean legacy-c legacy-test

# Optional Linux DPDK backend (requires pkg-config libdpdk)
make -C cpp dpdk dpdk-client
```

The DPDK request payload is a fixed 32-byte UDP frame. `counterd_dpdk` handles
ARP plus IPv4/UDP requests and returns an ACK in-place; `counterd_udp_client`
is a one-request smoke client. Start it only after the dedicated ENI has been
bound to DPDK and its management ENI remains under the kernel driver.

The kernel reference server also supports a component ID, an explicitly
validated static cluster allocation, and UDP snapshot peers:

```sh
# Create a 256-bit binary control-plane key once per fixed cluster. Keep the
# file readable only by the counterd service account; never pass it on a CLI.
install -d -m 0700 /var/lib/counterd
openssl rand -out /var/lib/counterd/handoff.key 32
chmod 0600 /var/lib/counterd/handoff.key

./cpp/build/counterd 9090 1000000 4 100000 50000 peak \
  --component-id=0 --cluster-reservation=0:500000 \
  --cluster-reservation=1:500000 \
  --udp-bind-port=10090 --udp-peer=10.0.0.12:10090 \
  --handoff-bind-port=11090 --handoff-leader=0 --strict-owner=0 \
  --handoff-peer=1:10.0.0.12:11090 \
  --handoff-auth-key-file=/var/lib/counterd/handoff.key \
  --handoff-journal=/var/lib/counterd/component-0.handoff \
  --strict-owner-endpoint=10.0.0.11:9090
```

Every cluster member must use the same `--cluster-reservation` list. Each node
binds a distinct handoff port and configures one `--handoff-peer` endpoint for
every other member. When a component approaches its locally reserved low-water
mark, the leader stops peak admissions everywhere, sums the drained totals, and
commits the global value to `--strict-owner`. Non-owner TCP daemons return
`M IPv4:TCP_PORT` (`MOVED`) when `--strict-owner-endpoint` is set, so a
client/router can retry without a separate owner lookup. Before commit, a
missing member causes a timeout/abort and peak mode reopens; after commit,
retries continue so the cluster is not reopened in a split state.

Handoff control frames are HMAC-SHA-256 authenticated with a 128-bit tag and
are also checked against the configured private IPv4 endpoint and component ID.
Each node records `prepared` before sending `PREPARED`, and records `committed`
before enabling strict mode. A restart with only a `prepared` record keeps its
admission gate closed: an operator must reconcile that node instead of guessing
whether the old leader committed. Use a private security group as an additional
network boundary. Replication frames are still unauthenticated and remain an
experiment-only observation channel, never an admission authority.

| Claim | Status |
| --- | --- |
| Local atomic variable-delta limit enforcement | Tested. |
| Automatic local peak and danger transitions with admission draining | Tested. |
| Safe local reservation-based peak mode | Tested. |
| Validated static multi-node escrow allocation | Tested in-process; deployment requires identical plan on every node. |
| UDP monotonic snapshot replication, duplicate-safe merge, ACK/retry protocol | Implemented; integration test runs when socket binding is allowed. |
| Limit-exhausted edge signal/cache | Tested: only an accepted exact-limit request emits it. |
| Roughly 70K completed TPS in the small-instance offered-load experiment | Demonstrated with the topology caveat above. |
| Fixed-membership distributed barrier and handoff to one strict owner | Implemented and loopback-tested, including prepare timeout/abort behaviour. |
| Authenticated, durable handoff recovery | Implemented: HMAC protects control messages; a committed journal restores strict routing and an ambiguous prepared journal fences requests. |
| C++ direct-engine CSV benchmark | Implemented for strict/local-peak/danger; it is not a network or DPDK result. |
| Distributed CRDT admission | Deliberately not implemented: CRDT merge alone is unsafe for a hard limit. |
| RSS pinning, huge pages, DPDK, kernel bypass | Optional code is implemented; hardware deployment and measurement are not validated. |
| NIC/kernel bottleneck proof | Not done; needs matched instrumentation and saturation tests. |
