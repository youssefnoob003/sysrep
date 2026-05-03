# Distributed Flash Sale System: Engineering Documentation

## 1. Executive Summary
The Distributed Flash Sale System is an advanced proof-of-concept designed to model the extreme high-concurrency environments of modern ticketing platforms. The primary objective of this project is to explicitly demonstrate the chaotic nature of distributed computing—specifically race conditions, split-brain scenarios, and lock contention—and to architect algorithmic solutions to solve them.

By utilizing a Python Flask API Gateway as a load balancer and a dynamic cluster of Java-based gRPC nodes, the system allows for real-time visualization of cluster topologies. It contrasts the massive data corruption (overselling tickets) of a non-synchronized system against the perfect consistency (and inherent performance trade-offs) of a system implementing Leader Election, Heartbeat Failure Detection, and a custom Peer-to-Peer Distributed Mutex.

---

## 2. System Architecture & Component Design

### 2.1 The API Gateway (Python Flask)
The Python backend serves a dual purpose:
1. **Frontend Host:** It serves the `index.html` dashboard, which includes high-precision latency tracking and a "Simulate Load" feature capable of firing 30 concurrent user requests instantly.
2. **Reverse Proxy Load Balancer:** When a user initiates a `BuyTicket` request, the Python gateway randomly selects an active Java node port (e.g., from `50051` to `50060`) and routes the HTTP request to it. This guarantees a chaotic, uncoordinated influx of traffic to the backend grid.

### 2.2 The Distributed Nodes (Java gRPC)
The backbone of the system is a highly configurable Java application utilizing the `io.grpc` library. The nodes operate entirely peer-to-peer. At startup, the cluster can be dynamically sized using terminal arguments:
* `-nodes=10`: Instructs the node that the cluster size is 10, meaning it will attempt to communicate with ports 50051 through 50060.
* `-leaders=5`: Defines the topology limit. Out of the 10 nodes, only the 5 with the lowest port numbers will be granted "Leader" status.
* `-sync`: Enables coordination mechanics (Leader Election, Proxying).
* `-mutex`: Enables the strict distributed lock protocol.

### 2.3 The Shared State (database.txt)
Instead of a robust database like PostgreSQL, the system intentionally relies on a raw text file (`database.txt`) to store the integer count of remaining tickets. Because the nodes interact with this file directly via raw OS file I/O streams without native locks, it serves as the perfect fragile medium to expose concurrency flaws.

---

## 3. Core Algorithmic Implementations

### 3.1 Bully Leader Election & Proxying
When the cluster boots in `-sync` mode, it executes a variant of the Bully Algorithm. Every node pings every other node in the `allPorts` array. The nodes sort the active respondents and the nodes with the lowest port numbers declare themselves Leaders.
* **Followers:** If a node is not a Leader, it refuses to touch the database. Instead, it acts as a proxy, forwarding the client's request to a randomly selected Leader via a dedicated `forwardBuy` gRPC call.

### 3.2 Fault Tolerance: Heartbeats & Re-elections
Distributed systems must assume hardware failure. Followers run a background daemon thread (`ScheduledExecutorService`) that pings the primary Leader every 3 seconds via a `heartbeat` RPC. 
If the Leader process dies (or network partitioning occurs), the Follower increments a failure counter. Upon hitting the threshold (2 consecutive failures), the Follower declares the Leader dead and forcefully triggers a cluster-wide Re-Election, allowing the system to self-heal. Additionally, if a `forwardBuy` proxy request times out, it immediately triggers a re-election.

### 3.3 Idempotency & Duplicate Request Handling
In distributed networks, "Timeout" does not mean "Failure"—it just means the response didn't arrive. If a gateway retries a request that was actually successful on the backend, a user would be double-charged.
To solve this, the frontend generates a unique UUID (`request_id`) for every click. Leaders maintain a `ConcurrentHashMap<String, BuyTicketResponse>`. Before processing any logic, the Leader checks if the UUID exists. If it does, it intercepts the execution and instantly returns the cached response, guaranteeing exactly-once semantics.

### 3.4 The Two-Phase Performance Funnel
To accurately model production systems, we decoupled the transaction into two phases to demonstrate why multi-leader topologies are faster:
1. **The Funnel (300ms):** This represents heavy, non-colliding business logic (e.g., verifying credit cards, encrypting data). It is protected only by a *Local Lock* (`ReentrantLock`).
2. **The Critical Section (30ms):** The actual reading and writing of `database.txt`. This is protected by the *Distributed Mutex*.

Because of this architectural split, if we configure `-leaders=5`, the 5 leaders process their 300ms funnels *in parallel*. Latency for 30 users drops from ~9 seconds (1 Leader) down to ~2 seconds (5 Leaders), proving that multi-leader setups achieve massive horizontal scaling while the mutex protects the tiny 30ms danger window.

### 3.5 The Peer-to-Peer Distributed Spinlock
Because there is no central orchestrator (like ZooKeeper), the Leaders must achieve mutual exclusion autonomously.
Before entering the 30ms Critical Section, a Leader sends a `requestLock` RPC to all other nodes. 
* If a peer is already holding or processing a lock, it replies `DENIED`.
* If a Leader receives even a single `DENIED`, it immediately sends `releaseLock` RPCs to undo any partial grants, backs off for a random jitter duration (50-200ms), and retries the entire process.
* It only enters the Critical Section when 100% of peers reply `GRANTED`.

---

## 4. Chronological Bug Reports & Engineering Resolutions

Building this system from scratch exposed us to incredibly complex distributed bugs. Here is a chronological post-mortem of the major issues we faced and solved:

### Bug 1: The "Fake Mutex" Illusion
* **The Symptom:** In the earliest iteration, enabling the `-mutex` flag did nothing. Race conditions still occurred.
* **The Cause:** The original code used a standard Java `ReentrantLock`. While this prevented threads *within the same JVM* from colliding, it offered absolutely zero protection across the physical network of 3 separate JVM processes.
* **The Fix:** We architected and implemented the true Peer-to-Peer Distributed Spinlock protocol via gRPC.

### Bug 2: The Race Condition Paradox (Too Fast to Collide)
* **The Symptom:** When testing 5 leaders without the mutex, we expected massive data corruption. However, the system processed requests perfectly with almost zero race conditions.
* **The Cause:** The 300ms simulated processing delay was placed *before* the file I/O, spreading the requests out over time. Because file I/O takes microseconds, the statistical probability of two nodes executing `Files.writeString` at the exact same microsecond was virtually zero.
* **The Fix:** We introduced an artificial `Thread.sleep(30)` *exactly* between the file read and file write. This artificially widened the "danger window," guaranteeing that overlapping nodes would read stale data and corrupt the database, perfectly demonstrating the flaw for presentation purposes.

### Bug 3: File I/O Truncation (`For input string: ""`)
* **The Symptom:** Under high load, Java would crash with `NumberFormatException: For input string: ""`.
* **The Cause:** When `Files.writeString` executes, the OS truncates the file to 0 bytes before writing the new string. With 5 un-synchronized leaders hammering the file, Node A would truncate the file just as Node B attempted to read it. Node B read an empty file and crashed.
* **The Fix:** We wrote a robust I/O wrapper. If `readTicketCount()` reads an empty string, it assumes a collision, sleeps for 5ms, and retries up to 10 times before failing.

### Bug 4: The Gridlock Deadlock (12-Second Latencies)
* **The Symptom:** When running 5 leaders *with* the Mutex, the system ground to a halt. Requests took 12.9 seconds, and eventually failed with "Leader unreachable, re-election in progress."
* **The Cause:** Inside the Distributed Mutex algorithm, if a lock was denied, the code executed `Thread.sleep(150)` to back off. However, this sleep was accidentally placed **inside** a `synchronized (mutexStateLock)` block. Because the thread slept while holding the monitor lock, incoming `releaseLock` RPCs from other nodes were blocked from executing. The entire cluster deadlocked itself waiting for locks to release.
* **The Fix:** We extracted the `Thread.sleep()` outside of the `synchronized` block, allowing nodes to gracefully step aside and process network messages instantly.

### Bug 5: Spinlock Queue Starvation (The 1-in-30 Failure)
* **The Symptom:** After fixing the deadlock, the system was lightning fast, but exactly 1 out of 30 users would fail with "Could not acquire distributed lock."
* **The Cause:** 5 Leaders hit the mutex at the exact same time, creating a massive queue of nodes backing off and retrying. The original retry limit was set to `maxRetries = 10`. One node simply got unlucky with the random backoff timings and hit its 10th retry, giving up prematurely.
* **The Fix:** We increased the spinlock patience. `maxRetries` was bumped to 50, and the gRPC connection deadline was increased from 10s to 20s, giving the chaotic queue plenty of time to resolve itself smoothly.

### Bug 6: Sibling-Thread State Corruption (The 1% Race Condition)
* **The Symptom:** Even with the Distributed Mutex enabled and functioning, we still observed exactly 1 race condition incident under extreme load.
* **The Cause:** The ultimate edge case. The Python gateway randomly routes requests. Thus, a single Leader JVM might receive 3 concurrent requests at the same time, processed by 3 sibling threads. Thread 1 acquires the Distributed Lock and enters the database. Simultaneously, Thread 2 tries to acquire the Distributed Lock, gets denied, and executes its failure cleanup: it sets `lockHeldByLocal = false` and broadcasts `releaseLock` to all peers. **Thread 2 accidentally unlocked the database globally while Thread 1 was still inside it!** Leader B saw the database was unlocked, rushed in, and corrupted the data.
* **The Fix:** We introduced a local `distributedMutexInternalLock` (`ReentrantLock`). This forces sibling threads on the same JVM to queue up locally *before* they are allowed to participate in the distributed peer-to-peer algorithm, entirely preventing internal state corruption.

---

## 5. Deep Dive: Hard Code Chunks Explained

### The Nested Mutex Safety Mechanism
The final implementation of the transaction logic requires highly defensive programming, heavily utilizing `try-finally` blocks to ensure locks are *always* released even if a thread crashes.

```java
// 1. Serialize sibling threads on the same JVM
distributedMutexInternalLock.lock();
try {
    // 2. Execute the Peer-to-Peer network algorithm
    boolean locked = acquireDistributedLock();
    if (!locked) {
        return; // Fails safely, jumps to the outer finally block to unlock sibling lock
    }

    try {
        // 3. The 30ms Danger Window
        int tickets = readTicketCount();
        Thread.sleep(30); 
        writeTicketCount(tickets - 1);
    } finally {
        // 4. Guaranteed global release, regardless of I/O errors
        releaseDistributedLock();
    }
} finally {
    // 5. Guaranteed local release, allowing the next sibling thread to proceed
    distributedMutexInternalLock.unlock();
}
```
**Why this matters:** The interplay between local locks (JVM memory) and distributed locks (Network State) is the hardest part of distributed systems. Forgetting a single `unlock()` in a `catch` block results in permanent cluster death.

---

## 6. System Limitations & Future Work

1. **No Persistent Logging (Raft/Paxos):** The system relies on a transient Spinlock. If a Leader hardware-faults *during* the critical section before sending `releaseLock` messages, the lock could technically hang on the peers (though gRPC deadlines mitigate this). A true production system would rely on State Machine Replication (Raft/Paxos) where consensus is achieved via log appending, rather than raw lock acquisitions.
2. **Network Partition Vulnerability (CAP Theorem):** The peer-to-peer mutex requires 100% unanimous consent. If one node's network cable is cut, no node can acquire the lock, prioritizing Consistency over Availability. A Quorum-based approach (majority consent, e.g., 6 out of 10 nodes) would make the system partition-tolerant.

---

## 7. Academic Defense Q&A

**Q1: Why did you build a custom peer-to-peer distributed lock instead of using a centralized orchestrator like Redis or ZooKeeper?**
*Answer:* Using a tool like Redis abstracts away the complexity of distributed consensus. A centralized sequencer is essentially a single point of failure and bottleneck. By building a peer-to-peer spinlock from scratch, we demonstrated the raw, foundational mechanics of distributed computing: handling network contention, algorithmic backoffs, local-state vs remote-state synchronization, and autonomous split-brain resolution.

**Q2: What exactly happens in your system if a Leader's JVM crashes while it holds the distributed lock?**
*Answer:* In our implementation, the lock state is heavily reliant on gRPC request/response mechanics. If a Leader dies, it won't send the unlock message. However, the peers holding the "Granted" state will eventually reset because we utilized gRPC timeouts (`withDeadlineAfter`). Furthermore, the Followers will detect the missed heartbeats via the background daemon and trigger a Bully re-election. To make the lock truly robust in a production setting, we would need to implement "Lock Leases" (Time-To-Live logic) so that locks autonomously expire on the peers.

**Q3: How does the system scale with 5 leaders if they all still have to wait in line for the mutex? Doesn't that defeat the purpose of having multiple leaders?**
*Answer:* This touches on the core theory of Amdahl's Law. The bottleneck in ticketing systems isn't the final database write, but the heavy business logic preceding it (credit card validation, queueing, fraud checks). We explicitly architected a "Two-Phase Funnel." The heavy processing (300ms) is completely decoupled from the Critical Section (30ms). 5 Leaders process the 300ms funnels concurrently, achieving massive horizontal scaling. The mutex only enforces serialization for the final 30ms.

**Q4: How did you ensure that network retries don't cause a user to buy two tickets accidentally?**
*Answer:* We implemented Idempotency Keys. The frontend JavaScript generates a unique UUID for every click. When a Leader completes a transaction, it caches the resulting object in a `ConcurrentHashMap` keyed by that UUID. If a network timeout causes the API Gateway to resend the request, the Leader intercepts the UUID, skips the database entirely, and returns the cached "Success" message, guaranteeing exactly-once semantics.

**Q5: Explain the "Sibling-Thread State Corruption" bug. How did a lock meant to prevent race conditions actually cause one?**
*Answer:* The distributed lock relied on a local boolean variable (`lockHeldByLocal`) to track its intent. Because the Flask gateway routed multiple concurrent HTTP requests to the *same* Java node, that JVM spawned multiple sibling threads. These sibling threads executed the distributed lock algorithm concurrently, overwriting the boolean. One thread would fail to acquire the lock and execute its cleanup routine (broadcasting "Unlock" to all peers), entirely unaware that its sibling thread had successfully acquired the lock and was inside the database. The cleanup effectively "stole" the lock from the sibling, globally unlocking the database and causing a race condition. We solved this by adding a local `ReentrantLock` to serialize access to the algorithm itself.
