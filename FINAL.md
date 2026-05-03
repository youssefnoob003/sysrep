# Flash Sale Ticket System: Comprehensive Algorithm & Architecture Guide

## Table of Contents
1. [System Overview](#system-overview)
2. [Core Problem: Distributed Race Condition](#core-problem)
3. [The Solution Architecture](#solution-architecture)
4. [Leader Election Algorithm (Bully)](#leader-election)
5. [Distributed Mutex Protocol](#distributed-mutex)
6. [Database Access & Control](#database-access)
7. [Request Flow: Sync vs Unsync](#request-flow)
8. [Python-Java Communication (gRPC)](#python-java-communication)
9. [Three Improvements (Fixes)](#improvements)
10. [Limitations & Production Concerns](#limitations)

---

## System Overview

This system demonstrates how distributed systems prevent race conditions when multiple nodes compete for the same shared resource (ticket inventory).

**Setup:**
- 3 Java nodes on ports 50051, 50052, 50053
- 1 Python Flask gateway on port 5000
- Shared database: \`database.txt\` (single integer = ticket count)
- Communication: gRPC between nodes; HTTP from client to Python; gRPC from Python to nodes

**Two operational modes:**
1. **Unsynchronized (UNSYNC)** — Each node acts independently; demonstrates raw race condition
2. **Synchronized (SYNC)** — Nodes coordinate via leader + distributed mutex; prevents race condition

---

## Core Problem: Distributed Race Condition

### What is a Race Condition?

A race condition occurs when multiple processes compete to read/modify shared state, and the final result depends on **timing and execution order** rather than logic.

### The Flash Sale Race Condition

**Setup:** 1 ticket available, 3 buyers on 3 different nodes all request simultaneously.

\`\`\`
┌─ Ticket Count = 1 (database.txt) ─┐
│                                    │
Node A (port 50051):    Node B (port 50052):    Node C (port 50053):
1. Read: 1              1. Read: 1              1. Read: 1
2. Sleep 500ms          2. Sleep 500ms          2. Sleep 500ms
3. Write: 0  ✅ SOLD    3. Write: 0  ❌ WRONG   3. Write: 0  ❌ WRONG
│
└─ Result: All three buyers BOUGHT (2 tickets oversold!)
   Final count = 0, but 3 people got tickets
\`\`\`

**Why it happens:**
- All nodes READ before ANY node WRITES
- Each node thinks count=1, each decrements it independently
- No coordination = all writes use the same stale data

### Unsync Mode Behavior (Corrected)

In the corrected unsync mode:
- Each node processes buy requests **sequentially** (one at a time)
- But different nodes process **in parallel** (no inter-node coordination)
- This creates a genuine distributed race condition

\`\`\`
Timeline (all timestamps are overlapping):

Node A:  [Read=1] ────────[Sleep 500ms]──────── [Write=0] ✅
Node B:  [Read=1] ────────[Sleep 500ms]──────── [Write=0] ❌ overwrites A
Node C:  [Read=1] ────────[Sleep 500ms]──────── [Write=0] ❌ overwrites B

All three nodes read BEFORE ANY of them write.
Result: 3 sold, 1 available → 2 tickets oversold!
\`\`\`

---

## Solution Architecture

The solution prevents the race condition using **leader-follower pattern + distributed mutex**.

### High-Level Flow

\`\`\`
Client (Python)
       ↓
       ├─→ Random Node (Follower)
       │        ↓
       │     Forward to Leader
       │        ↓
       ├─→ Leader Node
               ↓
          [Acquire Distributed Lock]
          [Read ticket count]
          [Sleep (simulating work)]
          [Write ticket count]
          [Release Distributed Lock]
               ↓
            Response back through chain
\`\`\`

### Who Can Access the Database?

**Unsync Mode:**
- **ALL nodes** can read and write directly (no coordination)
- Each node reads the file independently
- Each node writes independently
- Creates race condition intentionally for demo

**Sync Mode:**
- **ONLY the leader** can write to the database
- Followers can read (for display purposes)
- All writes must be coordinated via:
  1. Leader acquisition of distributed lock from all peers
  2. Leader executes critical section (read → process → write)
  3. Leader releases distributed lock
  4. Followers must forward all BUY requests to the leader

**Access Control Pattern:**
\`\`\`
Follower receives buy request:
  ├─ Cannot write → Forward to leader
  ├─ Leader receives request
  ├─ Leader acquires lock from all peers
  ├─ Leader executes critical section
  ├─ Leader releases lock
  └─ Response propagates back to follower → client
\`\`\`

---

## Leader Election Algorithm (Bully)

### Algorithm: Bully Election

**Rule:** The node with the LOWEST PORT NUMBER becomes leader.

### Startup Election (\`runLeaderElection()\`)

\`\`\`java
Executed once at startup:

1. This node sends ElectionRequest to all other nodes
2. Other nodes respond ElectionResponse with accepted=true
3. This node compares ports:
   - If I have the lowest port → I am leader ★
   - Otherwise → the peer with lowest port is leader
4. All nodes now know: leaderPort = min(responding_ports)
5. Output: Print "[ELECTION] I am the LEADER" or "[ELECTION] Leader is node X"
\`\`\`

**Example with 3 nodes:**
- Port 50051 runs election
- Contacts 50052 and 50053, both respond
- Comparison: min(50051, 50052, 50053) = 50051
- Result: Port 50051 becomes leader

### Failure Detection & Re-election (\`startHeartbeatMonitor()\`)

Every 3 seconds, followers send heartbeat to leader:

\`\`\`
Follower (port 50052):
  Every 3 seconds:
    Try: Send HeartbeatRequest to port 50051
    
    Success (leader responds): 
      └─ Reset failure counter to 0
    
    Failure (timeout or error):
      ├─ Increment failure counter
      ├─ If failures >= 2:
      │   └─ Print "[HEARTBEAT] 💀 Leader presumed DEAD"
      │   └─ Trigger new election (runLeaderElection())
      └─ Otherwise, wait for next heartbeat

Leader doesn't monitor itself (no heartbeat needed)
\`\`\`

**Key insight:** If the leader dies and doesn't respond to 2 consecutive heartbeats, followers detect it and elect a new leader.

---

## Distributed Mutex Protocol

### What is a Distributed Mutex?

A traditional mutex (lock) works within ONE process. A **distributed mutex** coordinates access across multiple processes/machines.

**Purpose:** Only ONE leader can execute the critical section at a time.

### Lock State Tracking

Each node maintains lock state for incoming requests:

\`\`\`java
// On each node
private volatile boolean lockHeldByRemote = false;  // Is lock currently held?
private int lockHolderPort = -1;                    // Who holds it?
private final Object mutexStateLock = new Object(); // Synchronize access
\`\`\`

### Lock Acquisition (\`acquireDistributedLock()\`)

When the leader wants to execute a buy request:

\`\`\`
Leader (port 50051) wants to buy:
  
  1. Send LockRequest to all peers (50052, 50053)
     "I want to acquire lock, can I?"
  
  2. Each peer responds LockResponse with granted=true/false
     Peer 50052: granted=true  ✓ (lock is free)
     Peer 50053: granted=true  ✓ (lock is free)
  
  3. Leader checks: ALL peers granted?
     YES → Lock acquired successfully
     NO → Retry or fail
  
  4. Leader executes critical section (PROTECTED):
     - Read ticket count
     - Sleep 500ms
     - Write ticket count (only now is it safe!)
\`\`\`

### Lock Release (\`releaseDistributedLock()\`)

After leader finishes:

\`\`\`
Leader sends UnlockRequest to all peers:
  "Release the lock I was holding"

Each peer responds:
  - Clear lockHeldByRemote = false
  - Set lockHolderPort = -1
  - Now other leaders can acquire lock
\`\`\`

### Lock Denial Scenario

\`\`\`
Leader A (50051) holds lock (executing buy):
  
Leader B (50053) tries to acquire lock:
  - Sends LockRequest to peer 50052
  - Peer 50052 checks: Is lock free?
    lockHeldByRemote = true (held by port 50051)
    ├─ DENY: return granted=false
  - Leader B fails to get lock
  - Leader B retries after 200ms backoff
\`\`\`

### The "Real" Mutex Fix

Original implementation: Always granted = true (fake!)
Fixed implementation: Track actual lock state, deny if held

\`\`\`java
// BEFORE (Fake Mutex): Always grant
if (true) {  // Always!
    responseObserver.onNext(LockResponse.newBuilder().setGranted(true).build());
}

// AFTER (Real Mutex): Check state
synchronized (mutexStateLock) {
    if (!lockHeldByRemote) {
        lockHeldByRemote = true;        // Mark as held
        lockHolderPort = requester;     // Remember who holds it
        responseObserver.onNext(LockResponse.newBuilder().setGranted(true).build());
    } else {
        responseObserver.onNext(LockResponse.newBuilder().setGranted(false).build());
    }
}
\`\`\`

---

## Database Access & Control

### File: \`database.txt\`

\`\`\`
Contents: Single integer (ticket count)
Example:  1

Location: ../database.txt (relative to maven_cluster)
\`\`\`

### Read Operation

\`\`\`java
private int readTicketCount() throws IOException {
    String content = new String(Files.readAllBytes(Paths.get(DB_FILE))).trim();
    return Integer.parseInt(content);
}
\`\`\`

**Access pattern:**
- Unsync: All nodes read directly (any time)
- Sync: Only leader reads, and only inside critical section (after lock acquired)

### Write Operation

\`\`\`java
private void writeTicketCount(int count) throws IOException {
    Files.writeString(Paths.get(DB_FILE), String.valueOf(count));
}
\`\`\`

**Access pattern:**
- Unsync: All nodes write directly (creates race condition)
- Sync: Only leader writes, and only inside critical section (protected by lock)

### Critical Section (Sync Mode)

\`\`\`
Critical Section = Inside Mutex Lock:

  ┌─────────────────────────────────────┐
  │ [Start] Lock acquired from all peers │
  │                                      │
  │ 1. int tickets = readTicketCount()  │ Read is now safe
  │ 2. Thread.sleep(500)                │ Simulate processing
  │ 3. if (tickets > 0)                 │
  │      writeTicketCount(tickets - 1)  │ Write is now safe
  │                                      │
  │ [End] Lock released to all peers     │
  └─────────────────────────────────────┘

Key: No other leader can enter this section until lock is released.
\`\`\`

---

## Request Flow: Sync vs Unsync

### Unsync Mode (Parallel, No Coordination)

\`\`\`
Python Client:
  ├─→ POST /api/buy (buyer_name="Alice")
  │        ↓
  │     Random node selection → port 50051
  │        ↓
  │     ServerNode.buyTicket() on port 50051
  │        ↓
  │     handleBuyUnsynchronized():
  │        ├─ Queue to unsyncSequentialExecutor
  │        ├─ [This node processes sequentially]
  │        └─ One at a time per node
  │        ↓
  │     [Read] tickets = 1
  │     [Sleep] 500ms
  │     [Write] tickets = 0
  │        ↓
  │     Response: "Success, bought!"
  │
  └─ PROBLEM: Node B & C also doing this in parallel!
     All read before any write → all sell the same ticket
\`\`\`

**Timing Chart:**
\`\`\`
Node A:  |--Read--| (tickets=1)
Node B:  |--Read--| (tickets=1)
Node C:  |--Read--| (tickets=1)
         |Sleep 500ms...
         ...all sleeping...
         |-----------Write 0-| ✅
         |-----------Write 0-| ❌ overwrites A
         |-----------Write 0-| ❌ overwrites B
\`\`\`

### Sync Mode (Sequential Coordination)

\`\`\`
Python Client:
  ├─→ POST /api/buy (buyer_name="Alice", request_id=UUID)
  │        ↓
  │     Random node selection → port 50052 (Follower)
  │        ↓
  │     ServerNode.buyTicket() on port 50052
  │        ↓
  │     Is port 50052 the leader? NO
  │        ├─ Forward to leader (port 50051)
  │        └─ Wait for leader response
  │
  ├→ Leader (port 50051) receives ForwardBuyRequest
  │        ↓
  │     [ACQUIRE LOCK]
  │     - Request lock from ports 50052, 50053
  │     - Wait for: granted ✓, granted ✓
  │     - Lock acquired!
  │        ↓
  │     [CRITICAL SECTION]
  │     - Read tickets = 1
  │     - Sleep 500ms
  │     - Write tickets = 0
  │     - Cache response (idempotency)
  │        ↓
  │     [RELEASE LOCK]
  │     - Tell peers: unlock
  │        ↓
  │     Response back to follower
  │        ↓
  │  Response back to client
\`\`\`

**Timing Chart:**
\`\`\`
Node A (Leader):  |Locked| Read=1, Sleep, Write=0 |Unlocked|
                                                            ↑
                  Only now can other leaders proceed
                  
Node B (Follower): Forward to A → Wait → Get response
\`\`\`

---

## Python-Java Communication (gRPC)

### How Python Talks to Java

**Protocol:** gRPC with Protocol Buffers

### Step 1: Python Generates Stub

\`\`\`python
# webapp/app.py

import ticket_pb2
import ticket_pb2_grpc

def get_random_stub():
    port = random.choice([50051, 50052, 50053])
    channel = grpc.insecure_channel(f'localhost:{port}')
    stub = ticket_pb2_grpc.TicketServiceStub(channel)  # ← Autogenerated from .proto
    return stub, port
\`\`\`

Proto definition (auto-generates Python and Java):
\`\`\`protobuf
message BuyTicketRequest {
    string buyer_name = 1;
    string request_id = 2;
}

service TicketService {
    rpc BuyTicket (BuyTicketRequest) returns (BuyTicketResponse);
}
\`\`\`

### Step 2: Python Sends Buy Request

\`\`\`python
@app.route('/api/buy', methods=['POST'])
def buy_ticket():
    data = request.get_json()  # {'buyer_name': 'Alice'}
    buyer_name = data.get('buyer_name', 'Unknown')
    
    stub, port = get_random_stub()
    request_id = str(uuid.uuid4())  # Generate unique ID for idempotency
    
    # Call gRPC service on Java node
    response = stub.BuyTicket(
        ticket_pb2.BuyTicketRequest(
            buyer_name=buyer_name,
            request_id=request_id
        ),
        timeout=15  # Wait max 15 seconds
    )
    
    return jsonify({
        'success': response.success,
        'message': response.message,
        'remaining_tickets': response.remaining_tickets,
        'served_by_port': response.served_by_port,
        'routed_to_port': port
    })
\`\`\`

### Step 3: Java Receives Request

\`\`\`java
// ServerNode.java
private class TicketServiceImpl extends TicketServiceGrpc.TicketServiceImplBase {
    @Override
    public void buyTicket(BuyTicketRequest request, 
                          StreamObserver<BuyTicketResponse> responseObserver) {
        String buyer = request.getBuyerName();          // ← From Python
        String requestId = request.getRequestId();      // ← From Python
        
        // Process and send back response
        responseObserver.onNext(BuyTicketResponse.newBuilder()
            .setSuccess(true)
            .setMessage("Success!")
            .setRemainingTickets(0)
            .setServedByPort(port)
            .build());
        responseObserver.onCompleted();
    }
}
\`\`\`

### Message Flow Diagram

\`\`\`
Browser Client
    ↓ HTTP POST
    ↓ (/api/buy)
    ↓
Python Flask (port 5000)
    ├─ Generate request_id (UUID)
    ├─ Choose random Java port
    │
    ↓ gRPC (Protocol Buffer)
    ↓ (BuyTicketRequest)
    ↓
Java Node (port 50051/50052/50053)
    ├─ If follower: forward to leader
    ├─ If leader: acquire lock, execute, release lock
    │
    ↓ gRPC (BuyTicketResponse)
    ↓
Python Flask
    ├─ Convert to JSON
    ├─ Return to browser
    ↓ HTTP Response
    ↓
Browser displays result
\`\`\`

---

## Three Improvements (Fixes)

### Fix 1: Leader Failure Detection (Heartbeat)

**Problem:** If leader dies, followers don't know. They keep trying to forward requests to a dead node.

**Solution:** Heartbeat monitoring (every 3 seconds)

\`\`\`java
// heartbeatScheduler.scheduleAtFixedRate()
private void startHeartbeatMonitor() {
    heartbeatScheduler.scheduleAtFixedRate(() -> {
        if (leaderPort == this.port) return;  // I'm the leader, skip
        
        try {
            HeartbeatResponse resp = stub.heartbeat(
                HeartbeatRequest.newBuilder()
                    .setSenderPort(this.port)
                    .build());
            heartbeatFailures.set(0);  // ✓ Leader alive, reset
        } catch (Exception e) {
            int failures = heartbeatFailures.incrementAndGet();
            if (failures >= MAX_HEARTBEAT_FAILURES) {  // 2 failures
                System.out.println("[HEARTBEAT] 💀 Leader dead — RE-ELECTION!");
                runLeaderElection();  // Trigger new election
            }
        }
    }, 3, 3, TimeUnit.SECONDS);  // Every 3 seconds
}
\`\`\`

**What happens:**
- Follower pings leader every 3 seconds
- If leader doesn't respond 2 times in a row → presume dead
- Immediately trigger new election
- New leader takes over

### Fix 2: Real Distributed Mutex

**Problem:** Original code always granted lock (lockHeldByRemote always false). Fake mutex!

**Solution:** Track actual lock state per peer

\`\`\`java
// Before: Always grants
if (true) {  // ❌ FAKE!
    responseObserver.onNext(LockResponse.newBuilder().setGranted(true).build());
}

// After: Track state
synchronized (mutexStateLock) {
    if (!lockHeldByRemote) {  // Is lock free?
        lockHeldByRemote = true;
        lockHolderPort = requester;  // Remember who holds it
        responseObserver.onNext(LockResponse.newBuilder().setGranted(true).build());  // ✓ GRANT
    } else {  // Lock already held
        responseObserver.onNext(LockResponse.newBuilder().setGranted(false).build());  // ✗ DENY
    }
}

// On unlock
synchronized (mutexStateLock) {
    if (lockHolderPort == requester) {
        lockHeldByRemote = false;  // Free the lock
        lockHolderPort = -1;
    }
}
\`\`\`

### Fix 3: Idempotency via Request ID Deduplication

**Problem:** If a request times out and client retries, leader processes it twice → sells 2 tickets for 1 buy.

**Solution:** Cache responses by request_id

\`\`\`java
private void processWithMutex(String buyer, String requestId, 
                               StreamObserver<BuyTicketResponse> responseObserver) {
    
    // Check cache first
    if (requestId != null && !requestId.isEmpty()) {
        BuyTicketResponse cached = processedRequests.get(requestId);
        if (cached != null) {
            System.out.println("[IDEMPOTENCY] ♻️ Duplicate request_id — returning cached response");
            responseObserver.onNext(cached);  // Return same response without reprocessing
            responseObserver.onCompleted();
            return;
        }
    }
    
    // Process normally
    // ... acquire lock, read, sleep, write ...
    
    // Cache result for future retries
    if (requestId != null && !requestId.isEmpty()) {
        processedRequests.put(requestId, response);  // Remember for duplicates
    }
}
\`\`\`

**Flow:**
\`\`\`
First request (id=ABC):
  ├─ Not in cache
  ├─ Acquire lock, execute, cache response
  └─ Return: "Sold!"

Retry (same id=ABC):
  ├─ Check cache: found!
  ├─ Skip execution
  └─ Return same: "Sold!"  (consistent, no double-sell)
\`\`\`

---

## Limitations & Production Concerns

### Current Limitations

#### 1. Hard-Coded Topology
**Problem:** Node ports are hardcoded in \`ALL_PORTS = {50051, 50052, 50053}\`
\`\`\`java
private static final int[] ALL_PORTS = {50051, 50052, 50053};
\`\`\`
**Cannot scale dynamically.** If you add node 50054, code doesn't know about it.

**Production:** Use service discovery (Consul, Kubernetes DNS, ZooKeeper)

#### 2. Trivial Database (Text File)
**Problem:** \`database.txt\` with single integer
- Not ACID-compliant
- No transaction guarantees
- File corruption possible

**Production:** Use PostgreSQL with transactions:
\`\`\`sql
BEGIN TRANSACTION;
  SELECT count FROM tickets WHERE id=1 FOR UPDATE;  -- Lock row
  UPDATE tickets SET count = count - 1 WHERE id=1;
COMMIT;
\`\`\`

#### 3. No Network Partition Handling (Split-Brain)
**Problem:** If network splits, two leaders can elect themselves

**Production:** Use consensus algorithm (Raft, Paxos)

#### 4. No Persistence (Leader Election)
**Problem:** Leader info lost if all nodes restart

**Production:** Persist leader info to stable storage

#### 5. Blocking Synchronous Calls
**Problem:** Followers block waiting for leader responses

**Production:** Use async calls with timeouts, circuit breakers

#### 6. JVM Pause During Lock
**Problem:** GC can pause execution during critical section

**Production:** Use low-latency JVM tuning, real-time OS

#### 7. No Request Deduplication Across Nodes
**Problem:** Idempotency cache only on leader

**Production:** Write idempotency proof to replicated database

#### 8. Hard-Coded Timeouts
**Problem:** Network latencies not adaptive

**Production:** Adaptive timeouts based on latency histogram

#### 9. No Encryption
**Problem:** gRPC calls use plain text

**Production:** Use TLS certificates

#### 10. No Graceful Shutdown
**Problem:** Executor services don't drain queued requests

**Production:** Signal handler + graceful drain

---

## Summary: What Happens in Each Mode

### Unsync Mode (Race Condition Demonstration)

All requests processed in parallel on each node, creates race condition across nodes.

### Sync Mode (Race Condition Prevented)

All requests routed to leader, processed sequentially with distributed lock protection.

---

## Conclusion

This Flash Sale system demonstrates core distributed systems concepts: leader election, distributed mutex, heartbeat monitoring, and request idempotency. The limitations are intentional for learning purposes. Production systems use Raft/Paxos consensus, replicated databases, service discovery, and sophisticated failure detection.
