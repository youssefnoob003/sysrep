package com.flashsale;

import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ServerNode — A single node in the Flash Sale Ticket Booking cluster.
 *
 * Usage:
 *   Without sync:  mvn exec:java -Dexec.mainClass="com.flashsale.ServerNode" -Dexec.args="50051"
 *   With sync:     mvn exec:java -Dexec.mainClass="com.flashsale.ServerNode" -Dexec.args="50051 -sync"
 *
 * Fixes applied:
 *   1. Leader Failure Recovery — heartbeat thread detects dead leader, triggers re-election
 *   2. Real Distributed Mutex — peers track lock state, deny if already held
 *   3. Idempotency — request_id deduplication prevents double-selling on retries
 */
public class ServerNode {

    // ─── Cluster topology ───
    private final int[] allPorts;
    private static final String DB_FILE  = "../database.txt";

    // ─── Instance state ───
    private final int port;
    private final boolean syncMode;
    private final int numLeaders;
    private final boolean useMutex;
    private volatile List<Integer> leaderPorts = new ArrayList<>();

    // Local mutex for file access within this JVM
    private final ReentrantLock localLock = new ReentrantLock();

    // Distributed lock state (used when this node is the leader)
    private final ReentrantLock distributedLock = new ReentrantLock(); // the 300ms funnel lock
    private final ReentrantLock distributedMutexInternalLock = new ReentrantLock(); // serializes access to the distributed mutex algorithm on the same node

    // Lock grant tracking — other nodes' permission for this leader to proceed
    private final Map<Integer, Boolean> lockGrants = new ConcurrentHashMap<>();

    // ─── FIX 1: Heartbeat — track consecutive failures for leader detection ───
    private final AtomicInteger heartbeatFailures = new AtomicInteger(0);
    private static final int MAX_HEARTBEAT_FAILURES = 2;
    private ScheduledExecutorService heartbeatScheduler;

    // ─── FIX 2: Real Mutex — actual lock state on each peer ───
    private volatile boolean lockHeldByRemote = false;
    private volatile boolean lockHeldByLocal = false;
    private int lockHolderPort = -1;
    private final Object mutexStateLock = new Object(); // guards lockHeldByRemote & lockHeldByLocal & lockHolderPort

    // ─── FIX 3: Idempotency — cache of processed request IDs ───
    private final ConcurrentHashMap<String, BuyTicketResponse> processedRequests = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ForwardBuyResponse> processedForwards = new ConcurrentHashMap<>();

    // ─── Sequential Processing for Unsync Mode ───
    // In unsync mode, process requests one at a time on this node (no inter-node coordination)
    private final ExecutorService unsyncSequentialExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "unsync-sequential-executor");
        t.setDaemon(true);
        return t;
    });

    public ServerNode(int port, boolean syncMode, int numLeaders, boolean useMutex, int numNodes) {
        this.port = port;
        this.syncMode = syncMode;
        this.numLeaders = numLeaders;
        this.useMutex = useMutex;
        this.allPorts = new int[numNodes];
        for (int i = 0; i < numNodes; i++) {
            this.allPorts[i] = 50051 + i;
        }
    }

    // ════════════════════════════════════════════════════════
    //  MAIN
    // ════════════════════════════════════════════════════════
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: ServerNode <port> [-sync]");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        boolean syncMode = false;
        boolean useMutex = false;
        int numLeaders = 1;
        int numNodes = 3;

        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("-sync")) syncMode = true;
            else if (args[i].equals("-mutex")) useMutex = true;
            else if (args[i].startsWith("-leaders=")) {
                numLeaders = Integer.parseInt(args[i].substring(9));
            } else if (args[i].startsWith("-nodes=")) {
                numNodes = Integer.parseInt(args[i].substring(7));
            }
        }

        ServerNode node = new ServerNode(port, syncMode, numLeaders, useMutex, numNodes);
        node.start();
    }

    private void start() throws Exception {
        Server server = ServerBuilder.forPort(port)
                .addService(new TicketServiceImpl())
                .build()
                .start();

        String mode = syncMode ? "SYNCHRONIZED (leader election + mutex)" : "UNSYNCHRONIZED (race conditions possible!)";
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("  Node started on port " + port);
        System.out.println("  Mode: " + mode);
        System.out.println("╚══════════════════════════════════════════════════╝");

        if (syncMode) {
            // Give other nodes a moment to start, then run election
            Thread.sleep(3000);
            runLeaderElection();

            // FIX 1: Start heartbeat monitoring after initial election
            startHeartbeatMonitor();
        }

        server.awaitTermination();
    }

    // ════════════════════════════════════════════════════════
    //  LEADER ELECTION (Bully-style: lowest port wins)
    // ════════════════════════════════════════════════════════
    private void runLeaderElection() {
        System.out.println("[ELECTION] Starting leader election...");

        List<Integer> respondingPorts = new ArrayList<>();
        respondingPorts.add(this.port);

        for (int peerPort : allPorts) {
            if (peerPort == this.port) continue;
            try {
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress("localhost", peerPort)
                        .usePlaintext()
                        .build();
                TicketServiceGrpc.TicketServiceBlockingStub stub =
                        TicketServiceGrpc.newBlockingStub(channel)
                                .withDeadlineAfter(3, TimeUnit.SECONDS);

                ElectionResponse resp = stub.requestElection(
                        ElectionRequest.newBuilder()
                                .setCandidatePort(this.port)
                                .build());

                if (resp.getAccepted()) {
                    respondingPorts.add(peerPort);
                }
                channel.shutdown();
            } catch (Exception e) {
                System.out.println("[ELECTION] Peer " + peerPort + " unreachable.");
            }
        }

        Collections.sort(respondingPorts);
        leaderPorts = respondingPorts.subList(0, Math.min(numLeaders, respondingPorts.size()));

        if (leaderPorts.contains(this.port)) {
            System.out.println("★★★ [ELECTION] I am a LEADER (port " + port + ") ★★★");
        } else {
            System.out.println("[ELECTION] Leaders are " + leaderPorts);
        }

        // Reset heartbeat failure counter after election
        heartbeatFailures.set(0);
    }

    // ════════════════════════════════════════════════════════
    //  FIX 1: HEARTBEAT MONITOR (detect leader failure)
    // ════════════════════════════════════════════════════════
    private void startHeartbeatMonitor() {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-monitor");
            t.setDaemon(true);
            return t;
        });

        heartbeatScheduler.scheduleAtFixedRate(() -> {
            if (leaderPorts.contains(this.port)) {
                // I am a leader, no need to monitor myself
                return;
            }
            if (leaderPorts.isEmpty()) {
                // No leader known, trigger election
                System.out.println("[HEARTBEAT] No leader known, triggering election...");
                runLeaderElection();
                return;
            }

            int targetLeader = leaderPorts.get(0);

            try {
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress("localhost", targetLeader)
                        .usePlaintext()
                        .build();
                TicketServiceGrpc.TicketServiceBlockingStub stub =
                        TicketServiceGrpc.newBlockingStub(channel)
                                .withDeadlineAfter(2, TimeUnit.SECONDS);

                HeartbeatResponse resp = stub.heartbeat(
                        HeartbeatRequest.newBuilder()
                                .setSenderPort(this.port)
                                .build());

                channel.shutdown();

                if (resp.getAlive()) {
                    heartbeatFailures.set(0); // Leader is alive, reset counter
                }
            } catch (Exception e) {
                int failures = heartbeatFailures.incrementAndGet();
                System.out.println("[HEARTBEAT] ❌ Leader " + targetLeader + " unreachable ("
                        + failures + "/" + MAX_HEARTBEAT_FAILURES + " failures)");

                if (failures >= MAX_HEARTBEAT_FAILURES) {
                    System.out.println("[HEARTBEAT] 💀 Leader presumed DEAD — triggering RE-ELECTION!");
                    heartbeatFailures.set(0);
                    runLeaderElection();
                }
            }
        }, 3, 3, TimeUnit.SECONDS); // Check every 3 seconds

        System.out.println("[HEARTBEAT] Monitoring started (every 3s, threshold=" + MAX_HEARTBEAT_FAILURES + ")");
    }

    // ════════════════════════════════════════════════════════
    //  DATABASE I/O
    // ════════════════════════════════════════════════════════
    private int readTicketCount() throws IOException {
        for (int i = 0; i < 10; i++) {
            String content = new String(Files.readAllBytes(Paths.get(DB_FILE))).trim();
            if (!content.isEmpty()) {
                return Integer.parseInt(content);
            }
            try { Thread.sleep(5); } catch (InterruptedException e) {}
        }
        return 0; // Fallback
    }

    private void writeTicketCount(int count) throws IOException {
        Files.writeString(Paths.get(DB_FILE), String.valueOf(count));
    }

    // ════════════════════════════════════════════════════════
    //  DISTRIBUTED LOCK (Leader requests permission from peers)
    //  FIX 2: Now with retry logic if denied
    // ════════════════════════════════════════════════════════
    private boolean acquireDistributedLock() {
        System.out.println("[MUTEX] Requesting distributed lock from peers...");

        int maxRetries = 50;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            boolean mustWait = false;
            synchronized (mutexStateLock) {
                if (lockHeldByRemote) {
                    mustWait = true;
                } else {
                    lockHeldByLocal = true;
                }
            }

            if (mustWait) {
                try { Thread.sleep((long) (Math.random() * 100) + 50); } catch (Exception e) {}
                continue; // Someone else holds it locally, wait and retry
            }

            boolean allGranted = true;

            for (int peerPort : allPorts) {
                if (peerPort == this.port) continue;
                try {
                    ManagedChannel channel = ManagedChannelBuilder
                            .forAddress("localhost", peerPort)
                            .usePlaintext()
                            .build();
                    TicketServiceGrpc.TicketServiceBlockingStub stub =
                            TicketServiceGrpc.newBlockingStub(channel)
                                    .withDeadlineAfter(2, TimeUnit.SECONDS);

                    LockResponse resp = stub.requestLock(
                            LockRequest.newBuilder()
                                    .setRequesterPort(this.port)
                                    .build());

                    channel.shutdown();

                    if (!resp.getGranted()) {
                        System.out.println("[MUTEX] Lock DENIED by peer " + peerPort
                                + " (attempt " + attempt + "/" + maxRetries + ")");
                        allGranted = false;
                        break; 
                    }
                } catch (Exception e) {
                    System.out.println("[MUTEX] Peer " + peerPort + " unreachable — DENIED.");
                    allGranted = false;
                    break;
                }
            }

            if (allGranted) {
                System.out.println("[MUTEX] ✅ Distributed lock ACQUIRED.");
                return true;
            }

            // Release any partial grants before retrying
            releaseDistributedLock();

            if (attempt < maxRetries) {
                try {
                    long backoff = (long) (Math.random() * 150) + 50; 
                    System.out.println("[MUTEX] Retrying in " + backoff + "ms...");
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }

        System.out.println("[MUTEX] ❌ Failed to acquire distributed lock after " + maxRetries + " attempts.");
        return false;
    }

    private void releaseDistributedLock() {
        synchronized (mutexStateLock) {
            lockHeldByLocal = false;
        }
        System.out.println("[MUTEX] Releasing distributed lock...");
        for (int peerPort : allPorts) {
            if (peerPort == this.port) continue;
            try {
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress("localhost", peerPort)
                        .usePlaintext()
                        .build();
                TicketServiceGrpc.TicketServiceBlockingStub stub =
                        TicketServiceGrpc.newBlockingStub(channel);

                stub.releaseLock(
                        UnlockRequest.newBuilder()
                                .setRequesterPort(this.port)
                                .build());

                channel.shutdown();
            } catch (Exception e) {
                System.out.println("[MUTEX] Could not reach peer " + peerPort + " for unlock.");
            }
        }
        System.out.println("[MUTEX] 🔓 Distributed lock RELEASED.");
    }

    // ════════════════════════════════════════════════════════
    //  gRPC SERVICE IMPLEMENTATION
    // ════════════════════════════════════════════════════════
    private class TicketServiceImpl extends TicketServiceGrpc.TicketServiceImplBase {

        // ─── Client-Facing: BuyTicket ───
        @Override
        public void buyTicket(BuyTicketRequest request, StreamObserver<BuyTicketResponse> responseObserver) {
            String buyer = request.getBuyerName();
            String requestId = request.getRequestId();
            System.out.println("\n═══ [BUY] Request from " + buyer + " received on port " + port
                    + " (request_id=" + requestId + ") ═══");

            if (!syncMode) {
                // ── UNSYNCHRONIZED MODE: direct file access, no coordination ──
                handleBuyUnsynchronized(buyer, responseObserver);
            } else {
                // ── SYNCHRONIZED MODE: coordinate via leader + mutex ──
                handleBuySynchronized(buyer, requestId, responseObserver);
            }
        }

        private void handleBuyUnsynchronized(String buyer, StreamObserver<BuyTicketResponse> responseObserver) {
            // Queue the request to process sequentially on this node
            unsyncSequentialExecutor.execute(() -> {
                try {
                    int tickets = readTicketCount();
                    System.out.println("[UNSYNC] " + buyer + ": Read ticket count = " + tickets);

                    // Artificial delay to show processing time
                    System.out.println("[UNSYNC] " + buyer + ": Sleeping 500ms (simulating processing)...");
                    Thread.sleep(500);

                    if (tickets > 0) {
                        int newCount = tickets - 1;
                        writeTicketCount(newCount);
                        System.out.println("[UNSYNC] ✅ " + buyer + ": BOUGHT ticket! Wrote count = " + newCount);

                        responseObserver.onNext(BuyTicketResponse.newBuilder()
                                .setSuccess(true)
                                .setMessage("Success: Ticket bought by " + buyer + "!")
                                .setRemainingTickets(newCount)
                                .setServedByPort(port)
                                .build());
                    } else {
                        System.out.println("[UNSYNC] ❌ " + buyer + ": SOLD OUT.");
                        responseObserver.onNext(BuyTicketResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Failed: Sold Out! No tickets left for " + buyer + ".")
                                .setRemainingTickets(0)
                                .setServedByPort(port)
                                .build());
                    }
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    System.err.println("[ERROR] " + e.getMessage());
                    responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
                }
            });
        }

        private void handleBuySynchronized(String buyer, String requestId, StreamObserver<BuyTicketResponse> responseObserver) {
            if (leaderPorts.isEmpty()) {
                // Election hasn't completed yet
                responseObserver.onNext(BuyTicketResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("System initializing — leader election in progress.")
                        .setRemainingTickets(-1)
                        .setServedByPort(port)
                        .build());
                responseObserver.onCompleted();
                return;
            }

            if (!leaderPorts.contains(port)) {
                // ── FOLLOWER: forward to leader ──
                int targetLeader = leaderPorts.get(new Random().nextInt(leaderPorts.size()));
                System.out.println("[SYNC] I am a FOLLOWER. Forwarding " + buyer + "'s request to Leader (port " + targetLeader + ")...");
                try {
                    ManagedChannel channel = ManagedChannelBuilder
                            .forAddress("localhost", targetLeader)
                            .usePlaintext()
                            .build();
                    TicketServiceGrpc.TicketServiceBlockingStub stub =
                            TicketServiceGrpc.newBlockingStub(channel)
                                    .withDeadlineAfter(20, TimeUnit.SECONDS);

                    ForwardBuyResponse fwdResp = stub.forwardBuy(
                            ForwardBuyRequest.newBuilder()
                                    .setBuyerName(buyer)
                                    .setOriginPort(port)
                                    .setRequestId(requestId)  // FIX 3: propagate request_id
                                    .build());

                    channel.shutdown();

                    responseObserver.onNext(BuyTicketResponse.newBuilder()
                            .setSuccess(fwdResp.getSuccess())
                            .setMessage(fwdResp.getMessage() + " (forwarded by port " + port + ")")
                            .setRemainingTickets(fwdResp.getRemainingTickets())
                            .setServedByPort(port)
                            .build());
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    // FIX 1: If forwarding fails, leader might be dead — trigger re-election
                    System.err.println("[ERROR] Forward failed: " + e.getMessage());
                    System.out.println("[SYNC] 💀 Leader unreachable during forward — triggering RE-ELECTION!");
                    heartbeatFailures.set(MAX_HEARTBEAT_FAILURES); // Force immediate re-election on next heartbeat
                    responseObserver.onError(Status.UNAVAILABLE
                            .withDescription("Leader unreachable, re-election in progress. Please retry.")
                            .asRuntimeException());
                }
            } else {
                // ── LEADER: process ──
                processWithMutex(buyer, requestId, responseObserver);
            }
        }

        private void processWithMutex(String buyer, String requestId, StreamObserver<BuyTicketResponse> responseObserver) {
            // FIX 3: Idempotency check — return cached response if already processed
            if (requestId != null && !requestId.isEmpty()) {
                BuyTicketResponse cached = processedRequests.get(requestId);
                if (cached != null) {
                    System.out.println("[IDEMPOTENCY] ♻️ Duplicate request_id=" + requestId + " — returning cached response.");
                    responseObserver.onNext(cached);
                    responseObserver.onCompleted();
                    return;
                }
            }

            System.out.println("[LEADER] Processing " + buyer + "'s request...");

            // 1. Acquire the local lock first (serializes requests within this JVM - bottleneck!)
            distributedLock.lock();
            try {
                System.out.println("[LEADER] ⌛ " + buyer + ": Heavy local processing (300ms bottleneck)...");
                Thread.sleep(300);
            } catch (Exception e) {} finally {
                distributedLock.unlock();
            }

            // 2. Distributed Critical Section
            if (useMutex) {
                distributedMutexInternalLock.lock();
            }
            try {
                if (useMutex) {
                    // Acquire distributed lock from peers
                    boolean locked = acquireDistributedLock();
                    if (!locked) {
                        responseObserver.onNext(BuyTicketResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Failed: Could not acquire distributed lock for " + buyer + ".")
                                .setRemainingTickets(-1)
                                .setServedByPort(port)
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }
                }

                try {
                    // Critical section: read → sleep → write
                    int tickets = readTicketCount();
                    System.out.println("[LEADER] 🔒 " + buyer + ": Read ticket count = " + tickets);

                    // 3. Artificially widen the race condition window so it happens reliably without mutex!
                    Thread.sleep(30);

                    BuyTicketResponse response;
                    if (tickets > 0) {
                        int newCount = tickets - 1;
                        writeTicketCount(newCount);
                        System.out.println("[LEADER] ✅ " + buyer + ": BOUGHT ticket! Wrote count = " + newCount);

                        response = BuyTicketResponse.newBuilder()
                                .setSuccess(true)
                                .setMessage("Success: Ticket bought by " + buyer + "!")
                                .setRemainingTickets(newCount)
                                .setServedByPort(port)
                                .build();
                    } else {
                        System.out.println("[LEADER] ❌ " + buyer + ": SOLD OUT.");
                        response = BuyTicketResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Failed: Sold Out! No tickets left for " + buyer + ".")
                                .setRemainingTickets(0)
                                .setServedByPort(port)
                                .build();
                    }

                    // FIX 3: Cache the response for idempotency
                    if (requestId != null && !requestId.isEmpty()) {
                        processedRequests.put(requestId, response);
                    }

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();

                } catch (Exception e) {
                    System.err.println("[ERROR] " + e.getMessage());
                    responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
                } finally {
                    if (useMutex) {
                        // Release distributed lock
                        releaseDistributedLock();
                    }
                }
            } finally {
                if (useMutex) {
                    distributedMutexInternalLock.unlock();
                }
            }
        }

        // ─── Client-Facing: GetTicketCount ───
        @Override
        public void getTicketCount(TicketCountRequest request, StreamObserver<TicketCountResponse> responseObserver) {
            try {
                int count = readTicketCount();
                responseObserver.onNext(TicketCountResponse.newBuilder().setCount(count).build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
            }
        }

        // ─── Node-to-Node: Leader Election ───
        @Override
        public void requestElection(ElectionRequest request, StreamObserver<ElectionResponse> responseObserver) {
            int candidatePort = request.getCandidatePort();
            System.out.println("[ELECTION] Received election request from candidate port " + candidatePort);

            // Accept the election — we'll determine the leader by comparing ports
            responseObserver.onNext(ElectionResponse.newBuilder()
                    .setAccepted(true)
                    .setLeaderPort(port)
                    .build());
            responseObserver.onCompleted();

            // Update our own leader view (not strictly necessary since we elect independently)
            if (leaderPorts.isEmpty()) {
                leaderPorts.add(Math.min(port, candidatePort));
                if (leaderPorts.contains(port)) {
                    System.out.println("★★★ [ELECTION] I am a LEADER (port " + port + ") ★★★");
                }
            }
        }

        // ─── Node-to-Node: Forward Buy (Leader receives forwarded requests) ───
        @Override
        public void forwardBuy(ForwardBuyRequest request, StreamObserver<ForwardBuyResponse> responseObserver) {
            String buyer = request.getBuyerName();
            int originPort = request.getOriginPort();
            String requestId = request.getRequestId();
            System.out.println("[LEADER] Received forwarded request for " + buyer + " from port " + originPort
                    + " (request_id=" + requestId + ")");

            // FIX 3: Idempotency check for forwarded requests
            if (requestId != null && !requestId.isEmpty()) {
                ForwardBuyResponse cached = processedForwards.get(requestId);
                if (cached != null) {
                    System.out.println("[IDEMPOTENCY] ♻️ Duplicate forward request_id=" + requestId + " — returning cached.");
                    responseObserver.onNext(cached);
                    responseObserver.onCompleted();
                    return;
                }
            }

            // Process, return via ForwardBuyResponse
            distributedLock.lock();
            try {
                System.out.println("[LEADER] ⌛ " + buyer + ": Heavy local processing (300ms bottleneck)...");
                Thread.sleep(300);
            } catch (Exception e) {} finally {
                distributedLock.unlock();
            }

            if (useMutex) {
                distributedMutexInternalLock.lock();
            }
            try {
                if (useMutex) {
                    boolean locked = acquireDistributedLock();
                    if (!locked) {
                        responseObserver.onNext(ForwardBuyResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Failed: Could not acquire distributed lock for " + buyer + ".")
                                .setRemainingTickets(-1)
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }
                }

                try {
                    int tickets = readTicketCount();
                    System.out.println("[LEADER] 🔒 " + buyer + ": Read ticket count = " + tickets);

                    // Race condition window
                    Thread.sleep(30);

                    ForwardBuyResponse response;
                    if (tickets > 0) {
                        int newCount = tickets - 1;
                        writeTicketCount(newCount);
                        System.out.println("[LEADER] ✅ " + buyer + ": BOUGHT ticket! Wrote count = " + newCount);

                        response = ForwardBuyResponse.newBuilder()
                                .setSuccess(true)
                                .setMessage("Success: Ticket bought by " + buyer + "!")
                                .setRemainingTickets(newCount)
                                .build();
                    } else {
                        System.out.println("[LEADER] ❌ " + buyer + ": SOLD OUT.");
                        response = ForwardBuyResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Failed: Sold Out! No tickets left for " + buyer + ".")
                                .setRemainingTickets(0)
                                .build();
                    }

                    // FIX 3: Cache the forward response
                    if (requestId != null && !requestId.isEmpty()) {
                        processedForwards.put(requestId, response);
                    }

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    System.err.println("[ERROR] " + e.getMessage());
                    responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
                } finally {
                    if (useMutex) {
                        releaseDistributedLock();
                    }
                }
            } finally {
                if (useMutex) {
                    distributedMutexInternalLock.unlock();
                }
            }
        }

        // ─── Node-to-Node: Distributed Mutex — RequestLock ───
        // FIX 2: Real mutex — track actual lock state, deny if already held
        @Override
        public void requestLock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
            int requester = request.getRequesterPort();
            synchronized (mutexStateLock) {
                if (!lockHeldByRemote && !lockHeldByLocal) {
                    lockHeldByRemote = true;
                    lockHolderPort = requester;
                    System.out.println("[MUTEX] Lock requested by port " + requester + " — GRANTED.");
                    responseObserver.onNext(LockResponse.newBuilder().setGranted(true).build());
                } else {
                    System.out.println("[MUTEX] Lock requested by port " + requester
                            + " — DENIED.");
                    responseObserver.onNext(LockResponse.newBuilder().setGranted(false).build());
                }
            }
            responseObserver.onCompleted();
        }

        // ─── Node-to-Node: Distributed Mutex — ReleaseLock ───
        // FIX 2: Real mutex — only the holder can release
        @Override
        public void releaseLock(UnlockRequest request, StreamObserver<UnlockResponse> responseObserver) {
            int requester = request.getRequesterPort();
            synchronized (mutexStateLock) {
                if (lockHolderPort == requester) {
                    lockHeldByRemote = false;
                    lockHolderPort = -1;
                    System.out.println("[MUTEX] Lock released by port " + requester + ".");
                }
            }
            responseObserver.onNext(UnlockResponse.newBuilder().setReleased(true).build());
            responseObserver.onCompleted();
        }

        // ─── Node-to-Node: Heartbeat ───
        // FIX 1: Leader responds to heartbeat pings from followers
        @Override
        public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
            responseObserver.onNext(HeartbeatResponse.newBuilder()
                    .setAlive(true)
                    .setLeaderPort(leaderPorts.isEmpty() ? -1 : leaderPorts.get(0))
                    .build());
            responseObserver.onCompleted();
        }
    }
}
