# Part 5: Lifecycle of Write and Read Queries and Load


Apache Doris separates query processing into two distinct planes: the Frontend (FE) plane, which handles metadata, transaction coordination, and distributed planning, and the Backend (BE) plane, which handles data ingestion, storage, and execution. Understanding how a write query transforms from a SQL statement into durable, replicated columnar files—and how a read query navigates those files without locking—requires tracing the full lifecycle of both operations.

FE parses SQL, resolves target table/partitions, computes target Tablets, and generates an execution plan dispatched to BEs.

## 5.1 Writing Queries in Apache Doris

A write query in Doris (whether an INSERT, STREAM LOAD, or BROKER LOAD) is not considered successful until a majority of Tablet replicas have durably persisted the data and the FE has committed the transaction. The lifecycle proceeds through five distinct stages.

Stage 1: SQL Parsing & Distributed Planning (FE)

When a write query arrives at the FE, it first undergoes parsing and semantic analysis. The FE determines which table and partitions are targeted, resolves the partition keys (for range or list partitioning), and computes the target Tablets using the table's distribution key (hash or random).

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 1: Write Planning (FE)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Client ──► FE Coordinator                                                 │
│                │                                                            │
│                ├── Parse SQL / Load Request                                 │
│                ├── Resolve Table, Partition, Distribution Key               │
│                └── Compute Target Tablets (Bucket-Hash Mapping)             │
│                                                                             │
│   Output: List of (Tablet ID → BE Node) mappings                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

The FE does not touch the data itself. Instead, it generates a plan fragment that instructs each BE node which Tablets it is responsible for receiving.

Stage 2: Data Shipment & DeltaWriter Creation (BE)

The FE dispatches the plan to the relevant BE nodes. Each BE creates a DeltaWriter for every Tablet replica it hosts. The DeltaWriter is the per-replica, per-transaction component responsible for buffering incoming rows.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 2: Ingestion & Buffering (BE)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   FE ──► BE-1 (Tablet Replica R1) ──► DeltaWriter ──► MemTable              │
│      ──► BE-2 (Tablet Replica R2) ──► DeltaWriter ──► MemTable              │
│      ──► BE-3 (Tablet Replica R3) ──► DeltaWriter ──► MemTable              │
│                                                                             │
│   • Rows arrive via BRPC/HTTP                                               │
│   • Appended to an in-memory row vector, UNSORTED; no aggregation yet       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Each replica maintains its own independent MemTable. There is no shared memory state across replicas.

Stage 3: MemTable Flush & Segment Formation (BE)

When a MemTable reaches its size threshold (default 200MB, adaptive; 100MB for aggregate-key tables), or when the load task signals completion, the DeltaWriter triggers a flush. The MemTable's sorted data is encoded, compressed, and written to one or more Segment files. The flush produces a new, immutable Rowset with a unique version.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 3: Flush to Immutable Storage                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   MemTable (Memory)                                                         │
│      │ flush(commit)                                                        │
│      ▼                                                                      │
│   Rowset [v2-v2] (Immutable)                                                │
│      ├── Segment 1 (columnar, encoded, compressed)                          │
│      └── Segment 2 (~230MB = 90% of the 256MB cap)                          │
│                                                                             │
│   • Rowset metadata registered in local TabletMeta                          │
│   • Physical files written to tablet data directory                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Stage 4: Replication & Quorum Commit (FE-BE Coordination)

This is the critical consistency stage. The FE acts as a transaction coordinator. It waits for each BE to report the success of its local flush. An import is considered committed only when more than half of the replicas (simple majority) have successfully persisted their Rowsets.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 4: Two-Phase Commit                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   BE-1 ──► "Flush Success (v2)" ──┐                                         │
│   BE-2 ──► "Flush Success (v2)" ──┼──► FE Coordinator                       │
│   BE-3 ──► "Flush Failed" ────────┘                                         │
│                                                                             │
│   FE Decision: 2 of 3 succeeded → MAJORITY REACHED → COMMIT                 │
│                                                                             │
│   • Transaction marked committed in FE journal                              │
│   • New version visible to subsequent queries                               │
│   • Failed replica (BE-3) will be repaired asynchronously                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

If the majority is not reached, the FE aborts the transaction. The successful BE replicas roll back their uncommitted Rowsets.

Stage 5: Background Compaction (BE)

After commit, the new Rowset is active and queryable. However, to maintain read efficiency, the BE periodically schedules Compaction jobs. Compaction reads multiple small Rowsets, merges them into a larger, more efficient Rowset, and atomically replaces the old versions in TabletMeta.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 5: Background Compaction                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Rowset v1 + Rowset v2 + Rowset v3                                         │
│      │                                                                      │
│      ▼ Compaction Task                                                      │
│   Rowset v4 (merged, deduplicated, re-indexed)                              │
│      │                                                                      │
│      └── TabletMeta updated: v4 visible, v1-v3 obsolete                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Compaction is entirely asynchronous and does not block writes or reads.

## 5.2 Reading Queries in Apache Doris

A read query in Doris must navigate the distributed, versioned storage without acquiring locks. It does so by leveraging FE metadata for routing and BE-level MVCC for consistency.

Stage 1: SQL Parsing & Distributed Planning (FE)

Like writes, read queries begin at the FE. The parser analyzes the SQL, the analyzer resolves table schemas, and the optimizer generates a distributed execution plan. For reads, the optimizer performs two critical optimizations:

Partition Pruning: Eliminates partitions that cannot contain relevant data based on filter predicates. Tablet Routing: Identifies which Tablets (and therefore which BE nodes) host the required data.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 1: Read Planning (FE)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Client ──► FE                                                             │
│                │                                                            │
│                ├── Parse & Analyze SQL                                      │
│                ├── Partition Pruning (range/list elimination)               │
│                ├── Tablet Routing (hash bucket mapping)                     │
│                └── Generate Plan Fragments (ScanNode, ExchangeNode, etc.)   │
│                                                                             │
│   Output: BE execution plan + list of target Tablets                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Stage 2: Tablet Routing & Replica Selection (FE)

The FE does not send the query to every replica. Instead, it selects one healthy replica per Tablet to serve the query. The selection algorithm considers:

Version completeness: The replica must contain all committed versions up to the query's snapshot timestamp. Data health: Avoid replicas with missing or corrupt segments. Load balancing: Distribute scan tasks evenly across BE nodes.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 2: Replica Selection                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Tablet T1 ──► Replica R1 @ BE-1  ──┐                                      │
│             ──► Replica R2 @ BE-2  ──┼──► FE selects R1 (healthy, latest)   │
│             ──► Replica R3 @ BE-3  ──┘                                      │
│                                                                             │
│   Query routed to BE-1 for Tablet T1 scan                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Stage 3: Version Selection & MVCC (BE)

Upon receiving the plan, the BE's TabletScanner opens the target Tablet. It consults the local TabletMeta to determine which Rowsets are visible at the query's snapshot version. Because Rowsets are immutable and versioned, the scanner sees a consistent, point-in-time view of the data without locking.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 3: MVCC Version Selection                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   TabletMeta VersionGraph                                                   │
│   ├── Rowset v1 [0-1]  ──┐                                                  │
│   ├── Rowset v2 [2-2]  ──┼──► Visible to query (snapshot version = 2)       │
│   └── Rowset v3 [3-3]  ──┘                                                  │
│                                                                             │
│   • Compacted Rowset v4 [0-3] may also be visible (if compaction finished)  │
│   • Stale Rowsets (replaced by compaction) are excluded                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Stage 4: Segment Scanning & Predicate Pushdown (BE)

For each visible Rowset, the TabletScanner opens its constituent Segments. Doris employs aggressive predicate pushdown and zone map pruning:

ZoneMap Index: Each Segment stores min/max statistics per column. If a predicate (age > 100) contradicts the ZoneMap, the entire Segment is skipped. BloomFilter: For equality predicates, BloomFilters eliminate pages that cannot contain the target value. Ordinal Index: Maps row IDs to file offsets for efficient seeking. Page-level Decompression: Only relevant 64KB Pages are read and decompressed.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 4: Segment Scan & Pruning                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Segment File                                                              │
│   ├── ZoneMap: {age: [10, 90]}                                              │
│   │      └── Predicate: age > 100 → SKIP ENTIRE SEGMENT                     │
│   │                                                                         │
│   ├── ZoneMap: {age: [95, 150]}                                             │
│   │      └── Predicate: age > 100 → READ                                    │
│   │          ├── BloomFilter: city = 'NYC' → May contain                    │
│   │          └── Page 1, Page 3 decompressed and scanned                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Stage 5: Result Aggregation & Return (BE → FE)

Scanned data is processed through the execution pipeline (aggregation, sorting, joins) within the BE. Intermediate results are streamed back to the FE via the root fragment's result sink (over the same brpc channel family that carries shuffle data). The FE performs final merging (if necessary) and returns the result set to the client.

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Stage 5: Distributed Execution & Return                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   BE-1 (Scan T1) ──┐                                                        │
│   BE-2 (Scan T2) ──┼──► ResultSink ──► FE ──► Client                        │
│   BE-3 (Scan T3) ──┘                                                        │
│                                                                             │
│   • Streaming aggregation reduces memory pressure                           │
│   • Final merge at FE for ORDER BY / LIMIT                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 5.3 Load Channel and MemTable

---
