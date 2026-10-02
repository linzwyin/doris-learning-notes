# Part 1: Overview and Core Concepts


## 1.1 What is Apache Doris

Apache Doris is a high-performance, real-time analytical database (OLAP) that delivers sub-second interactive queries. It is designed to provide low-latency, high-concurrency analytics on massive datasets while simplifying operational complexity.

Core characteristics:

 - MPP (Massively Parallel Processing) distributed execution engine
 - Columnar storage and vectorized query execution
 - Standard SQL interface with MySQL protocol compatibility
 - Unified support for both real-time data ingestion and batch loading
 - Built-in high availability without external dependencies (e.g., ZooKeeper)

## 1.2 Cluster Architecture Overview

An Apache Doris cluster consists of two core types of nodes and two optional variants:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Apache Doris Cluster Architecture                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐         ┌────────────────────────────────────────────┐    │
│   │   Client    │◄───────►│  FE Cluster (Frontend)                     │    │
│   │ (MySQL CLI/ │  SQL    │  ┌─────────┐  ┌─────────┐  ┌─────────┐     │    │
│   │  JDBC/etc)  │         │  │ Leader  │  │Follower │  │Follower │     │    │
│   └─────────────┘         │  │         │  │         │  │         │     │    │
│                           │  └────┬────┘  └────┬────┘  └────┬────┘     │    │
│                           │       └────────────┴────────────┘          │    │
│                           │              BDB-JE Replication            │    │
│                           │              (+ 0-N Observers, async)      │    │
│                           └──────────────────┬─────────────────────────┘    │
│                                              │                              │
│                           ┌──────────────────┼─────────────────────────┐    │
│                           │  BE Cluster (Backend)                      │    │
│                           │  ┌─────────┐  ┌─────────┐  ┌─────────┐     │    │
│                           │  │  BE-1   │◄─┤  BE-2   │◄─┤  BE-3   │     │    │
│                           │  │ (Tablet │  │ (Tablet │  │ (Tablet │     │    │
│                           │  │ Replica)│  │ Replica)│  │ Replica)│     │    │
│                           │  └─────────┘  └─────────┘  └─────────┘     │    │
│                           │  ┌─────────┐  ┌─────────┐  (optional)      │    │
│                           │  │  CN-1   │  │  CN-2   │  Compute Nodes:  │    │
│                           │  │(Compute │  │(Compute │  no tablet,      │    │
│                           │  │  only)  │  │  only)  │  query/ETL only  │    │
│                           │  └─────────┘  └─────────┘                  │    │
│                           │         Inter-BE Shuffle/Replication       │    │
│                           └────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

 - FE (Frontend): Responsible for metadata management, query planning, and cluster coordination.
 - BE (Backend): Responsible for data storage, query execution, and data ingestion.
 - FE (Observer) (optional): A read-only FE role. Receives journals from the Leader asynchronously, but holds no vote and never joins leader election. Used for read scaling and cross-region deployment. (Detailed in Part 2.)
 - BE (Compute Node) (optional): A stateless BE variant (be_node_role=computation) that hosts no Tablet replicas and only executes queries. Can be added or removed at will for elastic compute scaling. (Details in Section 1.6.)

## 1.3 Concept Mapping with Hadoop/HDFS (Quick Reference)

If you are familiar with HDFS, the following mapping will help you quickly understand Doris:

```text
 ┌─────────────────┬─────────────────┐
 │ HDFS            │ Doris           │
 ├─────────────────┼─────────────────┤
 │ NameNode        │ FE (Frontend)   │
 │ DataNode        │ BE (Backend)    │
 │ Block (128MB)   │ Segment (256MB) │
 │ EditLog         │ Journal         │
 │ fsimage         │ Checkpoint      │
 │ ZKFC            │ BDB-JE Election │
 └─────────────────┴─────────────────┘
```

Key difference: Doris FE uses built-in BDB-JE for high availability and does not rely on ZooKeeper or external fencing scripts.

## 1.4 Doris Nodes: FE and BE in Detail

### 1.4.1 FE (Frontend) Nodes

The FE is the "brain" of Doris, managing database metadata and coordinating queries. It is typically deployed as a highly available cluster of 1-3 nodes.

Primary responsibilities:

 - Maintain the catalog hierarchy: Databases, Tables, Partitions, Materialized Views
 - Track Tablet distribution: replica placement, version states, load balancing
 - Coordinate distributed transactions: two-phase commit (2PC) for data ingestion
 - Manage cluster topology: BE node registration, heartbeat monitoring, failover
 - SQL parsing and optimization: generate distributed execution plans and aggregate results from BE nodes
Metadata persistence:

 - Stored in BDB-JE (Berkeley DB Java Edition)
 - Leader FE writes journal entries; Follower FEs replay journals to maintain consistency
 - Periodic checkpoints for fast recovery

### 1.4.2 BE (Backend) Nodes

The BE is the "muscle" of Doris, providing horizontally scalable storage and compute capacity. Typically 3 or more nodes are deployed.

Core functions:

 - Store data locally in columnar format (Tablet → Rowset → Segment hierarchy)
 - Execute query fragments (PlanFragments) dispatched from FE and exchange data directly with other BEs
 - Handle data ingestion via MemTable flush and background Compaction
 - Manage replica lifecycle: clone, repair, balance
Interaction with FE:

 - Receive query plans and DDL commands from FE
 - Respond to FE heartbeats with disk/resource status; push periodic tablet reports to the Master FE
 - Stream Load high-throughput ingestion may connect directly to BE
Key design: Autonomy

 - BEs operate independently for local storage management without FE micromanagement
 - Background tasks (Compaction, GC) run autonomously
 - Inter-BE communication handles data shuffle and replication

## 1.5 Table Models

Every Doris table is created with a model — DUPLICATE KEY, UNIQUE KEY, or AGGREGATE KEY. The model determines how data is stored.

### 1.5.1 Duplicate Key Model

 - Stores every original data record without deduplication or aggregation.
 - Typical use: raw logs, events, time-series data

### 1.5.2 Unique Key Model

 - New data overwrites any existing row with the same Key.
 - Typical use: High-frequency data updates, Efficient data deduplication, Partial column updates

### 1.5.3 Aggregate Model

 - Performs pre-aggregation during data ingestion and the background merge stage
 - Typical use: Detail data summarization, Queries that do not depend on raw details

## 1.6 Node Variants and Deployment Modes (doris 4.0)

Beyond the classic two-node-type deployment, Doris 4.0 supports optional node variants and a cloud-native deployment mode.

### 1.6.1 FE Observer (read replica)

 - Role: Read-only FE. Receives journal replication from the Leader asynchronously, but has no vote and can never be elected as Leader.
 - Use cases: metadata read scaling, cross-region query planning, backup.
 - Election boundary: only Leader + Followers form the BDB-JE replication group that elects the Leader; Observers sit outside the quorum.
(See Part 2.5 for the full architecture diagram.)

### 1.6.2 Compute Node (stateless BE)

 - A Compute Node is a BE started with be_node_role=computation (the default "mix" role stores Tablet replicas; a Compute Node stores none).
 - It hosts no Tablet replicas, is excluded from replica allocation and tablet scheduling, and is only used for query execution and external table scanning.
 - Compute Nodes can be added or removed at will, enabling elastic compute scaling without any tablet migration or rebalance.

### 1.6.3 Cloud Mode: Compute-Storage Separation

In Cloud mode, tablet metadata and data are moved off the local disks of BE nodes and onto external services, turning BEs into stateless compute units:

```text
 ┌───────────────────────────────────────────────────────────────────────────┐
 │                    Doris Cloud (Compute-Storage Separation)               │
 ├───────────────────────────────────────────────────────────────────────────┤
 │                                                                           │
 │   Client ──► FE  ──►  Stateless BEs  (local cache only, elastic)          │
 │              │        Data files live in S3 / OSS / HDFS (object store)   │
 │              │                                                            │
 │              └──►  MetaService   (transactional metadata, KV store)       │
 │              └──►  Recycler      (GC of expired data/metadata)            │
 │                                                                           │
 │   Source tree: cloud/src/meta-service, cloud/src/recycler, ...            │
 └───────────────────────────────────────────────────────────────────────────┘
```

This document focuses on the classic (compute-storage coupled) deployment unless stated otherwise.

---
