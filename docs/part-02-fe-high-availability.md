# Part 2: FE High Availability


## 2.1 Leader-Follower Architecture

The FE layer achieves high availability through multi-node deployment:

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                    Leader-Follower Architecture                             │
 │                    1 Leader + 2 Followers + 0-2 Observers                   │
 ├─────────────────────────────────────────────────────────────────────────────┤
 │  ┌─────────────┐           ┌─────────────┐           ┌─────────────┐        │
 │  │   FE-1      │◄─────────►│   FE-2      │◄─────────►│   FE-3      │        │
 │  │  (Leader)   │    BDB    │  (Follower) │    BDB    │  (Follower) │        │
 │  │             │Replication│             │Replication│             │        │
 │  │  ┌───────┐  │           │  ┌───────┐  │           │  ┌───────┐  │        │
 │  │  │BDB-JE │  │           │  │BDB-JE │  │           │  │BDB-JE │  │        │
 │  │  │Master │  │           │  │Replica│  │           │  │Replica│  │        │
 │  │  └───────┘  │           │  └───────┘  │           │  └───────┘  │        │
 │  │  Writes     │           │  Replays    │           │  Replays    │        │
 │  │  Journals   │──────────►│  Journals   │──────────►│  Journals   │        │
 │  └─────────────┘           └─────────────┘           └─────────────┘        │
 │       ▲                                                    ▲                │
 │       └────────────────────────────────────────────────────┘                │
 │                        Heartbeat / Election                                 │
 └─────────────────────────────────────────────────────────────────────────────┘
```

 - Leader: Handles all metadata writes and synchronizes journal entries to Followers.
 - Follower: Replays journal entries to maintain consistent state and can participate in leader election when the Leader fails.
 - Election: When the Leader fails, BDB-JE's built-in protocol automatically completes a new Leader election within seconds.

## 2.2 Checkpoint Mechanism

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                             Checkpoint Mechanism                            │
 ├─────────────────────────────────────────────────────────────────────────────┤
 │                                                                             │
 │   Memory State ──► Journal Log ──► Checkpoint ──► Disk                      │
 │        │              │                │                                    │
 │        │              │                │                                    │
 │        ▼              ▼                ▼                                    │
 │   ┌────────┐     ┌─────────┐      ┌──────────────┐                          │
 │   │Catalog │     │#100-#106│      │  image.106   │                          │
 │   │Tablet  │────►│(rolling)│─────►│(snapshot)    │                          │
 │   └────────┘     └─────────┘      └──────────────┘                          │
 │                                                                             │
 │   Recovery: Load image.106 + Replay #107+                                   │
 │   (Fast vs. Slow: replay all #1-#106 without checkpoint)                    │
 └─────────────────────────────────────────────────────────────────────────────┘
```

 - The Leader FE periodically generates checkpoints (metadata snapshots).
 - The polling thread runs every 60 seconds. If any new finalized journal exists beyond the latest image (and JVM memory usage is below metadata_checkpoint_memory_threshold, default 70%), a checkpoint is made.
 - After the new checkpoint is generated, the Leader FE pushes the image file to every other FE via HTTP.
 - Only when the push to all other FEs succeeds are the old journal logs covered by the snapshot cleaned up from BDB-JE.
 - During checkpoint generation, the Leader FE loads the current image and replays subsequent journal logs to build a complete in-memory metadata copy, which temporarily consumes additional heap memory.
 - When a Standby FE (Follower or Observer) receives a new image from the Leader, it first writes the data to a temporary file named image.ckpt. Once the write completes successfully, this file is atomically renamed to image.<logid> (where <logid> is the ID of the last journal entry included in the snapshot), replacing the previous image file. As a result, the image/ directory typically contains only one valid image file at any given time.
 - The image file on Standby FE is used only during restart to accelerate recovery
```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                Image (Baseline) + Journal (Roll-forward)                    │
 │                         at FE Restart                                       │
 ├─────────────────────────────────────────────────────────────────────────────┤
 │                                                                             │
 │   BDB-JE local journal (fe-meta/bdb/)            image/ directory           │
 │   ┌────────────────────────────────┐          ┌────────────────┐            │
 │   │ #1 ───► #106 │ #107 ───► #150  │          │   image.106    │            │
 │   │   (history)  │  (increment)    │          │  (full baseline│            │
 │   │   skipped    │   replayed      │          │   through #106)│            │
 │   └────────────────────────────────┘          └────────────────┘            │
 │                                                                             │
 └─────────────────────────────────────────────────────────────────────────────┘
```

 - An image is a full metadata snapshot identified by a journal sequence number, not a wall-clock timestamp: image.<106> means "all metadata changes up to and including journal entry #106 have been merged and frozen into one file."
 - The local BDB-JE environment does not start logging from the image point. Journals have been appended continuously since cluster creation (or since the last checkpoint cleanup), so the local environment holds two segments: the historical segment (#1–#106, already covered by the image) and the incremental segment (#107–#latest, after the image).
 - At restart the historical segment is skipped: the FE first sets replayedJournalId to the latest image sequence number, and then replays only journal entries larger than that id (Env.java: loadImage / replayJournal).
 - The historical segment does not accumulate forever. Once the Leader's checkpoint has been pushed to all other FEs, the journal logs covered by the snapshot are deleted from BDB-JE, so the local log size is always bounded by the latest image.
 - BDB-JE is also the replication carrier: every journal entry the Leader writes is propagated to all FEs through BDB-JE's own replication protocol. Each FE's local BDB-JE environment is a local replica of this shared log.
 - Recovery is therefore a baseline + roll-forward model (analogous to MySQL xtrabackup --apply-log: the image is the backup baseline, the journals are redo logs). Journal entries are applied directly onto the same in-memory Catalog object built from the image — not onto a separate copy.
 - After replay, the in-memory metadata is exactly consistent with the local BDB-JE, not "best effort": replayJournal replays every entry larger than the current journal id, one by one, up to the newest local entry.
 - Boundary case: replay always brings memory exactly to the local BDB-JE tip, and the local environment is itself kept at the group's tip by BDB-JE's replication protocol — this sync happens inside editLog.open(), before replay begins. If the FE has been offline so long that required log files were already cleaned up, an InsufficientLogException is raised; Doris then copies the missing log files from another group member (NetworkRestore) and restarts the FE.
 - Until replay completes, the FE does not serve traffic. On a node about to become Master, replay is a stage of the transfer-to-master flow. On Followers/Observers there is no such one-shot phase: a background "replayer" daemon keeps replaying new journal entries from the local BDB-JE into memory continuously, and read availability is gated on replay progress (the canRead flag).

## 2.3 Failure Recovery

```text
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                        Failure Recovery Flow                                │
 ├─────────────────────────────────────────────────────────────────────────────┤
 │                                                                             │
 │   Normal:  Leader ◄──► Follower ◄──► Follower                               │
 │               │                                                             │
 │               │ Leader Dies                                                 │
 │               ▼                                                             │
 │   Detect:   Follower detects timeout (30s)                                  │
 │               │                                                             │
 │               ▼                                                             │
 │   Election: Vote by LastVLSN → New Leader elected                           │
 │               │                                                             │
 │               ▼                                                             │
 │   Active:   New Leader serves after brief transfer-to-master (fencing       │
 │               │ replay)                                                     │
 │               ▼                                                             │
 │   Fencing:  Old Leader demoted → process exits → restart, rejoin as Follower│
 └─────────────────────────────────────────────────────────────────────────────┘
```

Recovery mechanisms: 1. Normal: Followers continuously replay the Leader's journal entries, keeping metadata synchronized in real time. Followers maintain an in-memory metadata state identical to the Leader. No "cold loading" is needed during failover. 2. Detect: When a leader FE is lost, follower FEs can detect in 30s. Because standby FEs keep a health check with the leader. 3. Election: Among the electable nodes participating in the election, the node with the most up-to-date set of logs will win the election. 4. Active: The new leader serves after a short transfer-to-master phase ( fencing + replay journal), which is quick because its in-memory state is already current. 5. Fencing: If the old leader is still alive (e.g., after a network partition), BDB-JE demotes it automatically once it loses heartbeat with the group, and the new master's fencing (a higher epoch in epochDB) guards against split-brain. If it rejoins via restart, the order is: rejoin the replication group (editLog.open()), let BDB-JE replication bring its local journal up to the group tip, then replay from its local image into memory and serve again as a Follower. No checkpoint download is involved; only if its local journal files were already cleaned up does it copy the missing log files from another member (NetworkRestore) and restart.

## 2.4 Failover and Fencing

Failover is managed by the BDB-JE built-in replication protocol, without requiring ZooKeeper or external fencing scripts.

BDB-JE's Replication Group mechanism ensures: 1. There is only one Master BDB-JE in the Replication Group. 2. Master BDB-JE can serve read and write requests. Replica BDB-JE can replay logs from the Master and only serve read requests. 3. Every write transaction can be assigned a Virtual Log Sequence Number. It is globally unique. The write transactions will be replicated to Replica BDB-JE to be replayed. 4. Write transactions can only be committed after majority BDB-JE nodes confirm. 5. The BDB-JE with the most up-to-date logs (highest LastVLSN) wins the election and becomes the new Master BDB-JE.

There are 2 kinds of failovers.

 - Graceful switch (admin-invoked): not available in 4.0.8. The common practice is to stop the current Leader and let BDB-JE elect a new Master; in-flight transactions are completed or rolled back during the failover window.
 - Ungraceful failover: Network partitions or JVM GC pauses may trigger an election while the old Leader is still running. Doris prevents split-brain through the following mechanisms.
Fencing and split-brain prevention: 1. When a new leader FE is elected, there will be a monotonically increasing Epoch Number. 2. The Epoch Number will be persisted into the shared epochDB. All FEs can get the shared information. 3. The new leader FE's Epoch Number is strictly greater than the old leader's. 4. The new leader's Epoch Number is propagated to BEs through the heartbeat ( carried in TMasterInfo). A BE accepts a heartbeat only if its epoch is greater than the locally maintained value; heartbeats with a stale epoch are ignored, so a demoted old leader can never regain command of the BE. 5. When the old leader is demoted (MASTER to any other type), the FE process exits immediately by design (System.exit). When it comes back via restart, it rejoins the group as a Follower: loads its local image, catches its local journal up via BDB-JE replication, replays into memory, and resumes service. No image download is involved.

Comparison with HDFS HA:

```text
 ┌──────────────────────────────┬──────────────────────────────┐
 │ HDFS HA                      │ Doris FE HA                  │
 ├──────────────────────────────┼──────────────────────────────┤
 │ External: ZooKeeper          │ Built-in: BDB-JE             │
 │ Failover controller          │ Replication protocol         │
 │ Manual fencing (SSH/STONITH) │ Automatic epoch fencing      │
 │ Ungraceful: fencing scripts  │ Ungraceful: quorum reject    │
 └──────────────────────────────┴──────────────────────────────┘
```

## 2.5 Observer Nodes (Optional)

```text
 ┌───────────────────────────────────────────────────┐
 │           Observer Node Architecture              │
 ├───────────────────────────────────────────────────┤
 │   Core Group (Election + Writes)                  │
 │   ┌─────────┐      ┌─────────┐      ┌─────────┐   │
 │   │ Leader  │◄────►│Follower │◄────►│Follower │   │
 │   │         │ sync │         │ sync │         │   │
 │   └─────────┘      └─────────┘      └─────────┘   │
 │        │ async repl                               │
 │        ▼                                          │
 │   ┌───────────┐      ┌───────────┐                │
 │   │Observer   │      │Observer   │                │
 │   │ (Read)    │      │ (Read)    │                │
 │   │ No Vote   │      │ No Vote   │                │
 │   │ No Promote│      │ No Promote│                │
 │   └───────────┘      └───────────┘                │
 │   Use: Read scaling, Cross-region, Backup         │
 └───────────────────────────────────────────────────┘
```

 - Read-only replicas for metadata read scaling and query load offloading.
 - Do not participate in leader election or write consensus.
 - Use cases: read scaling, cross-region deployment, backup nodes.
 - Because replication to Observers is asynchronous, reads on an Observer may lag slightly behind the Leader (bounded by the replay window); read availability is gated by the replay progress (canRead).
---
