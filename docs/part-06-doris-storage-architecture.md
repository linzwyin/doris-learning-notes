# Part 6: Doris Storage Architecture

## 6.1 LSM-tree Concepts in Doris 4.0.8

Doris is an LSM-tree-based storage system, just like HBase and Bigtable. The description below follows the classic LSM-tree narrative (HBase: The Definitive Guide, Ch. 8) paragraph by paragraph, and marks the five places where Doris's design deviates from the classic pattern.

Write Path

Incoming data first lands in a MemTable, completely sequentially. The MemTable does NOT keep data sorted: inserts are append-only into a DorisVector<RowInBlock*> (`memtable.h`, 184). Ordering happens at flush time: the whole buffer is sorted once with pdqsort (a pattern- defeating quicksort) on the schema's key columns, aggregated if the table is AGGREGATE or UNIQUE, and written out as a new, immutable Segment file (`memtable.cpp`, 326, 384). Under memory pressure the MemTable may perform this sort+aggregate early to shrink itself (`memtable.h` shrink_memtable_by_agg). A MemTable is flushed when it reaches write_buffer_size, 200 MB by default (100 MB for AGGREGATE tables) (`be/src/common/config.cpp`).

Deviation 1 — No local WAL. In the classic description, the logfile is the durability anchor. Doris has no BE-side WAL on this path. Durability is provided instead by the FE-side two-phase commit plus redundancy: each row is written into the MemTables of all three replicas, and the transaction publishes only after a majority of replicas have flushed and reported (QUORUM_SUCC by default, TIMEOUT_SUCC as fallback) (`DatabaseTransactionMgr.java`, `TransactionState.java`). The "log" in Doris's LSM story is the FE edit journal; the "store" is replicated three times.

On-Disk Layout

Each flush produces a Rowset (a versioned, immutable unit of one or more Segments) rather than a single sorted run. Segments are columnar page files, NOT B-tree nodes: rows are stored column-by-column in data pages of 64 KB by default (`options.h` STORAGE_PAGE_SIZE_DEFAULT VALUE = 65536), and a Segment is cut when it grows past roughly 256 MB (`olap_define.h`; `segment_writer.h`). Deviation 2 — this is the second difference from the textbook description: an HBase store file is a row-oriented B+-tree-like structure, while a Doris Segment is a columnar page file with three levels of data pruning (ZoneMap, Bloom Filter, Ordinal index).

Rolling Merge

As more flushes accumulate, many small Rowsets appear. A background process aggregates them into larger ones, so that reads touch only a few files. Doris splits this into two tiers, roughly "minor" vs "major":

 - Cumulative Compaction (minor): rolls small Rowsets into medium ones, running continuously in the background.
 - Base Compaction (major): merges ALL Rowsets of a tablet into a single new base Rowset, dropping deleted rows and obsolete versions.
Deviation 3 — Doris also "splits the on-disk tree into separate trees": the split boundary is the bucket. Each bucket of a partition is an independent LSM instance (one tablet = one MemTable pipeline + one Rowset lineage), and buckets are placed on different BEs to spread writes and reads (`DistributionInfo.java`: HASH / RANDOM).

Read Path

Lookups are done in a merging fashion: the MemTable is searched first, then the on-disk Rowsets are searched next. Because all stores are sorted, no reordering is required. In Doris the "which Rowsets are visible" decision is a version-graph lookup (version_graph.h): the reader resolves the publish-versioned Rowset set for its snapshot and merges their results (MemTable + base + cumulative deltas) into one consistent view — Doris's MVCC read semantics. No write lock is needed on read; immutability of Rowsets makes concurrent read/write safe.

Deletes

Deletes are a special case. Doris uses TWO marker mechanisms depending on the table model — Deviation 4:

 - Delete predicate (DUPLICATE / AGGREGATE tables): a predicate record is stored in the Rowset metadata; it is evaluated at read time to skip masked rows, and dropped when a rewrite (Base Compaction) materializes it.
 - Delete bitmap (UNIQUE tables, Merge-on-Write): deleted positions are recorded in a bitmap per segment; reads skip marked positions; the bitmap is compacted away on rewrite.
TTL in Doris is a PARTITION-granularity concept, not a per-record predicate — Deviation 5. Expired partitions are dropped wholesale by the DynamicPartitionScheduler, and the CatalogRecycleBin retains metadata for 24 hours before final removal. HBase retires individual entries by timestamp predicate during compaction; Doris retires whole time-range partitions. For time-series workloads this is cheaper and more predictable, at the cost of no per-row TTL.

B+-tree vs. LSM-tree

The fundamental difference is how each architecture uses modern hardware. B+-trees work well until there are too many random modifications; they force costly optimizations (page splits, rewrites) to retain locality, and updates/deletes run at disk seek rates. LSM- trees work at disk transfer rates: random writes become sequential writes (MemTable + flush), reads never contend with writes (immutable files, MVCC), and storage is always in a sorted, optimized layout. You get cost transparency: with N visible Rowsets, a lookup does at most N segment seeks, and scanning consecutive keys costs no extra seeks.

```text
 ┌────────────────────────────────────────────────────────────────────┐
 │               Doris LSM-tree at a Glance (4.0.8)                   │
 ├────────────────────────────────────────────────────────────────────┤
 │                                                                    │
 │  WRITE SIDE                                                        │
 │  ┌──────────┐  flush (pdqsort + agg)   ┌───────────────────────┐  │
 │  │ MemTable │────────────────────────►│ Rowset v1 (immutable) │  │
 │  │ (append- │                         └──────────┬────────────┘  │
 │  │  only,   │   ┌──────────────────┐            │ rolling merge │
 │  │  200MB)  │──►│ Rowset v2 ... vN  │            ▼               │
 │  └──────────┘   └──────────────────┘   ┌───────────────────────┐ │
 │                                         │ Rowset vN' (merged,   │ │
 │  Durability: FE 2PC + 3-replica write   │ larger, sorted)       │ │
 │  + majority publish (QUORUM_SUCC)       └───────────────────────┘ │
 │                                                                    │
 │  READ SIDE                                                         │
 │  ┌──────────┐   ┌──────────────────────────────────────────────┐  │
 │  │ MemTable │──►│ Version Graph ──► visible Rowset set ──►     │  │
 │  └──────────┘   │ merge = one consistent MVCC view             │  │
 │                 └──────────────────────────────────────────────┘  │
 │                                                                    │
 │  Retire: partition-level TTL (DynamicPartitionScheduler)           │
 └────────────────────────────────────────────────────────────────────┘
```

Terminology mapping (HBase The Definitive Guide → Doris 4.0.8):

```text
 ┌────────────────────────┬────────────────────────────────────────────┐
 │ HBase / classic LSM    │ Doris 4.0.8                                 │
 ├────────────────────────┼────────────────────────────────────────────┤
 │ logfile (WAL)          │ FE edit journal + FE-side 2PC; no BE WAL    │
 │ in-memory store        │ MemTable (append-only, sorted at flush)     │
 │ flush threshold        │ write_buffer_size = 200MB (AGG: 100MB)      │
 │ store file             │ Rowset → Segment (columnar pages, ~256MB)   │
 │ minor/major compaction │ cumulative / base compaction                │
 │ split into trees       │ bucketing (HASH / RANDOM) per partition     │
 │ delete marker          │ delete predicate OR delete bitmap (by model)│
 │ TTL predicate          │ partition-level TTL (partition drop)        │
 │ timestamp-based MVCC   │ publish-version Rowset set via version graph│
 └────────────────────────┴────────────────────────────────────────────┘
```

In general, what can be emphasized about Doris's LSM-tree design is that it inherits the LSM strengths (sequential writes, read/write isolation, cost transparency) while substituting columnar segments for row-oriented store files, and cluster-level commit + replication for a local write-ahead log.

## 6.2 Storage

One of the least-known aspects of Doris is how data is actually stored on the BE's local disks. While the majority of users may never have to bother with this, you may have to get up to speed when you want to learn the meaning of the various advanced configuration options you have at your disposal — storage_root_path, write_buffer_size, segment and page sizing, compaction thresholds — or when disaster strikes and you have to recover a BE or an FE from what is left on disk. At that point, it is important to know where all the data is stored, which files are safe to copy, and which are only transient state. Of course, this shall not happen, but who can guarantee that?

The first step in understanding the various moving parts in Doris's storage layer is to understand the high-level picture. Figure 7-1 shows an overview of how the FE, the BE, and the local filesystem combine to store data.

```text
 ┌─────────────────────────────────────────────────────────────────────┐
 │  Figure 7-1: Overview of the Doris storage layer                    │
 ├─────────────────────────────────────────────────────────────────────┤
 │                                                                     │
 │   Client                                                            │
 │     │ MySQL protocol (port 9030)                                    │
 │     ▼                                                               │
 │   ┌─────────────┐   heartbeat / tablet report    ┌───────────────┐  │
 │   │     FE      │◄───────────────────────────────│      BE 1     │  │
 │   │  Catalog    │                                │               │  │
 │   │  (in-memory)│── query plan / tablet location─►│ ┌───────┐     │  │
 │   │  Tablet     │                                │ │Tablet │─────┼──┤
 │   │  locations  │                                │ │ Rowset│─────┼──┤
 │   └─────────────┘                                │ │Segment│     │  │
 │     │ journal (BDB-JE) / image                   │ └───────┘     │  │
 │     ▼                                            └───────────────┘  │
 │   fe-meta/  (bdb/, image/)                          ... BE N        │
 │                                                                     │
 └─────────────────────────────────────────────────────────────────────┘
```

The figure shows that Doris handles two kinds of persistent state: FE metadata (the catalog, journal, and checkpoints, stored under fe-meta/) and BE data (tablets, stored as files under each BE's storage root paths). The metadata is handled by the FE — which is why FE disaster recovery is a story of image + journal replay, described in Part 3. The data files are handled entirely by the BE processes, without any FE involvement in the read/write hot path. Within a storage root path, data is organized as a fixed directory tree (storage_root_path/data/<shard_id>/<tablet_id>/<schema_hash>/), under which immutable Rowset files are laid out; each Rowset directory holds one or more Segment files plus their index files (see "Rowset and Segment Format" for the layout inside a Segment).

The general communication flow is as follows. A client contacts the FE first when trying to access a particular row — there is no client-side routing cache and no external coordination service to look up. The FE parses and plans the query, and for each involved tablet it consults its in-memory tablet index (TabletInvertedIndex) to find which BEs hold replicas of the tablet and which version each replica has reached. This index is not stored on disk and not queried from elsewhere: it is built and maintained from BE heartbeats and tablet reports, which every BE pushes periodically to the FE (see "Heartbeat and Tablet Report" for details). So over time, the FE — not the client — has a complete, self-updating picture of where every row lives. The client only ever sees one address: the FE.

The FE is responsible for assigning tablets to BEs. This happens when a table is created, when a dynamic/auto partition scheduler creates a new partition, and continuously afterwards through the tablet scheduler (see "Tablet Scheduling and Load Balancing").  The BE learns of a new tablet through explicit tasks pushed by the FE — a create-tablet task at allocation time, or a clone task during repair and migration — and stages clone data under the tablet's own clone/ directory before the tablet opens. On restart, it re-discovers its tablets from the local meta store (a RocksDB instance holding TabletMeta records), and the periodic tablet report reconciles the FE's view with the BE's. When a tablet is opened it exposes a Rowset list — each Rowset a lightweight, versioned wrapper around one or more Segment files. The Tablet also owns the active MemTable for in-flight writes, and the BE a shared LoadChannel instance per load job that feeds the MemTables of all tablets involved in that load (see "Load Channel and MemTable" — the functional analog of HBase's shared HLog, minus the replay story: there is no WAL to replay, because durability is decided at the FE by two-phase commit, as established in the LSM-tree overview).

One more HBase analogy is worth dissolving here. HBase divides every stored file into HDFS blocks and lets you tune block size for large or small records. Doris divides each Segment into data pages instead — 64 KB by default — and pages are the unit of I/O, caching, and predicate pruning inside the BE page cache, not blocks in a distributed filesystem: a Doris BE writes to ordinary local disks (or to object storage in the cloud/separation deployment), and "where are my bytes" is answered by the directory tree above, not by asking HDFS. More on that in "Segment Format".

## 6.3 Write Path

(This section expands the write-side overview given in 7.1 "LSM-tree Concepts".)

The client issues an INSERT (or a Stream Load / Broker Load request) to the FE, which parses the statement and coordinates the load as a transaction. The FE assigns a transaction ID, hands the data channel endpoints back to the client, and the client (or the FE itself, acting as coordinator) forwards the rows directly to the BEs holding the target tablets' replicas. On each BE, a LoadChannel — one per load job, shared across all tablets the job touches — receives the incoming batches and hands them to a DeltaWriter, one per tablet replica (see "Load Channel and MemTable" for the full seven-step chain from network bytes to MemTable insert).

The first step is NOT to write the data to a write-ahead log — Doris has no WAL on this path, and this is the point where the Doris write path departs most visibly from the classic LSM narrative. Durability is decided at the FE, not on the BE disk: every load is a two-phase commit across all replicas of every involved tablet. The BE accumulates rows in memory, flushes them to immutable Segment files locally at its own convenience, and only then reports the flushed version to the FE; the transaction publishes when a quorum of replicas has reported success (QUORUM_SUCC by default, i.e., two of three; TIMEOUT_SUCC as a fallback for stragglers) (`DatabaseTransactionMgr.java`, `TransactionState.java`). A BE crash before the commit therefore loses only an uncommitted transaction — never acknowledged data — and the WAL's crash-replay role is absorbed entirely by the FE-managed commit protocol plus replica redundancy.

Once accepted, the row is placed in the MemTable. The MemTable does not keep data sorted: inserts are append-only into a flat buffer (DorisVector<RowInBlock*>, `memtable.h`, 184), which is why the in-memory path is pure sequential memory traffic and scales linearly with batch size. At the same time, it is checked to see if the MemTable is full and, if so, a flush to disk is requested. "Full" means the MemTable has reached write_buffer_size — 200 MB by default, tightened to 100 MB for AGGREGATE tables, where the aggregation itself is memory- hungry (`be/src/common/config.cpp`). Under cluster memory pressure the MemTable may also shrink itself early by performing its sort+aggregate ahead of time (`memtable.h` shrink_memtable_by_agg), trading CPU for headroom before the limit is hit.

The flush request is served by a background thread in the BE, which sorts the entire buffer with pdqsort on the schema's key columns, merges rows with identical keys if the table is AGGREGATE or UNIQUE, and writes the result as a new Segment file under the tablet's Rowset directory — plus a new Rowset version registered in the tablet's meta (`memtable.cpp`, 326, 384). Once the Segment is on disk and the Rowset is visible, the BE reports this version to the FE, and the FE records it as this replica's progress toward commit. So the system always knows what was persisted so far: not via a sequence number appended to a log, but via the version-annotated Rowset graph maintained per tablet (version_graph.h) — the version IS Doris's sequence number, and its authority rests with the FE, not with any single BE.

## 6.4 Files

Doris has one or more configurable storage root paths on each BE, set with storage_root_path. Unlike HBase, where everything lives under a single HDFS root directory readable from any machine, a Doris table's bytes are spread across the local disks of many BEs, and no single directory shows you the whole table — to "look at a table" you have to pick one BE and look at its shards of it. Within one storage root path, however, the layout is completely deterministic, and worth walking once.

Suppose we create a table with 8 buckets and two partitions, insert several batches, and let the flushes and compactions settle. The relevant portion of one BE's storage root path then looks like this (tablet IDs and rowset IDs abbreviated for readability):

```text
 ┌────────────────────────────────────────────────────────────────────┐
 │  Figure 6-2: Storage root layout on a single BE                    │
 ├────────────────────────────────────────────────────────────────────┤
 │  <storage_root>/                                                   │
 │  ├── data/                      ← all tablet data                  │
 │  │   ├── 0/                     ← shard (tablet_id % shards)       │
 │  │   │   ├── 10010/             ← tablet directory                 │
 │  │   │   │   ├── 12345/         ← schema hash (the "version" of    │
 │  │   │   │   │   │               the tablet's schema)              │
 │  │   │   │   │   │   ├── 02000000000000000001A1BC.../  ← rowset    │
 │  │   │   │   │   │   │   ├── 02000000000000000001A1BC..._0.dat     │
 │  │   │   │   │   │   │   ├── 02000000000000000001A1BC..._1.dat     │
 │  │   │   │   │   │   │   └── 02000000000000000001A1BC..._1_101.idx │
 │  │   │   │   │   │   └── 02000000000000000002D3EF.../  ← rowset    │
 │  │   │   │   │   │       └── ...                                  │
 │  │   │   │   │   └── .hdr files appear ONLY under clone/ staging   │
 │  │   │   └── ...                                                   │
 │  ├── meta/                      ← RocksDB: tablet & rowset meta    │
 │  ├── trash/                     ← delayed deletion (default: immediate)   │
 │  └── snapshot/                  ← snapshot staging                   │
 └────────────────────────────────────────────────────────────────────┘
```

Root-level directories

Each storage root path contains four fixed top-level entries. data/ holds the tablet directory tree described below. meta/ is a RocksDB instance holding every TabletMeta and RowsetMeta record on this disk; this is where the per-tablet descriptor analog of HBase's .regioninfo actually lives — NOT as a file next to the data, but as a record in the local meta store, which is why you will not find any per-tablet info file in the tree above.  trash/ implements delayed deletion: dropped tablets and obsolete rowsets are moved here first and only physically erased after trash_file_expire_time_sec. Caveat: in 4.0.8 this defaults to ZERO — deletion is effectively immediate, and an accidentally dropped tablet is recoverable from trash only if the cluster was configured with a nonzero window beforehand (`config.cpp`).

The most instructive root-level file in HBase — the write-ahead log under .logs — simply has no counterpart here. There is no log directory on a Doris BE, ever. The absence is structural, not incidental: durability is a property of the FE commit protocol and replica redundancy, not of any on-disk journal (see "Write Path" and the LSM-tree overview). If you ever find yourself looking for "the WAL directory" while doing disaster recovery on a BE, you are asking the wrong question: the question that makes sense is which rowsets the FE had already published for this tablet before the crash.

Tablet-level directories

Inside data/, the tree is data/<shard>/<tablet_id>/<schema_hash>/, where shard is the tablet ID modulo the number of map shards (an in-memory locking structure made visible in the path, tablet_manager. cpp:84-90), and schema_hash is a fingerprint of the tablet's current schema: a schema change (adding a column, changing keys) materializes a NEW schema_hash directory on disk and migrates the data, leaving the old tree to be swept by path GC. Below the schema hash sits one directory per Rowset, named after the rowset's globally unique ID. Within a rowset directory, each Segment is a single file named <rowset_id>_<segment_seq>.dat (`inverted_index_desc.h`: segment_ suffix = ".dat"), and each column with a built inverted index gets a companion <...>.idx file (`inverted_index_desc.h`: index_suffix = ".idx"). ZoneMaps, Bloom filters, and ordinal indexes are NOT separate files — they live inside the .dat file's footer (segment_ `writer.cpp`). All metadata describing which of these rowsets exist, and at which versions, is in meta/'s RocksDB; the files on disk are content-immutable and self-describing via their footers.

Two transient artifacts are worth knowing so you do not misread them during incident response. While a load is in flight, its segments are written under a temporary "0_" file name and only renamed to the rowset-ID name when the transaction publishes — a rowset directory full of 0_*.dat files is a load that has not committed yet, and is exactly the set of bytes that becomes garbage when such a transaction aborts. And during replica cloning (repair or migration), the BE stages incoming data under a clone/ prefix with a .hdr header file (`olap_define.h`: TRASH_PREFIX, CLONE_PREFIX), replacing the target tablet only when staging completes — the Doris analog of HBase's split/recovered.edits staging directories.

Tablet splits

Here Doris parts ways with the LSM canon, and it is worth stating the absence explicitly. A Doris tablet never splits. Bucketing is fixed at table creation (or chosen by AutoBucket from a data-size estimate) and does not react to growth: a tablet simply accumulates rowsets, and growth is absorbed by compaction and by adding NEW partitions — each partition creates its own fresh set of tablets, which is where horizontal scale actually comes from (see "Partitioning Strategy"). There is no daughter-region mechanics, no reference files, no .META. update, no mid-split region state. Operationally this removes an entire class of split-storm incidents that HBase operators know well, at the price of having to think about bucket counts up front.

Compactions

The rowsets of each tablet are monitored by background compaction threads to keep their number and total size under control. The flushes of MemTables slowly build up an increasing number of on-disk rowsets; the compaction process combines them into fewer, larger ones — and, unlike HBase, this is where the growth story ENDS, since no split is waiting at the end of it.

Compactions come in two varieties, cumulative and base, which map onto minor and major compaction but with a versioned twist. The cumulative compaction rewrites small, recent rowsets into larger ones: it becomes eligible once the rowset count exceeds cumulative_compaction_min_deltas (default 5) and processes at most cumulative_compaction_max_deltas (default 1000) deltas at a time (`config.cpp`). The base compaction merges ALL rowsets of the tablet into one — but before running it, the BE asks the size-driven promotion question: if the cumulative output would exceed compaction_promotion_size_mbytes (1 GB), or exceed compaction_promotion_ratio (5%) of the base size with at least compaction_promotion_min_size_mbytes (128 MB), or the version count passes compaction_promotion_version_count (1000), the cumulative boundary is advanced and a base compaction runs instead (`config.cpp`). The combined score of pending compaction work is the compaction score shown in frontends; per-disk threads pick the highest-score tablets under a permit limiter, so heavy tablets throttle themselves instead of saturating disk bandwidth. Vertical compaction — compacting column groups separately — is used for wide tables to bound memory usage. Everything above can be frozen with disable_auto_compaction for troubleshooting (`config.cpp`).

Two deliberate contrasts with the HBase policy are worth keeping in mind. First, compaction is purely score- and size-driven: there is no periodic "major compaction every 24 hours" and therefore no need for a compaction.jitter parameter to stagger one; Doris never rewrites a healthy tablet just because the clock says so. Second, minor compactions are never "promoted" mid-run by file count alone — the minor/major decision is the explicit promotion-policy evaluation above, taken at the cumulative boundary, which makes the choice inspectable and tunable where HBase's promotion heuristic is implicit.

## 6.5 Segment Format

The actual storage files are implemented by the Segment class, which was specifically created to serve one purpose: store Doris's data efficiently in columnar form. They follow the same lineage as HBase's HFiles — ultimately derived from the SSTable format used in Google's Bigtable architecture — but they serve it differently: where an HFile is a row-oriented sequence of serialized KeyValue instances, a Doris Segment stores every column separately, because analytical workloads read a handful of columns from a billion rows, not a handful of cells from a billion rows. Figure 6-3 shows the file format details.

```text
 ┌────────────────────────────────────────────────────────────────────┐
 │  Figure 6-3: Segment file layout (.dat)                            │
 ├────────────────────────────────────────────────────────────────────┤
 │                                                                    │
 │  ┌────────────┬────────────┬─────────────┬─────────────┬─────────┐ │
 │  │ Column 0   │ Column 1   │ Column 2    │ ...         │  Footer │ │
 │  │            │            │             │             │(written │ │
 │  │ data pages │ data pages │ data pages  │             │  last)  │ │
 │  │ + ZoneMap  │ + ZoneMap  │ + Bloom?    │             │         │ │
 │  │ + ordinal  │ + ordinal  │ + ordinal   │             │         │ │
 │  │   index    │   index    │             │             │         │ │
 │  └────────────┴────────────┴─────────────┴─────────────┴─────────┘ │
 │  (+ short-key index page written just before the footer; a         │
 │   primary-key index page as well if the table is UNIQUE)           │
 │                                                                    │
 │  Each page:  ┌────────────────────────┬───────────────┐            │
 │              │  page body (maybe      │ 4B footer_len │            │
 │              │  compressed)           │ + PageFooterPB│            │
 │              └────────────────────────┴───────────────┘            │
 │                                                                    │
 │  Tail of file (12 bytes, fixed):                                   │
 │  ┌───────────────┬──────────┬──────────┐                           │
 │  │ footer_size   │ CRC32C   │ "D0R1"   │                           │
 │  │ (4 bytes)     │ (4 bytes)│ (4 bytes)│                           │
 │  └───────────────┴──────────┴──────────┘                           │
 │                                                                    │
 │  Companion per indexed column:  <segment>.idx  (CLucene inverted   │
 │  index — a SEPARATE file, not part of the .dat layout)             │
 │                                                                    │
 └────────────────────────────────────────────────────────────────────┘
```

The file contains a variable number of pages, organized per column, where the only fixed region is the 12-byte trailer at the very end of the file. As the figure shows, the trailer is what makes the file self-describing: an opener reads just the last 12 bytes, locates the SegmentFooterPB via footer_size, and from the footer reaches every column's data pages, index pages, encoding, and null-map metadata (`segment_writer.cpp`). It is written after all column data has been persisted, finalizing the now immutable data store — the same "trailer last" discipline as HFile, with the magic number "D0R1" standing in for HFile's versioned magic headers (written at `segment_writer.cpp`, verified at open time in `segment.cpp`).

Each column holds two kinds of pages. The data pages carry the actual values, encoded per column (dictionary, bitmap-run, or plain encoding, chosen by the writer and recorded in the footer), and each data page carries its own ZoneMap — min/max/null statistics for the rows it contains — as part of its page footer (`page_io.cpp`: a page is body + 4-byte footer length + PageFooterPB). The index pages per column are the ZoneMap index (one entry per data page), the optional Bloom filter index, and the ordinal index (rowid → data page pointer) — the direct analogs of HFile's index blocks, except there is one set PER COLUMN, not one per file, which is what makes column pruning work: a query touching two columns never reads a page of the other thirty. UNIQUE-key tables additionally write a primary-key index to answer point lookups without scanning; every segment also ends with a short-key index page supporting sparse-key seeks on the table's sorted key prefix.

The page size is set by a table property, which is specified at table creation time by the user, or defaults to 64 KB (`options.h` STORAGE_PAGE_SIZE_DEFAULT_VALUE = 65536). The writer follows the page size limit to write out raw data close to the configured amount: it checks the accumulated size against data_page_size as rows are appended and closes the page on the boundary (`segment_writer.cpp`). If you store a value larger than the remaining page budget, the writer fits whatever it is given, so in practice pages are slightly larger or smaller than the target — not an exact science here either. And as with HFile, if you enable compression (LZ4 by default, per table), the codec decides how much data is enough: the same number of pages is written, but each body is smaller, and the page footer records both compressed and uncompressed sizes so the reader can verify and cache correctly.

One thing you may notice is the apparent size mismatch: a 64 KB Doris page against a ~256 MB segment file (`olap_define.h`; `segment_writer.h`) is a 4,000-to-1 ratio, much like HFile's 64 KB blocks against HDFS's 64 MB blocks. And the moral is the same: there is no correlation between these units. The segment file is an opaque binary file to the underlying filesystem, which only sees extents; the BE's page cache reads, caches, and prunes in units of pages; the storage engine reads and compacts in units of segments. HDFS knowing or not knowing what HBase stores has its exact parallel here — the kernel's view of foo.dat and Doris's view of it share nothing but the inode.

Two companion artifacts sit outside this layout. The inverted index — built on demand for indexed string columns — goes into a separate .idx file per segment (`inverted_index_desc.h`: ".dat" / ".idx"), because it is a CLucene index tree, not a page structure, and keeping it separate lets the engine skip it entirely at read time unless a matching predicate exists. And the delete bitmap of UNIQUE Merge-on- Write tables lives not in the file at all, but in the tablet's meta store and memory, masked in at read time — a reminder that in Doris, what is physically on disk is only ever half the visibility story; the version graph is the other half (see "LSM-tree Concepts").

Opening a segment is therefore a cheap operation: read 12 bytes, parse one footer, and stop. No column pages are touched until a scanner actually needs that column — which is why a thousand-segment tablet can still answer a count(*) on one small column in milliseconds, and why compaction can enumerate and rewrite segments without materializing their contents anywhere but the page buffer.

## 6.6 Row and Encoding Format

In essence, a stored row in Doris has no byte-level existence at all — which is the first and deepest difference from the KeyValue format. Each KeyValue in an HFile is a self-contained byte array carrying its own key (row key, column family, qualifier, timestamp) next to its value. A Doris row is a logical position: the Nth rowid of a segment, materialized only when a scanner joins the Nth value of one column stream with the Nth value of another. What is physically on disk is per-column streams, and the "key" is split into two parts: the sort key columns, stored once each as ordinary column streams (and sparsely, as prefix entries, in the segment's short-key index page), and the rowid, which is not stored anywhere — it is the offset arithmetic of the ordinal index. Figure 6-4 shows the layout of a single column's data page.

```text
 ┌────────────────────────────────────────────────────────────────────┐
 │  Figure 6-4: A data page of one column                             │
 ├────────────────────────────────────────────────────────────────────┤
 │                                                                    │
 │  ┌──────────────────────────────┬───────────────┬─────────────┐    │
 │  │  page body                   │ 4B footer_len│ PageFooterPB│    │
 │  │                              │               │             │    │
 │  │  encoded values (dict codes  │               │ · encoding  │    │
 │  │  or bitshuffle-fixed or      │               │ · statistics│    │
 │  │  plain slices)               │               │   (ZoneMap: │    │
 │  │                              │               │   min/max/  │    │
 │  │  [+ null bitmap if nullable] │               │   has_null) │    │
 │  │                              │               │ · nullmap_  │    │
 │  │                              │               │   size      │    │
 │  └──────────────────────────────┴───────────────┴─────────────┘    │
 │                                                                    │
 │  Dictionary-encoded string column (across the whole segment):      │
 │  ┌───────────────┬──────────────┬──────────────┬──────────────┐    │
 │  │ dict page     │ codeword pg  │ codeword pg  │ ...  (falls  │    │
 │  │ (distinct     │ (bitshuffle  │              │  back to     │    │
 │  │  values, 64KB │  fixed ints) │              │  PLAIN if    │    │
 │  │  budget)      │              │              │  dict > 64KB)│    │
 │  └───────────────┴──────────────┴──────────────┴──────────────┘    │
 │                                                                    │
 └────────────────────────────────────────────────────────────────────┘
```

The encoding per column is chosen by the writer from a fixed matrix, recorded in the segment footer: numeric types (ints, floats, dates, decimals) take PLAIN encoding, where fixed-width values are packed bitshuffle-style into the page body; string-family types (CHAR, VARCHAR, STRING, JSONB, VARIANT) default to DICT_ENCODING (`encoding_info.cpp`). In the dictionary scheme, repeated values never appear twice: the first pages of the column hold the dictionary of distinct values, and every data page holds fixed-width codewords, themselves bitshuffle-compressed (`binary_dict_page.h`: "header(4 bytes) + bitshuffle encoded codeword page"). A column whose cardinality explodes mid-segment is not a problem: once a dictionary page outgrows the shared 64 KB page budget, the writer falls back to plain encoding for the remaining pages of that column (binary_dict_ `page.h`), so the worst case of a dictionary is a plain string page, never an oversized lookup structure. Nullable columns carry a null bitmap inside the page body, and its size is recorded in the page footer so the reader can split values from bitmap without parsing either (`page_io.cpp`).

The reason this design beats the KeyValue approach for analytics has to do with what each page pays per row. An HBase page pays the key overhead — row key, family, qualifier, timestamp — on every cell, so for small payloads the key dwarfs the value and the operator's only remedies are short names and compression. A Doris page pays nothing per row except the encoded value itself: the column identity lives in the footer (one per page), the row identity lives in the ordinal arithmetic, and the null information lives in one shared bitmap. For a table with forty columns of which a query reads three, the other thirty-seven cost exactly zero bytes of I/O.

And the same two mitigations HBase relies on are here as well, in stronger form. Compression works on finite windows of data, and all repeating data compresses well: sorting the rows by key at flush time (see "Write Path") keeps equal keys adjacent — which is what run- length and dictionary codings feed on — and, just as importantly, keeps each page's min/max range tight, which is what makes the ZoneMap statistics in every page footer effective as a pruning weapon: a page whose ZoneMap does not intersect the predicate is skipped without ever being read. In the KeyValue format, sorting merely helped compression; in the columnar format, sorting also tells every page what it does NOT contain, and skipping what a page does not contain is where analytical engines get their speed.

## 6.7 Read Path

(This section expands the read-side overview given in 7.1 "LSM-tree Concepts".)

Doris keeps multiple rowsets per tablet, which contain the actual column data, in columnar segment files. These rowsets are created over time as batches aggregated in the MemTables are flushed to disk. The background process of compaction keeps the number of rowsets under control by rewriting smaller ones into larger ones; base compactions eventually merge the entire set into a single base rowset, after which the flushes start adding small rowsets again.

Since all rowsets and segments are immutable, there is no way to delete a particular row out of them, nor does it make sense to keep rewriting large segments to remove deleted rows one by one. Instead, a delete marker is written, which masks out the "deleted" rows. Doris has two such marker mechanisms, and the table model decides which one a tablet lives with: a delete predicate (for DUPLICATE and AGGREGATE tables) — a condition record, such as "k1 = 7" or "dt < '2024-01-01'", stored in the rowset metadata and evaluated per row at read time; and a delete bitmap (for UNIQUE tables in Merge-on-Write mode) — a per-segment Roaring bitmap of erased rowids. Either way, the buried data still exists somewhere on disk; the marker is simply elsewhere, consulted at read time.

Now consider what a query actually asks. You loaded a batch for a key yesterday; compaction has since merged some rowsets; another load today wrote a newer version of the same key into a fresh rowset that has not been compacted yet; someone also issued a DELETE. The question is: where does the logically visible state of that key reside? The answer, as in HBase, is "spread across several immutable structures at once" — but the client never goes looking for it the way an HBase client walks -ROOT- and .META.. The client sends the SQL to the FE; the FE plans the scan, resolves tablet locations from its in-memory index, and dispatches PlanFragments to the BEs. Each BE tablet scan is served by one TabletReader instance (tablet_reader.h: 85), and it is inside the TabletReader that the "where does the row live" question is answered, in two phases: decide what to read, then merge it.

Before any segment page is touched, a series of exclusion checks is conducted, from coarse to fine. The coarsest is versioning: the TabletReader resolves the query's read version against the tablet's version graph (version_graph.h) and keeps ONLY the rowsets visible at that snapshot — every rowset not in the visible set, including the entire output of transactions that committed after the snapshot, is skipped wholesale. This is the exact analog of HBase skipping store files older than the requested timestamp window, except the currency here is publish versions, not wall-clock time, and skipping is precise rather than heuristic: an invisible rowset is provably invisible. The remaining rowsets — including the MemTable, if the scan's version reaches it — are opened, and on each segment a second, page-grained exclusion runs against the predicates pushed down from the plan: the column ZoneMap index and the short-key index are applied first (`segment_iterator.cpp` _apply_index_expr), and data pages whose min/max do not intersect the predicate, or key ranges outside the seek span, are never fetched; Bloom filters answer "this segment definitely does not contain this key" without touching a data page. For UNIQUE Merge-on-Write tables there is one more mask applied before reading column data at all: the segment's delete bitmap is subtracted, by rowid, from the candidate row bitmap (segment_iterator. cpp:416-422: _row_bitmap -= delete_bitmap), so deleted positions are skipped at the index layer, the way HBase skips cells buried by tombstones — except the tombstone here is a set of positions, not a timestamp, and it sits in the tablet's meta, far from any segment.

What survives exclusion is a set of k sorted streams — one iterator per visible rowset segment, plus the MemTable — each sorted by the table's key because every flush sorts (see "Write Path"). The TabletReader merges them through a vectorized merge iterator (VMergeIterator, vertical_merge_iterator.h): a heap ordered by key, the direct descendant of HBase's KeyValueHeap, with one structural upgrade: instead of reconciling one KeyValue at a time, it reconciles batches of rows column by column, because each stream is columnar. When several streams hold the same key, the merge decides by table model: DUPLICATE tables emit all versions in key order; AGGREGATE tables combine them through the aggregation functions; UNIQUE tables rely on Merge-on-Write having already resolved the winner at write time, with the delete bitmap enforcing it here. Delete predicates are evaluated on the merged stream as ordinary filter expressions.

```text
 ┌────────────────────────────────────────────────────────────────────┐
 │  Figure 6-5: A tablet as its TabletReader sees it                  │
 ├────────────────────────────────────────────────────────────────────┤
 │                                                                    │
 │  read version = 42   (resolved via version graph)                  │
 │                                                                    │
 │  MemTable  ───────────────►  included (sorted, in memory)          │
 │  Rowset v40-41  ── SKIPPED (visible at v42, but ZoneMap-excluded:      │
 │                              no page intersects the predicate)         │
 │  Rowset v38-39  ──────────►  included, segment iters               │
 │  Rowset v30-37  ──────────►  included, segment iters               │
 │  Base Rowset 0-29 ────────►  included, segment iters               │
 │                              − delete bitmap (UNIQUE MoW):         │
 │                                rows {12, 4096, 8001} masked        │
 │  [delete predicate k1=7] ─►  evaluated on merged stream            │
 │                                                                    │
 │  VMergeIterator: heap merge by key ──► vectorized batches          │
 │                                        ──► ExchangeSink ──► FE     │
 └────────────────────────────────────────────────────────────────────┘
```

For the scan loop, the engine calls next_batch() on the TabletReader until the tablet's key range is exhausted or the fragment's resource limits kick in. Each call pulls one batch of rows from the merge heap: every surviving stream advances under its segment iterator, exclusion by exclusion — version set, page ZoneMaps, delete bitmap, predicates — until a block of the requested columns is filled. The final result is not a list of reconciled KeyValues but a set of columnar blocks, one per projected column, aligned by the rowid arithmetic described in "Row and Encoding Format". They flow into the scan node's operator tree, and from there through the exchange operator to the FE (see "Exchange Mode"). To the client, the result arrives as one logical relation — as if all versions of every row had always lived in a single entity. In reality, they never did, and that, in one sentence, is the whole trade the storage layer makes: it never updates anything, and it can therefore prove, at every layer from the version graph down to the page ZoneMap, what it does not need to read.

## 6.8 Tablet Lookups

For the clients to be able to find the backend hosting a specific key, Doris provides no catalog tables at all. HBase needs -ROOT- and .META. because its metadata is itself stored as table regions, so looking up a row means looking up a row. Doris's metadata lives in the FE's Catalog — an in-memory object graph replicated to every FE through the BDB-JE journal (see "Checkpoint Mechanism") — so there is nothing to look up recursively: every FE, Leader or Follower, already holds the entire picture. The three-level B+ tree-like scheme of ZooKeeper → -ROOT- → .META. collapses into a single process. What the client needs to know is only where the FE is, and that is a matter of configuration, not of discovery.

The second thing that collapses is the search itself. An HBase client finds a region by walking a key-range index with its row key. Doris does not search for a tablet — it COMPUTES it. The mapping from a row to a tablet is fixed at table creation time by the table's DistributionInfo, and the FE replays that mapping arithmetically at plan time, in two steps. Step one is partition pruning: the query's predicates are evaluated against the partition expression, and the set of partitions that can contain matching rows falls out by expression evaluation — no table scan, no index.  Step two is bucketing: for a HASH-distributed table, the FE hashes the values of the distribution columns (murmur hash — the same function the FE used to route the write; BEs never compute tablet routing themselves, they simply write the tablets the plan assigns) and reduces the result modulo the bucket count to get the tablet index; when predicates pin down the distribution columns, HashDistributionPruner prunes the buckets the query cannot touch, so an entire partition's worth of tablets may shrink to one. A RANDOM- distributed table skips the arithmetic and simply uses all buckets of each selected partition. The output of the two steps is a concrete list of tablet IDs — derived from the schema, deterministic, and correct by construction rather than by lookup.

With tablet IDs in hand, exactly one in-memory lookup remains. The FE maintains a TabletInvertedIndex (`TabletInvertedIndex.java`), which is the functional descendant of the .META. table but inverted for the access pattern that matters: a tabletMetaMap maps tablet ID to TabletMeta — the tablet's database, table, partition, and index lineage (`TabletInvertedIndex.java`) — and a two-level replicaMetaTable maps (tablet ID, backend ID) to the Replica record holding that replica's state and visible version (TabletInvertedIndex. java:99). getReplicas(tabletId) returns the full replica set in one map row read (`TabletInvertedIndex.java`). From there the coordinator picks a healthy replica whose version covers the query's snapshot — preferring the freshest, and preferring a BE that is already running other fragments of the same query — and attaches it to the plan as a scan range. Both the index and its contents are maintained without any client involvement: BEs push periodic tablet reports, the heartbeat machinery marks backends dead when they go silent, and the tablet scheduler creates or clones replicas to repair whatever the reports reveal (see "Tablet Scheduling").

Because the client never holds locations, there is also no stale cache to discover the hard way. The failure mode HBase designs around — a client learning from a failed request that its cached region map is wrong, then backing out through .META. and -ROOT- — cannot occur, because the client's cache contains exactly one entry: the FE it is connected to. Location staleness is confined to the FE and bounded there: a dead backend is detected by missed heartbeats within one heartbeat interval, a version mismatch is corrected by the next tablet report or by the scheduler's repair, and if a chosen replica fails mid-scan, the coordinator can reschedule the fragment onto another live replica of the same tablet. Everything that HBase pays round trips to keep consistent, Doris keeps consistent by construction: one replicated catalog instead of three levels of indirection, computed placement instead of searched placement, and a server-side index instead of a client-side cache.

The round-trip accounting makes the contrast quantitative. HBase needs three round trips to warm an empty cache, and six in the worst case of a fully stale one. Doris needs zero location round trips: the client→FE connection that carries the query also carries the answer's routing, because routing happens inside planning, in memory, once per query. Figure 6-6 shows the full chain, numbered in the order the pieces are exercised.

```text
 ┌────────────────────────────────────────────────────────────────────┐
 │  Figure 6-6: Key → Tablet → Backend in Doris                       │
 ├────────────────────────────────────────────────────────────────────┤
 │                                                                    │
 │  Client                                                            │
 │    │  (1) configured FE list — the ONLY thing the client caches    │
 │    ▼                                                               │
 │  FE ── (2) plan: partition pruning (expression eval)               │
 │           └─► bucketing: hash(dist keys) % N  ──► tablet IDs       │
 │    │                                                               │
 │    │  (3) TabletInvertedIndex (in-memory)                          │
 │    │      tabletMetaMap:     tablet_id ──► TabletMeta              │
 │    │      replicaMetaTable:  tablet_id × be_id ──► Replica(ver)    │
 │    ▼                                                               │
 │  (4) scan ranges attached to PlanFragments ──► dispatch to BEs     │
 │                                                                    │
 └────────────────────────────────────────────────────────────────────┘
```

Note what is NOT in the figure: no ZooKeeper node, no root region, no meta region, and no recursive back-out path. When something changes — a backend dies, a replica catches up, a new partition is created — no entry in the figure is invalidated on the client side, because the client holds nothing to invalidate. The hierarchy that HBase spends six round trips maintaining has, in Doris, exactly two live levels: the FE, which is authoritative, and the index inside it, which is derived. The first comes from configuration; the second, from arithmetic.

## 6.9 The Tablet Life Cycle

The state of a tablet is tracked twice, at two different levels, by two different owners — and separating the two is the key to understanding the whole lifecycle. The FE tracks replica MEMBERSHIP and VERSION HEALTH in the catalog: for every tablet it knows which BEs hold replicas, and for each replica it tracks the last committed version, the last successful version, and whether the replica has been judged bad. The BE tracks the local RUNTIME state of each tablet it hosts, through a compact state machine in the tablet's meta record (`tablet_meta.h`). Table 6-1 lists the possible states.

Table 6-1. Possible states of a tablet (BE side, `tablet_meta.h`)

State           Description NOTREADY        The tablet is under alter table, rollup, or clone; it exists but does not yet serve requests. RUNNING         The tablet is open and fully operational. TOMBSTONED      The tablet's integrity has been violated — e.g., its rowset chain has a gap of missing versions. It accepts no requests and reports itself to the FE, which schedules the tablet to be dropped. STOPPED         The tablet is shutting down; its files remain on disk until the drop finalizes.

Note immediately what is not in the table: there are no split states. A tablet in Doris is never split, so the entire Splitting/Split pair from HBase's region state machine simply does not exist, and the Offline/Pending/Open triplet collapses into NOTREADY→RUNNING, because tablet creation is a master-driven task with a single synchronous outcome, not an assignment that must later be confirmed (see "Tablet splits" under 7.4 "Files" for why growth never triggers a state change at all).

The transitions are initiated almost exclusively by the FE, but their completion is reported by the BE — which replaces the ZK znode from the HBase design. The typical arc looks like this. When a table is created, or a dynamic/auto partition scheduler materializes a new partition, the FE chooses backends for each new tablet's replicas according to its allocation policy and pushes create-tablet tasks to them; the BE stages the new tablet's data under its clone/ prefix with a .hdr header file, and only when the meta record is durable in its local RocksDB does the tablet become visible to its own storage engine (staged under the tablet's own directory as <tablet_path>/clone, `engine_clone_task.cpp`; see "Files"). From then on it is RUNNING: it takes part in loads (via the FE-coordinated two-phase commit) and in queries (via the scan ranges of dispatched plans).

The interesting states are the failure states. A BE that reboots finds its tablets on disk and re-opens them from the local meta store — no replay, no recovered.edits equivalent, because nothing uncommitted was ever durable (see "Write Path"). But if a tablet's rowset chain turns out to have a hole — versions the FE believes committed that this replica never persisted — the BE marks the tablet TOMBSTONED, refuses all traffic, and reports the fact upstream (`tablet_meta.h`). The FE, which independently notices version lag in the periodic tablet reports, reacts through the tablet scheduler: it either schedules a full clone of a healthy replica to replace the broken one, or, if enough healthy replicas remain, schedules this one to be dropped entirely. Either way the healing is initiated by the catalog and confirmed by the next report — a gossip loop with the FE as the sole authority, rather than a consensus protocol among servers.

The end of the arc mirrors the beginning. Dropping a table or partition is an FE metadata transaction; the FE then issues drop- tablet tasks, the BE transitions the tablet to STOPPED and moves its files into the trash area. One operational caveat, worth stating because it surprises people in incident response: in 4.0.8 the trash retention window is configured by trash_file_expire_time_sec and DEFAULTS TO ZERO — dropped tablet files are removed effectively immediately, and "recover it from trash" is only an option if the cluster was configured with a nonzero window beforehand (`config.cpp`). A decommissioning backend takes a longer road: the FE migrates every tablet off it one clone at a time, waiting for each destination replica to become healthy before scheduling the next, and only when the BE is empty does the cluster remove it — a draining process that has no equivalent urgency in any other transition.

Taken together, the lifecycle is austere compared to HBase's: four states instead of nine, two of which are terminal failure handlers; a single master-driven transition channel instead of a ZooKeeper state zoo; and no split-driven churn at all. What a tablet does most of the time is the only thing that produces no state transitions: quietly accumulate rowsets, compact them, and answer reads.
