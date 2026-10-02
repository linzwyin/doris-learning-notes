# Apache Doris 4.0.8 Internals

A source-verified deep dive into the internals of [Apache Doris](https://doris.apache.org/) 4.0.8 —
from cluster architecture down to the write path of a single row.

**Verified against `apache-doris-4.0.8-rc02-src`.** Every mechanism claim carries a
`file:line` anchor into the source tree, so each statement can be independently checked.

> 中文简介见下文。

---

## Why this repository is different

Official documentation tells you *what* Doris does. Blog posts tell you *what the author
thinks* it does. This repository aims at a third thing: **what the code actually does**,
told in textbook order — concepts first, mechanisms second, source-level detail third.

- Every architectural statement is anchored to `file:line` in the 4.0.8 source tree.
- Written in the tradition of *HBase: The Definitive Guide*: plain-language explanations,
  ASCII diagrams, no code dumps.
- Organized shallow-to-deep: follow a row's whole life first, then dissect each organ.

## How to verify a claim

Each mechanism statement looks like:

> the planner ships the expression form via `partitionFunctionExprs`
> (`fe/fe-core/src/main/java/org/apache/doris/planner/OlapTableSink.java:548`)

To check it:

```bash
# clone the matching source tree
git clone --branch 4.0.8-rc02 https://github.com/apache/doris.git
# jump to the anchor
sed -n '540,560p' fe/fe-core/src/main/java/org/apache/doris/planner/OlapTableSink.java
```

If an anchor ever drifts in a future release, that is expected — the anchors are a
property of **4.0.8**, not of Doris forever.

## Contents

| Part | Title | Status |
|------|-------|--------|
| 1 | [Overview and Core Concepts](docs/part-01-overview-and-core-concepts.md) | ✅ complete |
| 2 | [FE High Availability](docs/part-02-fe-high-availability.md) | ✅ complete |
| 3 | [Data Organization and Storage Components](docs/part-03-data-organization-and-storage-components.md) | ✅ 3.1–3.6 complete · 🚧 3.7–3.9 skeleton (Bucketing, Tablet Scheduling, Heartbeat & Tablet Report) |
| 4 | [Query Architecture](docs/part-04-query-architecture.md) | ✅ complete |
| 5 | [Lifecycle of Write and Read Queries and Load](docs/part-05-lifecycle-of-write-and-read-queries.md) | ✅ 5.1–5.2 complete · 🚧 5.3 skeleton (Load Channel and MemTable) |
| 6 | [Storage Architecture](docs/part-06-doris-storage-architecture.md) (LSM-tree, write path, file formats, read path) | ✅ complete |
| 8 | Schema Change | 🚧 skeleton |
| 9 | Transactions | 🚧 skeleton |
| 10 | Data Loading | 🚧 skeleton |
| 11 | Data Updating and Deleting | 🚧 skeleton |
| 12 | Auto-Increment | 🚧 skeleton |
| 13 | Data Exporting | 🚧 skeleton |
| 14 | JOIN | 🚧 skeleton |
| 15 | Materialized View | 🚧 skeleton |
| 16 | Stage | 🚧 skeleton |
| 17 | Source Overview | 🚧 skeleton |
| 18 | Operator Overview | 🚧 skeleton |
| 19 | Sink Overview | 🚧 skeleton |
| 20 | Performance Tuning | 🚧 skeleton |
| 21 | Data Catalog | 🚧 skeleton |

Parts 8–21 already have their section skeletons in the working manuscript; content is being filled in the same source-verified manner, one Part at a time.

Legend: ✅ complete · 🚧 skeleton (headings in place, content in progress) · planned (not started).

## Reading order

- **In a hurry** — read Part 1, then 3.1–3.5 (storage hierarchy), then 3.6 (how a row
  finds its tablet).
- **Interview prep** — Parts 2 and 3 cover the most-asked ground: FE HA / BDB-JE
  election, checkpoints, fencing, and the MemTable → Rowset → Segment hierarchy.
- **Thorough** — linear order, top to bottom. Each Part ends where the next one begins.

## Scope and disclaimer

- This document describes the **classic compute-storage-coupled** deployment. The cloud
  (compute-storage separation) mode is noted where relevant but not covered.
- This is an independent learning and teaching project, not an official Apache Doris
  publication. For operational decisions, always consult the official documentation.
- Line numbers refer to `apache-doris-4.0.8-rc02-src` and will drift in other releases.

## License

Documentation is licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

---

## 中文简介

这是一份**基于源码逐行核实**的 Apache Doris 4.0.8 内核机制文档。与官方文档（讲用法）
和博客（讲理解）不同，这里只讲**代码里真实发生的事**，并且每个机制论断都带有
`文件:行号` 锚点，读者可以独立复核。

- 体例对标《HBase 权威指南》：概念先行、机制其次、源码细节殿后，全程 ASCII 图，不贴代码。
- 组织方式由浅入深：先跟着一行数据走完它的一生，再逐个器官下刀解剖。
- 版本钉死 `apache-doris-4.0.8-rc02-src`——锚点是这个版本的属性，而非 Doris 的永恒属性。

**阅读建议**：赶时间读 Part 1 + 3.1–3.6；面试准备重点看 Part 2（FE 高可用）和 Part 3
（存储层级与数据分布）；系统学习则按顺序线性阅读。

本文档为个人独立学习项目，非 Apache Doris 官方出版物；生产决策请以官方文档为准。
