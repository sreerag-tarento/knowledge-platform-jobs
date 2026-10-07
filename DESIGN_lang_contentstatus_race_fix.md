# Design Document: Fixing Lost Enrolment-Completion Updates in `activity-aggregate-updater-v2`

## 1. Problem Statement

`activity-aggregate-updater-v2` consumes content-consumption events from Kafka and updates the corresponding course enrolment record (`user_enrolments`) — including the `lang_contentstatus` map, which tracks per-resource (leaf node) completion status per language, and drives the "is this course complete" decision.

**Symptom (production):** For some enrolments, one or two resources that are fully completed in `user_content_consumption` (the granular, per-resource table) never make it into the enrolment's `lang_contentstatus` map. As a result, the completion count never reaches the total leaf-node count, the enrolment is never marked `status = 2` (completed), and the completion certificate is never triggered — even though the user has genuinely finished every resource in the course.

**Where it happens:** Almost exclusively in production, where:
- The producer of these Kafka events runs on multiple pods, so two events for the same user/course/batch (e.g., completing resource A and resource B seconds apart) can be emitted close together, not strictly serialized.
- This Flink job itself runs with multiple parallel replicas/task slots, so two such events can be picked up and processed at nearly the same wall-clock time.

This does not reproduce in single-instance/local testing, which is why it surfaced only in production.

## 2. Root Cause

`ActivityAggregatesFunctionV2.processElement` (`activity-aggregate-updater-v2/src/main/scala/org/sunbird/job/aggregate/v2/functions/ActivityAggregatesFunctionV2.scala:56-116`) handles **every single incoming event independently**, doing a full read-modify-write cycle each time:

1. **Read** the enrolment row, including the entire `lang_contentstatus` map — `getEnrolment` (line 72, impl at `:453-466`).
2. **Merge** the event's resource(s) into an in-memory copy of that map (`updatedLangMap`, lines 96-103; `updateLangContentStatusInUserEnrolment`, lines 388-434).
3. **Write the entire merged map back** with a plain Cassandra `SET`:

```scala
// ActivityAggregatesFunctionV2.scala:288-289
var assignments = QueryBuilder.update(config.dbKeyspace, config.dbUserEnrolmentsTable)
  .`with`(QueryBuilder.set("lang_contentstatus", mapForCassandra))
```

`QueryBuilder.set` **fully overwrites** the `lang_contentstatus` column with exactly the map passed in. There is no `IF` clause, no version/CAS check, no lock of any kind anywhere in this path (confirmed by repo-wide search — no LWT, no Redis lock, no `synchronized`).

**Failure sequence** — two events for the same enrolment (resource A completes, resource B completes, seconds apart), picked up by two different job replicas at nearly the same time:

| Time | Instance 1 (resource A done) | Instance 2 (resource B done) |
|---|---|---|
| t0 | Reads map: `{resource3: done}` | Reads map: `{resource3: done}` |
| t1 | Merges in memory → `{resource3: done, A: done}` | Merges in memory → `{resource3: done, B: done}` |
| t2 | Writes `{resource3: done, A: done}` | — |
| t3 | — | Writes `{resource3: done, B: done}` — **overwrites t2's write entirely** |

Final DB state: `{resource3: done, B: done}` — **resource A's completion is silently lost**, even though `user_content_consumption` (written independently, one row per resource, at `updateContentStatuses`, lines 167-210) correctly recorded A as done. Because the completion count (`completedCount`, line 135 / 411) is derived from this same lossy map, the enrolment can never reach 100% and is never marked complete.

A secondary, related gap: even if the map write itself is made safe (Section 4), the **completion decision** (lines 105-113, 429) is computed from each instance's own pre-write, in-memory snapshot — not from the true post-write state — so it can also under-count and skip firing completion/certificate even after the map is correctly merged in the database.

## 3. How v1 Avoided This Problem

`activity-aggregate-updater` (v1) never does an isolated read-modify-write per event. Its pipeline:

```scala
// ActivityAggregateUpdaterStreamTask.scala:33-35
.keyBy(new ActivityAggregatorKeySelector(config))
.countWindow(config.thresholdBatchReadSize)   // default 1000 (v1 conf, line 40)
.process(new ActivityAggregatesFunction(config, httpUtil))
```

Events are **buffered into a count-based window** (up to `threshold.batch.read.size` events) before any processing happens. When the window fires, `ActivityAggregatesFunction.process` (`ActivityAggregatesFunction.scala:61-133`):

1. **Groups all buffered events in memory** by `(courseId, batchId, userId)` (line 77) — so if resource A and resource B's completion events are both sitting in the same window, they're merged together *before* any database call.
2. Does **one** Cassandra read per distinct enrolment for the whole batch (`getContentStatusFromDB`, line 89, batched via `IN` queries).
3. Merges DB state with the in-memory-merged batch (`finalUserConsumption`, lines 93-96, 221-240).
4. Does **one** batched write per window firing (`updateDB`, line 100).

Because multiple near-simultaneous completions for the same enrolment are coalesced into a single in-memory merge before the database is ever touched, there is no window in which two independent read→write cycles for the same enrolment can race and clobber each other. **v1 has no lock or CAS either** — its safety is entirely structural (batch-then-merge-then-write-once), not transactional. v2 was redesigned as a lean per-event `KeyedProcessFunction` (this was its architecture from day one, not a later regression) and this batching safety net was removed without anything replacing it.

Note: a prior fix attempt (commit `e06c0eda`, "KB-13323 ... LOCAL_QUORUM ... to avoid sync issue") added `ConsistencyLevel.LOCAL_QUORUM` to the v2 read/write path. This only guarantees replica-level read freshness (you won't read stale data from a lagging replica) — it does nothing to prevent two concurrent read-modify-write cycles for the same key from racing. It did not fix this bug.

## 4. Proposed Solution (Option A — targeted fix, no architectural rewrite)

The fix has two parts: make the **write** safe, and make the **completion decision** correct.

### 4.1 Make the `lang_contentstatus` write additive instead of a full overwrite

Cassandra map columns store each key as its own internal storage cell. `QueryBuilder.set(col, wholeMap)` deletes and replaces every cell. `QueryBuilder.putAll(col, deltaMap)` (equivalent to CQL `col = col + {...}`) only touches the cells for the keys included — it **adds** a cell if the key doesn't exist yet, or **overwrites** it if it does, and leaves every other key (including ones this instance never read) completely untouched.

This pattern already exists elsewhere in the same file and is proven in production:

```scala
// ActivityAggregatesFunctionV2.scala:436-439 (existing, for a different column)
QueryBuilder.update(config.dbKeyspace, config.dbUserActivityAggTable)
  .`with`(QueryBuilder.putAll("aggregates", progress.aggregates.asJava))
  .and(QueryBuilder.putAll("agg_last_updated", progress.agg_last_updated.asJava))
```

**Change:** In `updateUserEnrolmentLangStatus` (`ActivityAggregatesFunctionV2.scala:273-310`), replace:

```scala
.`with`(QueryBuilder.set("lang_contentstatus", mapForCassandra))
```

with an additive map update (`putAll` on the nested map, or equivalent `col = col + {...}` construction for the `Map[String, Map[String, Int]]` column type).

Effect: replaying the earlier example — Instance 1's write only touches key `A`, Instance 2's write only touches key `B`. Regardless of write order, both land, and the resulting map is `{resource3: done, A: done, B: done}` — no data loss, no special-casing needed for "key doesn't exist yet" (a `putAll`/map-addition creates a new cell automatically).

### 4.2 Base the completion decision on authoritative, race-free data — not the in-memory snapshot

Even with 4.1, each instance's local `updatedLangMap`/`langContentStatus` (built from its own read, before its own write) may not reflect a sibling instance's concurrent write. Deciding completion from this local snapshot (as done today at lines 135, 411, and used by `triggerCertificateIfRequired` at 160 and `updateLangContentStatusInUserEnrolment` at 429) can still under-count and fail to fire the completion/certificate event, even though the database itself is now correct.

**Change:** Compute the completion check from a fresh, authoritative source rather than the pre-write in-memory map. `user_content_consumption` (read via `readContentConsumption`, lines 313-344) is already race-free by construction — each resource is its own row/primary key, so concurrent writes to different resources never collide. Re-derive "how many leaf nodes are complete" from a live read against this table (or an equivalent authoritative count) at the point the completion decision is made, instead of trusting `langContentStatus`/`updatedLangMap` built earlier in the same function call.

### 4.3 (Hardening) Make the completion status transition idempotent

Add an `IF` condition (e.g., `IF status != 2`) to the final `status = 2` write in `updateUserEnrolmentLangStatus` (lines 294-299). This doesn't affect correctness of the data itself, but prevents two instances that both independently conclude "course is complete" around the same time from each firing a duplicate certificate-issue event (`createIssueCertEvent`, line 361).

### 4.4 Scope of change

| File | Change |
|---|---|
| `activity-aggregate-updater-v2/src/main/scala/org/sunbird/job/aggregate/v2/functions/ActivityAggregatesFunctionV2.scala` | `updateUserEnrolmentLangStatus` (:273-310): `set` → `putAll` for `lang_contentstatus`. `updateLangContentStatusInUserEnrolment` (:388-434) / `triggerCertificateIfRequired` (:118-165): derive `completedCount` from a fresh authoritative read instead of the in-memory map. Optionally add `IF status != 2` to the status write. |

No changes needed to `ActivityAggregatorV2StreamTask.scala` (keying is already correct), the dedup function, or the config files. This is a contained, low-risk change to the write/decision logic inside one function file — it does not require reintroducing windowing or Flink keyed state.

## 5. Why This Closes the Race (Not Just Reduces Its Probability)

- **Data loss** (4.1): `putAll` is commutative and per-cell — concurrent writes to different resource keys of the same map can never clobber each other, regardless of read staleness or timing. This holds for any number of concurrent writers, not just two.
- **Missed completion trigger** (4.2): deriving the decision from `user_content_consumption` — a table that is inherently race-free (one row per resource) — means the completion check always sees true global state at decision time, not a stale local snapshot.
- **Duplicate certificate events** (4.3): closed by making the state transition conditional, so only one writer's transition is ever "the one that counted."

## 6. Out of Scope / Not Proposed Here

- Reintroducing v1-style count-window batching in v2 (a larger architectural change, discussed as "Option B" in prior discussion) — not needed once 4.1/4.2 are in place, since correctness no longer depends on batching.
- Cassandra lightweight transactions (LWT/CAS) on `lang_contentstatus` itself — not needed because the additive `putAll` approach avoids the read-then-conditionally-write pattern entirely for this column.
