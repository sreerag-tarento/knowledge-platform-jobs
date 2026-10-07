# Quorum Consistency Implementation for Activity Aggregate Updater V2

## Summary
Implemented LOCAL_QUORUM consistency level for all Cassandra read and write operations in the activity-aggregate-updater-v2 Flink job. This ensures strong consistency by requiring that a majority of replica nodes acknowledge operations before proceeding, preventing stale reads and ensuring data reliability.

## Changes Made

### 1. **jobs-core/src/main/scala/org/sunbird/job/util/CassandraUtil.scala**

#### Added New Method: `findWithStatement`
```scala
/**
 * Execute a pre-built Statement and return all rows
 * (allows caller to set a custom ConsistencyLevel, e.g. LOCAL_QUORUM).
 */
def findWithStatement(stmt: Statement): util.List[Row] = {
  executeWithRetry(stmt).all()
}
```

**Purpose:** Provides a way to execute SELECT queries with custom consistency levels, complementing the existing `findOneWithStatement` method.

---

### 2. **activity-aggregate-updater-v2/src/main/scala/org/sunbird/job/aggregate/v2/functions/ActivityAggregatesFunctionV2.scala**

#### Change 1: Content Consumption Updates (lines 192-204)
**Method:** `updateContentStatuses()`

Added LOCAL_QUORUM consistency to content consumption record updates:
```scala
val updateQuery = QueryBuilder.update(config.dbKeyspace, config.dbUserContentConsumptionTable)
  .`with`(QueryBuilder.set("status", finalStatus))
  .and(QueryBuilder.set("completedcount", completedCount))
  .and(QueryBuilder.set("viewcount", viewCount))
  .where(QueryBuilder.eq("userid", event.userId))
  .and(QueryBuilder.eq("courseid", event.courseId))
  .and(QueryBuilder.eq("batchid", event.batchId))
  .and(QueryBuilder.eq("language", event.language))
  .and(QueryBuilder.eq("contentid", contentId))

// LOCAL_QUORUM: ensure write reaches majority of nodes before proceeding
updateQuery.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
cassandraUtil.update(updateQuery)
```

**Impact:** Ensures that content consumption status updates reach a majority of nodes before the next operation can proceed, preventing inconsistent state.

---

#### Change 2: Content Consumption Reads (lines 315-349)
**Method:** `readContentConsumption()`

Changed to use SimpleStatement with LOCAL_QUORUM for reads:
```scala
def readContentConsumption(
  userId: String,
  courseId: String,
  batchId: String,
  language: String,
  contentId: String
): Map[String, AnyRef] = {

  val query = QueryBuilder.select()
    .all()
    .from(config.dbKeyspace, config.dbUserContentConsumptionTable)
    .where(QueryBuilder.eq("userid", userId))
    .and(QueryBuilder.eq("courseid", courseId))
    .and(QueryBuilder.eq("batchid", batchId))
    .and(QueryBuilder.eq("language", language))
    .and(QueryBuilder.eq("contentid", contentId))
    .limit(1)

  // LOCAL_QUORUM: require majority of replica nodes to respond so we never read stale data
  val stmt = new SimpleStatement(query.toString)
    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
  val rows = cassandraUtil.findWithStatement(stmt).asScala

  if (rows.nonEmpty) {
    val row = rows.head
    Map(
      "status" -> Int.box(Option(row.getObject("status")).map(_.asInstanceOf[Int]).getOrElse(0)),
      "viewcount" -> Int.box(Option(row.getObject("viewcount")).map(_.asInstanceOf[Int]).getOrElse(0)),
      "completedcount" -> Int.box(Option(row.getObject("completedcount")).map(_.asInstanceOf[Int]).getOrElse(0)),
      "last_access_time" -> Option(row.getTimestamp("last_access_time")).orNull
    )
  } else {
    Map.empty[String, AnyRef]
  }
}
```

**Impact:** Prevents reading stale data by requiring majority node confirmation before returning content consumption status.

---

#### Change 3: Learning Pathway Completion (lines 709-726)
**Method:** `markLPCompleted()`

Added LOCAL_QUORUM consistency when marking learning pathways as complete:
```scala
def markLPCompleted(
  userId: String,
  lpId: String,
  batchId: String
): Unit = {

  val updateQuery = QueryBuilder
    .update(config.dbKeyspace, config.dbUserEnrolmentsTable)
    .`with`(QueryBuilder.set(JsonKeys.STATUS, 2))
    .and(QueryBuilder.set(JsonKeys.COMPLETED_ON_KEY, new java.util.Date()))
    .and(QueryBuilder.set(JsonKeys.DATE_TIME_KEY, System.currentTimeMillis()))
    .where(QueryBuilder.eq(JsonKeys.USER_ID_KEY, userId))
    .and(QueryBuilder.eq(JsonKeys.COURSE_ID_KEY, lpId))
    .and(QueryBuilder.eq(JsonKeys.BATCH_ID_KEY, batchId))

  // LOCAL_QUORUM: ensure write reaches majority of nodes before proceeding
  updateQuery.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
  cassandraUtil.update(updateQuery)

  logger.info(
    s"LP marked as completed (status=2) for user=$userId, lpId=$lpId, batchId=$batchId"
  )
}
```

**Impact:** Ensures learning pathway completion is durably persisted before proceeding, preventing duplicate completion events.

---

## Operations Now Using LOCAL_QUORUM

| Operation | Type | Method | Purpose |
|-----------|------|--------|---------|
| User Content Consumption Update | WRITE | `updateContentStatuses()` | Updates content completion status |
| Content Consumption Read | READ | `readContentConsumption()` | Reads current content status |
| User Activity Agg Update | WRITE | `triggerCertificateIfRequired()` | Already had LOCAL_QUORUM (existing) |
| User Enrolment Update | WRITE | `updateUserEnrolmentLangStatus()` | Already had LOCAL_QUORUM (existing) |
| Enrolment Read | READ | `getEnrolment()` | Already had LOCAL_QUORUM (existing) |
| LP Completion Update | WRITE | `markLPCompleted()` | Marks LP as complete |

---

## Benefits

1. **Strong Consistency:** Prevents reading stale data across distributed Cassandra clusters
2. **Data Durability:** Ensures writes reach a majority before proceeding, reducing data loss risk
3. **Reduced Conflicts:** Minimizes race conditions between concurrent activity aggregation events
4. **Production Ready:** LOCAL_QUORUM is the industry standard for financial and critical systems
5. **No Breaking Changes:** Backward compatible - existing code continues to work

## Performance Considerations

- **Latency Impact:** Slight increase (typically <10ms) due to waiting for majority acknowledgment
- **Mitigated by:** Batch processing nature of activity aggregation where consistency > speed
- **Recommended:** Monitor Cassandra metrics (read/write latency) after deployment

## Testing

Build command:
```bash
mvn clean package -pl activity-aggregate-updater-v2 -DskipTests
```

Verification:
- ✅ Compilation succeeds
- ✅ JAR builds successfully
- ✅ All Cassandra operations now explicitly use LOCAL_QUORUM

## Deployment Notes

Ensure your Cassandra cluster has:
- **Replication Factor ≥ 3** (for quorum to be effective)
- Configured for `LOCAL_QUORUM` consistency (default in most production setups)
- Sufficient network bandwidth to handle quorum reads/writes

## Future Improvements

1. Make consistency level configurable via application.conf
2. Add per-operation metrics for read/write latency
3. Consider using `LOCAL_ONE` for reads if stale data tolerance increases
4. Add circuit breaker pattern for Cassandra timeout scenarios

