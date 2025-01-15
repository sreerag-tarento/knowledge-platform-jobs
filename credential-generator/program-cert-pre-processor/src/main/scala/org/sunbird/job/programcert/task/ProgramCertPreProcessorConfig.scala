package org.sunbird.job.programcert.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class ProgramCertPreProcessorConfig(override val config: Config) extends BaseJobConfig(config, "program-cert-pre-processor") {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  //Redis config
  val collectionCacheStore: Int = 0
  val relationCacheStore: Int = config.getInt("redis-meta.database.collectionIndex")
  val contentCacheStore: Int = 5
  val metaRedisHost: String = config.getString("redis-meta.host")
  val metaRedisPort: Int = config.getInt("redis-meta.port")

  //kafka config
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaOutputTopic: String = config.getString("kafka.output.topic")
  val kafkaOutputFailedTopic: String = config.getString("kafka.output.failed.topic")
  val certificatePreProcessorConsumer: String = "program-cert-pre-processor-consumer"
  val generateCertificateProducer = "generate-certificate-sink"
  val generateCertificateFailedEventProducer = "generate-certificate-failed-event-sink"
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val generateCertificateParallelism: Int = config.getInt("task.generate_certificate.parallelism")

  //Tags
  val generateCertificateOutputTagName = "generate-certificate-request"
  val generateCertificateOutputTag: OutputTag[String] = OutputTag[String](generateCertificateOutputTagName)
  val generateCertificateFailedOutputTagName = "generate-certificate-failed-request"
  val generateCertificateFailedOutputTag: OutputTag[String] = OutputTag[String](generateCertificateFailedOutputTagName)

  //Cassandra config
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val keyspace: String = config.getString("lms-cassandra.keyspace")
  val userEnrolmentsTable: String = config.getString("lms-cassandra.user_enrolments.table")
  val dbBatchId = "batchId"
  val dbCourseId = "courseid"
  val dbUserId = "userid"
  val contentHierarchyTable: String = "content_hierarchy"
  val contentHierarchyKeySpace: String = "dev_hierarchy_store"
  val Hierarchy: String = "hierarchy"
  val childrens: String = "children"
  val batches: String = "batches"

  //API URL
  val contentBasePath = config.getString("service.content.basePath")
  val learnerBasePath = config.getString("service.learner.basePath")
  val userReadApi = config.getString("user_read_api")
  val contentReadApi = "/content/v4/read"
  val contentServiceBase: String = config.getString("service.content.basePath")
  val contentReadURL = contentServiceBase + "/content/v3/read/"

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val dbReadCount = "db-read-count"
  val dbUpdateCount = "db-update-count"
  val cacheHitCount = "cache-hit-cout"
  val programCertIssueEventsCount = "program-cert-issue-events-count"
  val cacheMissCount = "cache-miss-count"

  //Constants
  val status: String = "status"
  val name: String = "name"
  val defaultHeaders = Map[String, String]("Content-Type" -> "application/json")
  val identifier: String = "identifier"
  val userAccBlockedErrCode = "UOS_USRRED0006"
  val programCertPreProcess: String = "program_cert_pre_process"
  val parentCollections: String = "parentCollections"
  val issuedCertificates: String = "issued_certificates"
  val primaryCategory: String = "primaryCategory"
  val leafNodes: String = "leafNodes"
  val contentStatus: String = "contentstatus"
  val progress: String = "progress"
  val allowedPrimaryCategoryForProgram = List[String]("Course")
  val childrenCourses: String = "childrenCourses"
  val leafNodesKey = "leafnodes"

}
