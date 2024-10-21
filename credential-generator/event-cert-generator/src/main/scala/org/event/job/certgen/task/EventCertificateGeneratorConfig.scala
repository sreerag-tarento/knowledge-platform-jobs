package org.event.job.certgen.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.event.job.certgen.function.{NotificationMetaData, UserFeedMetaData}
import org.sunbird.job.BaseJobConfig

import java.util


class EventCertificateGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "event-certificate-generator") {


  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val notificationMetaTypeInfo: TypeInformation[NotificationMetaData] = TypeExtractor.getForClass(classOf[NotificationMetaData])
  implicit val userFeeMetaTypeInfo: TypeInformation[UserFeedMetaData] = TypeExtractor.getForClass(classOf[UserFeedMetaData])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaAuditEventTopic: String = config.getString("kafka.output.audit.topic")

  val enableSuppressException: Boolean = if (config.hasPath("enable.suppress.exception")) config.getBoolean("enable.suppress.exception") else false
  val enableRcCertificate: Boolean = if (config.hasPath("enable.rc.certificate")) config.getBoolean("enable.rc.certificate") else false
  val minCompletePercentageForCertificate: Double = if (config.hasPath("minCompletePercentageForCert")) config.getDouble("minCompletePercentageForCert") else 50.0

  // Producers
  val certificateGeneratorAuditProducer = "collection-certificate-generator-audit-events-sink"

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val notifierParallelism: Int = if (config.hasPath("task.notifier.parallelism")) config.getInt("task.notifier.parallelism") else 1
  val userFeedParallelism: Int = if (config.hasPath("task.userfeed.parallelism")) config.getInt("task.userfeed.parallelism") else 1

  //ES configuration
  val esConnection: String = config.getString("es.basePath")
  val certIndex: String = "certs"
  val certIndexType: String = "_doc"


  // Cassandra Configurations
  val sbKeyspace: String = config.getString("lms-cassandra.sbkeyspace")
  val certRegTable: String = config.getString("lms-cassandra.certreg.table")
  val dbEnrollmentTable: String = config.getString("lms-cassandra.user_enrolments.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val dbEventBatchTable: String = config.getString("lms-cassandra.event_batch.table")
  val dbBatchId = "batchid"
  val dbCourseId = "courseid"
  val dbUserId = "userid"
  val active: String = "active"
  val issuedCertificates: String = "issued_certificates"
  val dbEventId = "eventid"
  val dbContentId = "contentid"
  val dbContextId = "contextid"

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val enrollmentDbReadCount = "enrollment-db-read-count"
  val dbUpdateCount = "db-update-user-enrollment-count"
  val notifiedUserCount = "notified-user-count"
  val skipNotifyUserCount = "skipped-notify-user-count"
  val failedNotifyUserCount = "failed-notify-user-count"
  val eventBatchdbReadCount = "db-event-batch-read-count"

  // Consumers
  val certificateGeneratorConsumer = "certificate"

  // env vars
  val storageType: String = config.getString("cloud_storage_type")
  val containerName: String = config.getString("cloud_storage_container")
  val storageKey: String = config.getString("cloud_storage_key")
  val storageSecret: String = config.getString("cloud_storage_secret").replace("\\n", "\n")
  val storageEndpoint: String = config.getString("cloud_storage_endpoint")
  val azureStorageSecret: String = if (config.hasPath("cert_azure_storage_secret")) config.getString("cert_azure_storage_secret") else ""
  val azureStorageKey: String = if (config.hasPath("cert_azure_storage_key")) config.getString("cert_azure_storage_key") else ""
  val domainUrl: String = config.getString("cert_domain_url")
  val encServiceUrl: String = config.getString("service.enc.basePath")
  val certRegistryBaseUrl: String = config.getString("service.certreg.basePath")
  val learnerServiceBaseUrl: String = config.getString("service.learner.basePath")
  val basePath: String = domainUrl.concat("/").concat("certs")
  val awsStorageSecret: String = if (config.hasPath("cert_aws_storage_secret")) config.getString("cert_aws_storage_secret") else ""
  val awsStorageKey: String = if (config.hasPath("cert_aws_storage_key")) config.getString("cert_aws_storage_key") else ""
  val addCertRegApi = "/certs/v2/registry/add"
  val userFeedCreateEndPoint: String = "/private/user/feed/v1/create"
  val notificationEndPoint: String = "/v2/notification"
  val cephs3StorageSecret: String = if (config.hasPath("cert_cephs3_storage_secret")) config.getString("cert_cephs3_storage_secret") else ""
  val cephs3StorageKey: String = if (config.hasPath("cert_cephs3_storage_key")) config.getString("cert_cephs3_storage_key") else ""
  val cephs3StorageEndPoint: String = if (config.hasPath("cert_cephs3_storage_endpoint")) config.getString("cert_cephs3_storage_endpoint") else ""
  val AZURE: String = "azure"
  val CEPHS3: String = "cephs3"
  val AWS: String = "aws"
  val rcBaseUrl: String = config.getString("service.rc.basePath")
  val rcEntity: String = config.getString("service.rc.entity")
  val rcCreateApi: String = "service.rc.create.api"
  val rcDeleteApi: String = "service.rc.delete.api"
  val rcSearchApi: String = "service.rc.search.api"


  //constant
  val DATA: String = "data"
  val RECIPIENT_NAME: String = "recipientName"
  val ISSUER: String = "issuer"
  val BADGE_URL: String = "/Badge.json"
  val ISSUER_URL: String = basePath.concat("/Issuer.json")
  val EVIDENCE_URL: String = basePath.concat("/Evidence.json")
  val CONTEXT: String = basePath.concat("/v1/context.json")
  val PUBLIC_KEY_URL: String = "_publicKey.json"
  val VERIFICATION_TYPE: String = "SignedBadge"
  val SIGNATORY_EXTENSION: String = basePath.concat("v1/extensions/SignatoryExtension/context.json")
  val ACCESS_CODE_LENGTH: String = "6"
  val EDATA: String = "edata"
  val RELATED: String = "related"
  val OLD_ID: String = "oldId"
  val BATCH_ID: String = "batchId"
  val COURSE_ID: String = "courseId"
  val TEMPLATE_ID: String = "templateId"
  val USER_ID: String = "userId"
  val EVENT_ID: String = "eventId"
  val CONTENT_ID: String = "contentid"
  val CONTEXT_ID: String = "contextid"


  val courseId = "courseId"
  val batchId = "batchId"
  val userId = "userId"
  val notifyTemplate = "notifyTemplate"
  val firstName = "firstName"
  val trainingName = "TrainingName"
  val heldDate = "heldDate"
  val recipientUserIds = "recipientUserIds"
  val identifier = "identifier"
  val body = "body"
  val notificationSmsBody = "Congratulations! Download your course certificate from your profile page. If you have a problem downloading it on the mobile, update your DIKSHA app"
  val request = "request"
  val filters = "filters"
  val fields = "fields"
  val issued_certificates = "issued_certificates"
  val eData = "edata"
  val name = "name"
  val token = "token"
  val lastIssuedOn = "lastIssuedOn"
  val templateUrl = "templateUrl"
  val ratingMidPoint: String = "/app/toc/"
  val ratingEndPoint: String = "/overview?batchId="
  val ratingPageUrl: String = "ratingPageUrl"
  val `type` = "type"
  val certificate = "certificate"
  val action = "action"
  val courseName = "courseName"
  val templateId = "templateId"
  val cert_templates = "cert_templates"
  val courseBatch = "CourseBatch"
  val l1 = "l1"
  val id = "id"
  val data = "data"
  val category = "category"
  val certificates = "certificates"
  val badCharList = if (config.hasPath("task.rc.badcharlist")) config.getString("task.rc.badcharlist") else "\\x00,\\\\aaa,\\aaa,Ø,Ý"

  // Tags
  val auditEventOutputTagName = "audit-events"
  val auditEventOutputTag: OutputTag[String] = OutputTag[String](auditEventOutputTagName)
  val notifierOutputTag: OutputTag[NotificationMetaData] = OutputTag[NotificationMetaData]("notifier")
  val userFeedOutputTag: OutputTag[UserFeedMetaData] = OutputTag[UserFeedMetaData]("user-feed")

  //UserFeed constants
  val priority: String = "priority"
  val userFeedMsg: String = "You have earned a certificate! Download it from your profile page."
  val priorityValue = 1
  val userFeedCount = "user-feed-count"

  val courseProvider: String = "courseProvider"
  val coursePosterImage: String = "coursePosterImage"
  val profileUpdateLink: String = "profileUpdateLink"
  val profileUpdateUrl: String = "/app/user-profile/details"

  val newEmailTemplateNotificationEndPoint: String = "/v1/notification/email"
  val webPortalUrl: String = config.getString("web.portal.url")

  val enableUserNotification: Boolean = if (config.hasPath("enable.user.email.notification")) config.getBoolean("enable.user.email.notification") else true

}