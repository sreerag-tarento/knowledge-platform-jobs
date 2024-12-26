package org.sunbird.job.collectioncert.domain

import org.sunbird.job.collectioncert.task.CollectionCertPreProcessorConfig
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long)  extends JobRequest(eventMap, partition, offset) {
    
    def action:String = readOrDefault[String]("edata.action", "")

    def batchId: String = readOrDefault[String]("edata.batchId", "")

    def courseId: String = readOrDefault[String]("edata.courseId", "")

    def userId: String = {
        val list = readOrDefault[List[String]]("edata.userIds", List[String]())
        if(list.isEmpty) "" else list.head
    }
    
    def reIssue: Boolean = readOrDefault[Boolean]("edata.reIssue", false)

    def reIssueDate: Long = readOrDefault[Long]("edata.reIssueDate", 0L)

    def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata", Map[String, AnyRef]())


    def isValid()(config: CollectionCertPreProcessorConfig): Boolean = {
        config.issueCertificate.equalsIgnoreCase(action) && !batchId.isEmpty && !courseId.isEmpty &&
          !userId.isEmpty
    }

    def eventId: String = readOrDefault[String]("edata.eventId", "")

    def isValidEventType()(config: CollectionCertPreProcessorConfig): Boolean = { 
        config.issueEventCertificate.equalsIgnoreCase(action) && !batchId.isEmpty && !eventId.isEmpty && !userId.isEmpty
    }

    def eventType: String = readOrDefault[String]("edata.eventType", "")
}
