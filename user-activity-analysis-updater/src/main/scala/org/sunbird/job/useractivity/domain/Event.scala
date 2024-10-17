package org.sunbird.job.useractivity.domain

import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.useractivity.task.UserActivityAnalysisUpdaterConfig

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long)  extends JobRequest(eventMap, partition, offset) {
    
    def action:String = readOrDefault[String]("edata.action", "")

    def batchId: String = readOrDefault[String]("edata.batchId", "")

    def courseId: String = readOrDefault[String]("edata.courseId", "")

    def userId: String = readOrDefault[String]("edata.userId", "")

    def providerName: String = readOrDefault[String]("edata.providerName", "")

    def primaryCategory: String = readOrDefault[String]("edata.primaryCategory", "")

    def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata", Map[String, AnyRef]())

    def isValid()(config: UserActivityAnalysisUpdaterConfig): Boolean = {
        config.programCertPreProcess.equalsIgnoreCase(action) && !batchId.isEmpty && !courseId.isEmpty &&
          !userId.isEmpty
    }
}
