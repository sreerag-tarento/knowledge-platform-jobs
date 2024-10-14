package org.sunbird.job.content.publish.helpers

import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.util._


trait EventPublisher extends ObjectReader with ObjectUpdater {
    override def getExtData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)
        (implicit cassandraUtil: CassandraUtil): Option[ObjectExtData] = None

    override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)
        (implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

    override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)
        (implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

    override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig,  isChild: Boolean = false)
        (implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

    override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)
        (implicit cassandraUtil: CassandraUtil): Unit = None

    override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)
        (implicit cassandraUtil: CassandraUtil): Unit = None
}