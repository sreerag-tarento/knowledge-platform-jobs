package org.sunbird.job.certpublic.functions

import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.certpublic.task.CertificateGeneratorConfig
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class PostProcessOutputMetaData(userid: String, assessmentid: String,courseid: String)

class PostProcessFunction()
