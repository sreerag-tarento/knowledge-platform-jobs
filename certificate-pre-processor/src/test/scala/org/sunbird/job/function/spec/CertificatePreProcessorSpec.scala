package org.sunbird.job.function.spec

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito.when
import org.sunbird.job.Metrics
import org.sunbird.job.cache.RedisConnect
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.CertificateApiService
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}
import redis.embedded.RedisServer
import org.sunbird.job.functions.CertificatePreProcessor

class CertificatePreProcessorSpec extends BaseTestSpec {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig = new CertificatePreProcessorConfig(config)
  val mockHttpUtil = mock[HttpUtil]
  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {
    {
      put(jobConfig.dbReadCount, new AtomicLong())
      put(jobConfig.cacheReadCount, new AtomicLong())
    }
  })
  var cassandraUtil: CassandraUtil = _
  var redisServer: RedisServer = _
  val redisConnect = new RedisConnect(jobConfig)
  lazy private val gson = new Gson()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6340)
    redisServer.start()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.dbHost, jobConfig.dbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      redisServer.stop()
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => {
      }

    }
  }

  //test should generate final output event
  // mock db,api and certificateEventGenerator
  it should "prepareEventData" in {
    mockAll()
    val event =  gson.fromJson(EventFixture.EVENT_1, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    // test in streataskspec
//    new CertificatePreProcessor(jobConfig).processElement(event, ,metrics)

  }

  private def mockAll(): Unit = {
    CertificateApiService.httpUtil = mockHttpUtil
    when(mockHttpUtil.post(endsWith("/v1/search"), any[String])).thenReturn(HTTPResponse(200, """{"id":"api.user.search","ver":"v1","ts":"2020-10-24 14:24:57:555+0000","params":{"resmsgid":null,"msgid":"df3342f1082aa191128453838fb4e61f","err":null,"status":"success","errmsg":null},"responseCode":"OK","result":{"count":1,"content":[{"firstName":"Reviewer","lastName":"User","maskedPhone":"******7418","rootOrgName":"Sunbird","userName":"ntptest103","rootOrgId":"ORG_001"}]}}""".stripMargin))
    when(mockHttpUtil.get(endsWith("/v3/read/do_11309999837886054415"))).thenReturn(HTTPResponse(200, """{ "id": "api.v3.read", "ver": "1.0", "ts": "2020-10-24T15:25:39.187Z", "params": { "resmsgid": "2be6e430-160d-11eb-98c2-3bbec8c9cf05", "msgid": "2be4e860-160d-11eb-98c2-3bbec8c9cf05", "status": "successful", "err": null, "errmsg": null }, "responseCode": "OK", "result": {"content": {"name": "Test-audit-svg-7-oct-2", "status": "Live", "code": "org.sunbird.4SZ9XP"}}}""".stripMargin))
    when(mockHttpUtil.post(endsWith("/v1/org/read"), any[String])).thenReturn(HTTPResponse(200, """{ "id": "api.org.read", "ver": "v1", "ts": "2020-10-24 14:47:23:631+0000", "params": { "resmsgid": null, "msgid": "cc58e03e2789f6db8b4695a43a5c8a39", "err": null, "status": "success", "errmsg": null }, "responseCode": "OK", "result": {"keys": {"signKeys": [{"testKey": "testValue"}]}}}""".stripMargin))
  }

  private def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }
}
