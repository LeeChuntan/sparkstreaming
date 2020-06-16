package sparkstreaming

import conf.MyConf
import util.DBUtil
import util.KafkaUtil
import db.AssociatedQuery
import java.lang
import sparkstreaming.SparkstreamingBusiness
import util.MySqlQuery
import db.ReadTable
import org.I0Itec.zkclient.ZkClient
import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.sql._
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import com.mysql.jdbc.Driver
import kafka.utils.ZKGroupTopicDirs
import org.apache.spark.sql.types.DateType
import scala.collection.mutable
import scala.collection.mutable.{HashMap, Map}


/**
  * 主程序 读取消息流
  */
class SparkStreamingKafka
object SparkStreamingKafka {

  def main(args: Array[String]): Unit = {
    /**
      * kafka 配置参数 设置消费主体
      */
    val topic = MyConf.kafka_topic
    //kafka服务器
    val brokers = MyConf.kafka_brokers
    //消费者组
    val group = MyConf.kafka_group
    //多个topic 去重 消费
    val topics: Set[String] = Set(topic)
    //指定zk地址
    val zkQuorum = MyConf.zookeeper
    //topic在zk里面的数据路径 用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //得到zk中的数据路径
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingkafka").setMaster("local[*]")

    //设置流间隔批次
    val sc = new SparkContext(conf)
    val streamingContext = new StreamingContext(sc, Durations.seconds(3))
    val sqlc = new SQLContext(sc)

    //隐式转换需要
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    /**
      * 加载需要的配置
      * 1.地区表
      * 2.白名单
      * 3.系统特征表
      * 4.加载端口系统设置表
      * 5.异常时间段设定表
      */

    //1.加载地区表
    val AreaTable = ReadTable.ReadTable(conf, MyConf.mysql_table_area)
    AreaTable.cache()
    AreaTable.createOrReplaceTempView("area")

    //2.得到白名单关联表 解析出非名单内IP
    val datawhite = MySqlQuery.SqlQuery(conf, sc, MyConf.modeld_white)
    val rddwhite = datawhite.rdd
    var mapIp: Map[String,Int] = Map()
    var AbnormalIpLevel = 0
    rddwhite.collect().foreach(row =>{
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      AbnormalIpLevel = row.getAs[Int]("level")
//      AbnormalIpLevel = level
      val size: Int= data.size()
      for (i <- 0 to size-1) {
        val nObject = data.getJSONObject(i)
        val str = nObject.getString("ip")
        mapIp += (str  ->  AbnormalIpLevel)
      }
    })
    val listIp =  mapIp.map(_._1).toList
    println(mapIp)

    //3.解析出url规则  错误状态返回码
    val dataPort = MySqlQuery.SqlQuery(conf, sc, MyConf.modeld_landing)
    val rddPort = dataPort.rdd
    var mapSeverIp: Map[String,Int] = Map()
    var mapPort: Map[String,Int] = Map()
    var mapError: Map[String,Int] = Map()
    var mapUrl: Map[String,Int] = Map()
    var failLevel = 0
    rddPort.collect().foreach(row =>{
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      failLevel = level
      val size: Int= data.size()
      for (i <- 0 to size-1) {
        val nObject = data.getJSONObject(i)
        val port = nObject.getString("port")
        val serverIp = nObject.getString("ip")
        val error = nObject.getString("error")
        val url = nObject.getString("url")
        mapSeverIp += (serverIp -> level)
        mapPort += (port  -> level)
        mapError += (error -> level)
        mapUrl += (url -> level)
      }
    })
    val listUrl = mapUrl.map(_._1).toList
    val listError = mapError.map(_._1).toList
    val listServerIp = mapSeverIp.map(_._1).toList
    val listPort =  mapPort.map(_._1).toList
    val identificationFailLevel = mapUrl.map(_._2)


    //4.提取所有服务系统端口 服务地址
    val systemCode = ReadTable.ReadTable(conf,MyConf.mysql_table_sys)
    systemCode.createOrReplaceTempView("sys")
    var mapPortIp: Map[String,String] = Map()
    systemCode.collect().foreach(row =>{
      val ip = row.getAs[String]("ip")
      val port = row.getAs[String]("port")
      mapPortIp += (port -> ip)
    })
    val allServerIp = mapPortIp.map(_._2).toList.distinct
    val allServerport = mapPortIp.map(_._1).toList.distinct

    //5.获取异常时间模型
    val timeMap = AssociatedQuery.AssociatedQueryTime(conf, sqlc)

    /**
      * kafka参数配置
      */
    val kafkaParams = Map(
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: lang.Boolean),
      "auto.offset.reset" -> MyConf.kafka_offset_position
    )

    //定义一个空数据流 根据偏移量选择
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    //创建客户端 读取 更新 偏移量
    val zkClient = new ZkClient(zkQuorum)
    val zkExist: Boolean = zkClient.exists(s"$zkTopicPath")
    //使用历史偏移量创建流
    if (zkExist){
      val clusterEarliestOffsets: Map[Long, Long] = KafkaUtil.getPartitionOffset(topic)
      val nowOffsetMap: HashMap[TopicPartition, Long] = KafkaUtil.getPartitionOffsetZK(topic, zkTopicPath, clusterEarliestOffsets, zkClient)

      kafkaStream = KafkaUtils.createDirectStream(streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams, nowOffsetMap))
    }else {
      kafkaStream = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    }

    //通过rdd转换得到偏移量的范围
    var offsetRanges = Array[OffsetRange]()

    //对数据流进行处理
    kafkaStream.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val value = rdd.map(rd => rd.value())
        val df = spark.read.json(value)

        println("数据接受")
        df.show()

        //将dataframe识别的字段提取出来，观察是否存在需要字段
        val ColumnsList = df.columns.toList
        /**
          * 接收数据准备  该流量是否存在destination,query,http
          */
        if (ColumnsList.contains("query") && ColumnsList.contains("http") && ColumnsList.exists(word => word == "destination")) {
          //得到访问服务的全部流量   匹配上面提取出的服务系统地址和端口
          val dfData = df.filter($"destination.port".isin(allServerport:_*) && $"destination.ip".isin(allServerIp:_*))
          val data = dfData.select($"source.ip" as "ip",
            $"destination.ip" as "serviceIp",
            $"destination.port" as "port",
            $"@timestamp" as "requestTime",
            $"query" as "url",              //判断是否认证
            $"url.query" as "query",       //用来提取用户名
            $"http.response.status_code" as "status_code"
          )
          println("提取元素输出")
          data.show()

          //修改提取时间  时间格式转换
//          dataFlow.createOrReplaceTempView("data")
//          val data = sqlc.sql("select ip, serviceIp, port, SUBSTR(`requestTime`,1,10) as requestTime, url, query, status_code from data")
//          data.show()


          if (!data.rdd.isEmpty()) {
//            val VisitFlow = data.filter($"port".isin(listPort: _*) && $"serviceIp".isin(listServerIp: _*) && $"status_code".isNotNull)

            data.createOrReplaceTempView("flow")
            println("访问服务元素输出")
            data.show()

            //将访问服务的流量与地址信息进行匹配
            val AreaIpMatching = sqlc.sql(
              "SELECT w.ip, serviceIp, port, requestTime, url, query, status_code, c_area from flow w LEFT JOIN area a ON w.ip = a.c_ip"
            )
            AreaIpMatching.createOrReplaceTempView("mat")
            println("地址匹配上")
            AreaIpMatching.show()

            //将端口和系统进行匹配 分辨访问了哪些系统
            val PortMatching = sqlc.sql(
              "SELECT m.ip, serviceIp, monitordict_id as sysid, m.port, requestTime, url, query, status_code, c_area from mat m LEFT JOIN sys s ON m.port = s.port"
            )
            println("系统匹配上")
            PortMatching.show()
            PortMatching.printSchema()

            //第一次过滤  判断是否进行单点登陆认证的流量
            val VisitFlow = PortMatching.filter($"port".isin(listPort: _*) && $"serviceIp".isin(listServerIp: _*) && $"status_code".isNotNull)

            //第二次过滤 查看流量是否在白名单内 不在定义为异常输出
            val AbnormalIp = VisitFlow.filter(!$"ip".isin(listIp: _*))
            println("白名单外流量输出")
            AbnormalIp.show()

            if (!AbnormalIp.rdd.isEmpty()) {
              //异常插入 数据集、异常等级、异常类型、异常状态
              DBUtil.insertIntoMysqlByJdbc(AbnormalIp, AbnormalIpLevel, MyConf.whiteList, MyConf.abnor_status)
            }

            //第三次过滤  白名单内的流量
            val normalIp = VisitFlow.filter($"ip".isin(listIp: _*))

            //第四次过滤 状态码和url进行匹配  匹配上为认证失败 输出
            if (!normalIp.rdd.isEmpty()) {
              val IdentificationFlow = normalIp.filter($"status_code".isin(listError: _*) && $"url".isin(listUrl: _*))
              print("发起认证的流量")
              IdentificationFlow.show()

              val identFail = SparkStreamingIdentification.IdentificationCheck(IdentificationFlow, conf, sqlc)
              //传入数据集、异常等级、异常类型、异常状态
              println("输出异常等级：" + failLevel)
              //            DBUtil.insertIndentificationFailIntoMysqlByJdbc(identFail)
              if (!identFail.rdd.isEmpty()) {
                DBUtil.insertAnorDataIntoMysqlByJdbc(identFail, failLevel, MyConf.landingFail, MyConf.abnor_status)
              }

              //第五次过滤 正常流量匹配
              val identSuccessFlow = normalIp.filter(!$"status_code".isin(listError: _*) && $"url".isin(listUrl: _*))
              val identSuccess = SparkStreamingIdentification.IdentificationSuccess(identSuccessFlow, conf, sqlc)

              //将用户名和IP映射关系存进redis
              DBUtil.insertUserIpIntoRedis(identSuccess)
              DBUtil.insertNnorDataIntoMysqlByJdbc(identSuccess, MyConf.nor_level, MyConf.nor, MyConf.nor_status, timeMap)
            }

            //登录成功 进入业务操作的流量
            val businessFlow = PortMatching.filter($"sysid" !== 1)
//            SparkstreamingBusiness.businessAnalysis(conf, sqlc, businessFlow)
            DBUtil.insertBusinessIntoMysql(businessFlow)
          }
        }
        for (o <- offsetRanges) {
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          //          println(s"${zkPath}_${o.untilOffset.toString}")
          ZkUtils(zkClient, false).updatePersistentPath(zkPath, o.untilOffset.toString)
        }
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
