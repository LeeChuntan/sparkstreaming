package sparkstreaming




import conf.MyConf
import util.DBUtil
import util.KafkaUtil
import java.lang

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

//      .setMaster("local[*]")

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
      */
    val AreaTable = ReadTable.ReadTable(conf, MyConf.mysql_table_area)
    AreaTable.cache()
    AreaTable.createOrReplaceTempView("area")

    //得到白名单关联表 解析出非名单内IP
    val datawhite = MySqlQuery.SqlQuery(conf,sc,1)
    val rddwhite = datawhite.rdd
    var mapIp: Map[String,Int] = Map()
    var AbnormalIpLevel = 0
    rddwhite.collect().foreach(row =>{
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      AbnormalIpLevel = level
      val size: Int= data.size()
      for (i <- 0 to size-1) {
        val nObject = data.getJSONObject(i)
        val str = nObject.getString("ip")
        mapIp += (str  -> level)
      }
    })
    val listIp =  mapIp.map(_._1).toList
    //提出非白名单异常等级
//    val AbnormalIpLevel = mapIp.map(_._2)
    println(mapIp)
    //将IP列表作为广播变量
//    val broadcast = spark.sparkContext.broadcast(list)


    //得到白名单关联表   解析出url规则  错误状态返回码
    val dataPort = MySqlQuery.SqlQuery(conf,sc,4)
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

    //测试
    listUrl.foreach(a => println(a))

    val listError = mapError.map(_._1).toList
    //测试
    listError.foreach(a => println(a))

    val listServerIp = mapSeverIp.map(_._1).toList
    val listPort =  mapPort.map(_._1).toList

    val identificationFailLevel = mapUrl.map(_._2)

    val systemCode = ReadTable.ReadTable(conf,MyConf.mysql_table_sys)
    systemCode.createOrReplaceTempView("sys")

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

        val ColumnsList = df.columns.toList
        ColumnsList.foreach(a => print(a + ","))
//        val fieldList: List[String] = List("destination", "http", "query")
        /**
          * 接收数据准备  该流量是否存在destination,query,http
          * Q：集合中存在字段query 在解析中也会出现错误
          */
        if (ColumnsList.contains("query") && ColumnsList.contains("http") && ColumnsList.exists(word => word == "destination")) {
          val dfData = df.filter($"destination.port".isin(listPort: _*) && $"destination.ip".isin(listServerIp: _*))
          val data = dfData.select($"source.ip" as "ip",
            $"destination.ip" as "serviceIp",
            $"destination.port" as "port",
            $"@timestamp" as "requestTime",  //应该改为startTime
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

          //第一次过滤  判断是否访问服务的流量
          if (!data.rdd.isEmpty()) {
            val VisitFlow = data.filter($"port".isin(listPort: _*) && $"serviceIp".isin(listServerIp: _*) && $"status_code".isNotNull)

            //时间格式的转换
            println("时间格式转换输出")
            val a = VisitFlow.select($"requestTime".cast(DateType))
            a.show()
            a.printSchema()



            VisitFlow.createOrReplaceTempView("flow")
            println("访问服务元素输出")
            VisitFlow.show()
            //            }

            //将访问服务的流量与地址信息进行匹配
            val AreaIpMatching = sqlc.sql(
              "SELECT w.ip, serviceIp, port, requestTime, url, query, status_code, c_area from flow w LEFT JOIN area a ON w.ip = a.c_ip"
            )
            AreaIpMatching.createOrReplaceTempView("mat")
            println("地址匹配上")
            AreaIpMatching.show()

            //将端口进行绑定 匹配
            val PortMatching = sqlc.sql(
              "SELECT m.ip, serviceIp, id as sysid, m.port, requestTime, url, query, status_code, c_area from mat m LEFT JOIN sys s ON m.port = s.port"
            )
            println("系统匹配上")
            PortMatching.show()

            //第二次过滤 查看流量是否在白名单内 不在定义为异常输出
            val AbnormalIp = PortMatching.filter(!$"ip".isin(listIp: _*))
            println("白名单外流量输出")
            AbnormalIp.show()
            AbnormalIp.printSchema()

            if (!AbnormalIp.rdd.isEmpty()) {
              //异常插入 数据集、异常等级、异常类型、异常状态
              DBUtil.insertIntoMysqlByJdbc(AbnormalIp, AbnormalIpLevel, "1", 1)
            }

            //第三次过滤  白名单内的流量
            val normalIp = PortMatching.filter($"ip".isin(listIp: _*))

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
                DBUtil.insertAnorDataIntoMysqlByJdbc(identFail, failLevel, "3", 1)
              }

              //第五次过滤 正常流量匹配
              val identSuccessFlow = normalIp.filter(!$"status_code".isin(listError: _*) && $"url".isin(listUrl: _*))
              val identSuccess = SparkStreamingIdentification.IdentificationSuccess(identSuccessFlow, conf, sqlc)
//              identSuccess.createOrReplaceTempView("Nor")
              //将提取的用户名和IP建立映射关系
//              val userIp = sqlc.sql("select distinct ip, user from Nor where length(user) < 8 ")
//              userIp.show()
              //将用户和IP的映射表存进MySQL
//              DBUtil.insertUserIpIntoMysqlByJdbc(userIp)
              //将用户和IP映射关系存进redis
              DBUtil.insertUserIpIntoRedis(identSuccess)
              //获取用户名和IP映射关系
//              val usernameIp = MySqlQuery.SqlQueryUserIp(conf, sc)
//              usernameIp.createOrReplaceTempView("user")
              //对IP进行匹配
//              val NorIp = sqlc.sql("select n.ip, requestTime, c_area, sysid, username, user from Nor n left join user u on n.ip = u.ip ")
              //将映射数据持久化到内存
              //userIp.cache()
              //插入数据集、异常等级、异常类型、异常状态
//              println("展示用户名和IP匹配后的结果")
//              NorIp.show()
              DBUtil.insertAnorDataIntoMysqlByJdbc(identSuccess, 0, "0", 0)
            }
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
