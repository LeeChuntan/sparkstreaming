package sparkstreaming

import conf.MyConf
import util.DBUtil
import util.KafkaUtil
import java.lang

import BusinessAnalysis.{SparkStreamingIdentification, SparkstreamingBusiness}
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
import method.{AssociatedQueryConf, QueryAbnorLevel, ReadTable}
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

    //创建定时更新需要的对象
    var AreaTable: DataFrame = null
    var listIp: List[String] = null
    var mapUrlConf: Map[String, List[String]] = null
    var listUrl: List[String] = null
    var listError: List[String] = null
    var listServerIp: List[String] = null
    var listPort: List[String] = null
    var systemCode: DataFrame = null
    var sysPortIp: Map[String, List[String]] = null
    var allServerIp: List[String] = null
    var allServerport: List[String] = null
    var timeMap: Map[String, String] = null
    var failLevel: Int = 0
    var AbnormalIpLevel: Int = 0
    var AbnormalTimeLevel: Int = 0

    //最后更新时间
    var lastupdate: Long = 0L

    //隐式转换需要
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

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

        //判断时间
        if (System.currentTimeMillis() - lastupdate > MyConf.update_conf){
          println("定时执行")

          //加载地区表
          AreaTable = ReadTable.getTable(conf, MyConf.mysql_table_area)
          AreaTable.createOrReplaceTempView("area")

          //2.得到白名单关联表 解析出非名单内IP
          listIp = AssociatedQueryConf.getNormalIpList(conf, sqlc)
          //获取白名单异常等级
          AbnormalIpLevel = AssociatedQueryConf.AbnormalIpLevel

          //3.解析出url规则  错误状态返回码
          mapUrlConf = AssociatedQueryConf.getIdentUrlConf(conf, sqlc)
          //获取认证失败异常等级
          failLevel = AssociatedQueryConf.failLevel
          listUrl = mapUrlConf.get("url").get
          listError = mapUrlConf.get("error").get
          listServerIp = mapUrlConf.get("serverIp").get
          listPort =  mapUrlConf.get("port").get

          //4.提取所有服务系统端口 服务地址
          systemCode = ReadTable.getTable(conf,MyConf.mysql_table_sys)
          systemCode.createOrReplaceTempView("sys")
          sysPortIp = AssociatedQueryConf.getSystemIpPort(conf, sqlc, systemCode)
          allServerIp = sysPortIp.get("iplist").get
          allServerport = sysPortIp.get("portlist").get

          //5.获取异常时间模型
          timeMap = AssociatedQueryConf.getAbnormalTimeMap(conf, sqlc)
          AbnormalTimeLevel = AssociatedQueryConf.abnorTimeLevel
          lastupdate = System.currentTimeMillis()
        }

        //将dataframe识别的字段提取出来，观察是否存在需要字段
        val ColumnsList = df.columns.toList
        if (ColumnsList.contains("query") && ColumnsList.contains("http") && ColumnsList.contains("destination")) {
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

          if (!data.rdd.isEmpty()) {
            data.createOrReplaceTempView("flow")

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

              val identFail = SparkStreamingIdentification.getIdentificationFail(IdentificationFlow, conf, sqlc)
              //传入数据集、异常等级、异常类型、异常状态
              println("输出异常等级：" + failLevel)
              if (!identFail.rdd.isEmpty()) {
                DBUtil.insertAnorDataIntoMysqlByJdbc(identFail, failLevel, MyConf.landingFail, MyConf.abnor_status)
              }

              //第五次过滤 正常流量匹配
              val identSuccessFlow = normalIp.filter(!$"status_code".isin(listError: _*) && $"url".isin(listUrl: _*))
              val identSuccess = SparkStreamingIdentification.getIdentificationSuccess(identSuccessFlow, conf, sqlc)

              //将用户名和IP映射关系存进redis
              DBUtil.insertUserIpIntoRedis(identSuccess)
              DBUtil.insertNnorDataIntoMysqlByJdbc(identSuccess, AbnormalTimeLevel, MyConf.nor_level, MyConf.nor_type, MyConf.nor_status, timeMap)
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
