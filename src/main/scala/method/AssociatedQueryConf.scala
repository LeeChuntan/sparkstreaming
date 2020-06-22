package method

import com.alibaba.fastjson.JSON
import conf.MyConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.Map

object AssociatedQueryConf {

  var abnorTimeLevel: Int = _
  var AbnormalIpLevel: Int = _
  var failLevel: Int = _

  /**
    * 关联查询，获取异常时间配置
    * @param conf
    * @param sqlc
    * @return map
    */
  def getAbnormalTimeMap(conf: SparkConf, sqlc: SQLContext): Map[String,String] = {
  val WhiteTable = ReadTable.getTable(conf, MyConf.mysql_table_whitelist)
  WhiteTable.createOrReplaceTempView("t_analsmodel_json")

  val AnalsModel = ReadTable.getTable(conf,MyConf.mysql_table_model)
  AnalsModel.createOrReplaceTempView("t_analsmodel")

  val data = sqlc.sql("" +
    s"SELECT model.mleve as level, json.jsonstr as jsonstr FROM t_analsmodel model LEFT JOIN t_analsmodel_json json ON model.id = json.modeId WHERE model.id = 3 order by json.id desc limit 1 " +
    "")
  data.show()
  var stime: String = null
  var etime: String = null
//  var norTimeLevel = 0
  var mapTime: Map[String,String] = Map()
  data.collect().foreach(o => {
    val jsonstr = o.getAs[String]("jsonstr")
    val data = JSON.parseArray(jsonstr)
    abnorTimeLevel = o.getAs[Int]("level")
    val size: Int= data.size()
    for (i <- 0 to size-1) {
      val nObject = data.getJSONObject(i)
      stime = nObject.getString("starttime")
      etime = nObject.getString("endtime")
      mapTime += ("stime" -> stime)
      mapTime += ("etime" -> etime)
    }
  })
    mapTime
  }


  /**
    * 获取白名单IP
    * @param conf
    * @param sqlc
    * @return list
    */
  def getNormalIpList(conf: SparkConf, sqlc: SQLContext):List[String] = {

    val datawhite = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_white)
    val rddwhite = datawhite.rdd
    var mapIp: Map[String,Int] = Map()
//    var AbnormalIpLevel = 0
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
    listIp
  }


  /**
    * 获取认证url 及错误返回码
    * @param conf
    * @param sqlc
    * @return
    */
  def getIdentUrlConf(conf: SparkConf, sqlc: SQLContext): Map[String, List[String]] ={
    val dataPort = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_landing)
//    val rddPort = dataPort.rdd
    var mapUrlConf: Map[String, List[String]] = Map()
    var mapSeverIp: Map[String,Int] = Map()
    var mapPort: Map[String,Int] = Map()
    var mapError: Map[String,Int] = Map()
    var mapUrl: Map[String,Int] = Map()
//    var failLevel = 0
    dataPort.rdd.collect().foreach(row =>{
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      failLevel = level
      val size: Int= data.size()
      for (i <- 0 to size-1) {
        val nObject = data.getJSONObject(i)
        mapSeverIp += (nObject.getString("ip") -> level)
        mapPort += (nObject.getString("port")  -> level)
        mapError += (nObject.getString("error") -> level)
        mapUrl += (nObject.getString("url") -> level)
      }
    })
    mapUrlConf += ("url" -> mapUrl.map(_._1).toList)
    mapUrlConf += ("error" -> mapError.map(_._1).toList)
    mapUrlConf += ("serverIp" -> mapSeverIp.map(_._1).toList)
    mapUrlConf += ("port" -> mapPort.map(_._1).toList)

    mapUrlConf
  }

  /**
    * 获取服务系统的地址和端口
    * @param conf
    * @param sqlc
    * @param dataFrame
    */
  def getSystemIpPort(conf: SparkConf, sqlc: SQLContext, dataFrame: DataFrame): Map[String, List[String]] ={
    var mapPortIpList: Map[String, List[String]] = Map()
    var mapPortIp: Map[String,String] = Map()
    dataFrame.collect().foreach(row =>{
      val ip = row.getAs[String]("ip")
      val port = row.getAs[String]("port")
      mapPortIp += (port -> ip)
    })
    mapPortIpList += ("iplist" -> mapPortIp.map(_._2).toList.distinct)
    mapPortIpList += ("portlist" -> mapPortIp.map(_._1).toList.distinct)
    mapPortIpList
  }
}
