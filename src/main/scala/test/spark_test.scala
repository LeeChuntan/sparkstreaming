package test

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.mysql.jdbc.Driver
import conf.MyConf
import db.DBredis
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{SparkConf, SparkContext}
import org.datanucleus.store.rdbms.sql.SQLText
import redis.clients.jedis.Jedis
import com.github.nscala_time.time.Imports._
import scala.collection.mutable.Map

object spark_test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[*]")
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    import spark.implicits._
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

  /*  val table: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", MyConf.mysql_config("url"))
      .option("dbtable", MyConf.mysql_table_sys)
      .option("user",  MyConf.mysql_config("username"))
      .option("password", MyConf.mysql_config("password")).load()
    table.show()

    var mapIpPort: Map[String, String] = Map()
    table.collect().foreach(row =>{
      val ip = row.getAs[String]("ip")
      val port = row.getAs[String]("port")
      mapIpPort += (port  -> ip)
    })
    println(mapIpPort)
    val allServerIp = mapIpPort.map(_._2).toSet
    val allServerport = mapIpPort.map(_._1).toSet
    allServerIp.foreach(o => println(o))
    allServerport.foreach(o => println(o))*/


  /*  val WhiteTable = ReadTable.ReadTable(conf, MyConf.mysql_table_whitelist)
    WhiteTable.createOrReplaceTempView("t_analsmodel_json")

    val AnalsModel = ReadTable.ReadTable(conf,MyConf.mysql_table_model)
    AnalsModel.createOrReplaceTempView("t_analsmodel")

    val data = sqlc.sql("" +
      s"SELECT model.mleve as level, json.jsonstr as jsonstr FROM t_analsmodel model LEFT JOIN t_analsmodel_json json ON model.id = json.modeId WHERE model.id = 3 order by json.id desc limit 1 " +
      "")


    data.show()
    var stime: String = null
    var etime: String = null
    data.collect().foreach(o => {
      val jsonstr = o.getAs[String]("jsonstr")
      val data = JSON.parseArray(jsonstr)
      val level = o.getAs[Int]("level")
//      AbnormalIpLevel = level
      val size: Int= data.size()
      for (i <- 0 to size-1) {
        val nObject = data.getJSONObject(i)
         stime = nObject.getString("starttime")
         etime = nObject.getString("endtime")
      }
    })

    println(stime)
    println(etime)*/

  /*  val a: String = "2020-06-11 23:35:00"


    val b = tranTimeToHour(a)
    println(b)

    if (stime<b && etime>b){
      println("b在区间里")
    }else{
      println("b不在区间里")
    }*/

    val map = AssociatedQuery.AssociatedQueryTime(conf, sqlc)
//    map.map(_._1).foreach(o => )
//    map.map(_._2).foreach(o => println(o))
    val a = map.get("stime").get
    val b = map.get("etime").get
    println(a)
    println(b)

  }

//    val redis: Jedis = DBredis.getConnections()
//    println(redis.get("bmap_result_id"))

//    val b = tranTimeToLong(a)
//
//    println(b)

  def tranTimeToLong(time:String) :Long={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = format.parse(time)
    val timestamp: Long = dt.getTime()
    timestamp
  }

  def tranTimeToHour(time:String): String={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = format.parse(time)

    val format1 = new SimpleDateFormat("HH:mm:ss")
//    val timestamp: Long = dt.getTime()
    val hour = format1.format(dt)
    hour
  }
}
