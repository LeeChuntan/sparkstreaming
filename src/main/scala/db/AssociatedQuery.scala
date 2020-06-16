package db

import scala.collection.mutable.Map
import com.alibaba.fastjson.JSON
import conf.MyConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
  * 关联查询
  */
object AssociatedQuery {

  /**
    * 关联查询，获取异常时间配置
    * @param conf
    * @param sqlc
    * @return
    */
  def AssociatedQueryTime(conf: SparkConf, sqlc: SQLContext): Map[String,String] = {
  val WhiteTable = ReadTable.ReadTable(conf, MyConf.mysql_table_whitelist)
  WhiteTable.createOrReplaceTempView("t_analsmodel_json")

  val AnalsModel = ReadTable.ReadTable(conf,MyConf.mysql_table_model)
  AnalsModel.createOrReplaceTempView("t_analsmodel")

  val data = sqlc.sql("" +
    s"SELECT model.mleve as level, json.jsonstr as jsonstr FROM t_analsmodel model LEFT JOIN t_analsmodel_json json ON model.id = json.modeId WHERE model.id = 3 order by json.id desc limit 1 " +
    "")
  data.show()
  var stime: String = null
  var etime: String = null
  var norTimeLevel = 0

  var mapTime: Map[String,String] = Map()
  data.collect().foreach(o => {
    val jsonstr = o.getAs[String]("jsonstr")
    val data = JSON.parseArray(jsonstr)
    norTimeLevel= o.getAs[Int]("level")
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
}
