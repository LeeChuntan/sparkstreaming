package util

import conf.MyConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
object MySqlQuery {

  /**
    * sql关联查询返回数据集  返回的是模型里定义的异常配置
    * @param conf
    * @param sc
    * @param int
    * @return
    */
  def SqlQuery(conf: SparkConf, sc: SparkContext, int: Int):DataFrame ={
    val sqlc = new SQLContext(sc)
    //隐式转换需要
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val WhiteTable = ReadTable.ReadTable(conf, MyConf.mysql_table_whitelist)
    WhiteTable.createOrReplaceTempView("t_analsmodel_json")

    val AnalsModel = ReadTable.ReadTable(conf,MyConf.mysql_table_model)
    AnalsModel.createOrReplaceTempView("t_analsmodel")

    //数据集
    val data = sqlc.sql("" +
      s"SELECT model.mleve as level, json.jsonstr as jsonstr FROM t_analsmodel model LEFT JOIN t_analsmodel_json json ON model.id = json.modeId WHERE model.id = ${int} order by json.id desc limit 1 " +
      "")
    data
  }

  /**
    * 查询最新IP和用户名的匹配关系
    * @param conf
    * @param sc
    * @return
    */
  def SqlQueryUserIp(conf: SparkConf, sc: SparkContext): DataFrame = {
    val sqlc = new SQLContext(sc)
    //隐式转换需要
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val usernameIp = ReadTable.ReadTable(conf, MyConf.mysql_table_usernameIp)
    usernameIp.createOrReplaceTempView("user")
    val newTable = sqlc.sql("" +
      "select * from user where pid in (select max(pid) from user group by ip)" +
      "")
    newTable
  }
}
