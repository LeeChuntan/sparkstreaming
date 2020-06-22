package method

import conf.MyConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object QueryAbnorLevel {

  /**
    * sql关联查询返回数据集  返回的是模型里定义的异常配置
    * @param conf
    * @param sqlc
    * @param int
    * @return dataframe
    */
  def getAbnormalConf(conf: SparkConf, sqlc:SQLContext, int: Int):DataFrame ={
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val WhiteTable = ReadTable.getTable(conf, MyConf.mysql_table_whitelist)
    WhiteTable.createOrReplaceTempView("t_analsmodel_json")

    val AnalsModel = ReadTable.getTable(conf,MyConf.mysql_table_model)
    AnalsModel.createOrReplaceTempView("t_analsmodel")

    //数据集
    val data = sqlc.sql("" +
      s"SELECT model.mleve as level, json.jsonstr as jsonstr FROM t_analsmodel model LEFT JOIN t_analsmodel_json json ON model.id = json.modeId WHERE model.id = ${int} order by json.id desc limit 1 " +
      "")
    data
  }
}
