package db

import conf.MyConf
import com.mysql.jdbc.Driver
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object ReadTable {

  /**
    * 读表方法
    * @param conf
    * @return
    */
  def ReadTable(conf: SparkConf, tablename: String): DataFrame = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val table: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", MyConf.mysql_config("url"))
      .option("dbtable", tablename)
      .option("user",  MyConf.mysql_config("username"))
      .option("password", MyConf.mysql_config("password")).load()
     table
  }

  /**
    * 读取地区配置表
    * @param conf
    * @return
    */
  def ReadAreaTable(conf: SparkConf): DataFrame = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val areaTable: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", MyConf.mysql_config("url"))
      .option("dbtable", MyConf.mysql_table_area)
      .option("user",  MyConf.mysql_config("username")).option("password", "root@123").load()

    areaTable
  }

  /**
    * 读取状态匹配表
    * @param conf
    * @return
    */
  def ReadStatusTable(conf: SparkConf): DataFrame = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val statusTable: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", MyConf.mysql_config("url"))
      .option("dbtable", MyConf.mysql_table_status)
      .option("user",  MyConf.mysql_config("username"))
      .option("password", "root@123").load()

    statusTable
  }


  /**
    * 读取白名单
    * @param conf
    * @return
    */
  def ReadWhiteTableTable(conf: SparkConf): DataFrame = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val whiteTable: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", MyConf.mysql_config("url"))
      .option("dbtable", MyConf.mysql_table_whitelist)
      .option("user",  MyConf.mysql_config("username")).option("password", "root@123").load()

    whiteTable
  }
}
