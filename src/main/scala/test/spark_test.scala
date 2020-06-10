package test

import com.mysql.jdbc.Driver
import conf.MyConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{SparkConf, SparkContext}
import org.datanucleus.store.rdbms.sql.SQLText


object spark_test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("shy").setMaster("local[*]")
    conf.set("spark.sql.shuffle.partitions","1")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val table: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", "jdbc:mysql://localhost/test")
      .option("dbtable", "test1")
      .option("user", "root").load()

    table.show()




  }
}
