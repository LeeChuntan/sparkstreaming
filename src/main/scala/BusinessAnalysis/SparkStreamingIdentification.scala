package BusinessAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, _}


object SparkStreamingIdentification {

  /**
    * 认证模块 即状态码认证 失败
    * @param dataFrame
    * @param conf
    * @param sqlc
    * @return
    */
  def getIdentificationFail(dataFrame: DataFrame, conf: SparkConf, sqlc: SQLContext): DataFrame = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    dataFrame.createOrReplaceTempView("table_e")
    println("测试地方")
    dataFrame.show()
    val IdentificationFailCheck = sqlc.sql("select ip, requestTime, c_area, sysid, SUBSTRING_INDEX(e.`query`,'=',-1) as user from table_e e")
    println("验证失败流量提取用户名展示")
    IdentificationFailCheck.show()
    IdentificationFailCheck
  }

  /**
    * 成功
    * @param dataFrame
    * @param conf
    * @param sqlc
    */
  def getIdentificationSuccess(dataFrame: DataFrame, conf: SparkConf, sqlc: SQLContext): DataFrame = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    dataFrame.createOrReplaceTempView("table_f")
      //提出认证成功的IP的用户名
    val IdentificationSuccessCheck = sqlc.sql("select ip, requestTime, c_area, sysid, SUBSTRING_INDEX(f.`query`,'=',-1) as user from table_f f")
    println("验证成功的流量：测试输出用户名的长度")
    IdentificationSuccessCheck.show()
    IdentificationSuccessCheck

  }
}
