package BusinessAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 业务操作流量
  */
object SparkstreamingBusiness {

  def businessAnalysis(conf: SparkConf, sqlc: SQLContext, dataFrame: DataFrame): Unit = {

    println("测试正常业务操作的流量展示：")
    dataFrame.show()
    dataFrame.printSchema()

  }
}
