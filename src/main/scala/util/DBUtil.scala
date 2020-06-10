package util

import java.sql.{ResultSet, Statement, Connection => jsc}

import com.alibaba.fastjson.JSONObject
import com.sun.glass.ui.Window.Level
import db.DBredis
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis
import scala.collection.mutable.Map

object DBUtil {

  /**
    * 异常数据插入MySQL
    * @param dataFrame
    * @param level        异常等级
    * @param typeid       异常类型
    * @param status       异常状态
    * */
  def insertAnorDataIntoMysqlByJdbc(dataFrame: DataFrame, level: Int, typeid: String, status: Int): Unit = {
    dataFrame.foreachPartition(partitionsOfRecords => {
      var connection: jsc = null
      var statement: Statement = null
      val redis: Jedis = DBredis.getConnections()

      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        partitionsOfRecords.foreach(fields => {

          val time =System.currentTimeMillis()
          val pid = redis.incr("bmap_result_id")

          println("测试获取到的pid：" + pid)

          val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
            "values(" + pid + ",'" + fields(0) + "','" + fields(4) + "','" + fields(3) + "','" + fields(1) + "','" + fields(2) + "','" + typeid + "','" + level + "','" + status + "','" +time+ "','" + status + "')"
         //批量插入运行快
//          statement.addBatch()
          statement.execute(sql)
          //statement.executeBatch()
          connection.commit()

          println("执行到这里了吗")
          //登录失败
          if (typeid == "3"){
          val jsonObj = new JSONObject()
          jsonObj.put("pid", pid)
          jsonObj.put("username", fields(3))
          jsonObj.put("level", level)
          redis.publish("bmap_alarm_channel", jsonObj.toString)}
/*          else{
//            redis.set("ip_" + fields(0), fields(4).toString)
          }*/
        })
        redis.close()
      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }




/*  def insertNorDataIntoMysqlByJdbc(dataFrame: DataFrame,level: Int, typeid: String): Unit = {

    dataFrame.foreachPartition(partitionsOfRecords => {

      //      var map: Map[String,String] = Map()
      var connection: jsc = null
      var statement: Statement = null
      var rs: ResultSet = null

      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()

        val redis: Jedis = DBredis.getConnections()

        partitionsOfRecords.foreach(fields => {
          val time =System.currentTimeMillis()
          val pid = redis.incr("bmap_result_id")
          val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
            "values(" + pid + ",'" + fields(0) + "','" + fields(4) + "','" + fields(3) + "','" + fields(1) + "','" + fields(2) + "','" + typeid + "','" + level + "','" + "1" + "','" +time+ "','" + "1" + "')"

          statement.execute(sql)
          connection.commit()
        })
      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }*/




  /**
    * 白名单异常IP插入MySQL
    * @param dataFrame
    */

  def insertIntoMysqlByJdbc(dataFrame: DataFrame, level: Int, typeid: String, status: Int): Unit = {

    println("白名单插入方法接受到的流量展示")
    dataFrame.show()

    dataFrame.foreachPartition(partitionsOfRecords => {
      var connection: jsc = null
      var statement: Statement = null

      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        val redis: Jedis = DBredis.getConnections()

        partitionsOfRecords.foreach(fields => {
          val pid = redis.incr("bmap_result_id")
          val time =System.currentTimeMillis()
          println("白名单外遍历输出测试")
          val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
                      "values(" + pid + ",'" + fields(0) + "','" + " " + "','" + fields(2) + "','" + fields(4) + "','" + fields(8) + "','" + typeid + "','" + level + "','" + status + "','" +time+ "','" + status + "')"

          /*statement.addBatch(sql)
          statement.executeBatch()*/
          statement.execute(sql)
          //statement.executeBatch()
          connection.commit()
        })
        redis.close()
        connection.commit()

      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }


  def insertUserIpIntoMysqlByJdbc(dataFrame: DataFrame): Unit = {

    dataFrame.foreachPartition(partitionsOfRecords => {

      var connection: jsc = null
      var statement: Statement = null

      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        partitionsOfRecords.foreach(fields => {
          val time =System.currentTimeMillis()
          val sql = "insert into t_usernameIp(ip, username, update_time) " +
            "values('" + fields(0) + "','" + fields(1) + "','" + time + "')"

          statement.addBatch(sql)
          statement.executeBatch()
        })
        connection.commit()

      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }




  /**
    * 正常IP插入MySQL
    */
 /* def insertNormalIpIntoMysqlByJdbc(dataFrame: DataFrame): Unit = {

    dataFrame.foreachPartition(partitionsOfRecords => {

      var connection: jsc = null
      var statement: Statement = null

      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        val redis: Jedis = DBredis.getConnections()
        partitionsOfRecords.foreach(fields => {
          val pid = redis.incr("bmap_result_id")
          val time =System.currentTimeMillis()
          val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
            "values(" + pid + ",'" + fields(0) + "','" + fields(3) + "','" + "1" + "','" + fields(1) + "','" + fields(2) + "','" + "0" + "','" + "0" + "','" + "0" + "','" +time+ "','" + "0" + "')"

          statement.addBatch(sql)
          statement.executeBatch()
        })
        connection.commit()

      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }*/


  /**
    * 将认证失败的IP插入MySQL
    */
  /*def insertIndentificationFailIntoMysqlByJdbc(dataFrame: DataFrame): Long = {

    var timeSpot = System.currentTimeMillis()

    dataFrame.foreachPartition(partitionsOfRecords => {

//      var map: Map[String,String] = Map()
      var connection: jsc = null
      var statement: Statement = null
      var rs: ResultSet = null

      /**
        * 上面两种方法错误 截取当前时间戳 在插入redis的时候进行时间戳比较 将大于当前时间戳的值发送定于消息
        */
      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()

        val redis: Jedis = DBredis.getConnections()

        partitionsOfRecords.foreach(fields => {
          val time =System.currentTimeMillis()
          val pid = redis.incr("bmap_result_id")
          val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
            "values(" + pid + ",'" + fields(0) + "','" + fields(3) + "','" + "1" + "','" + fields(1) + "','" + fields(2) + "','" + "3" + "','" + "2" + "','" + "1" + "','" +time+ "','" + "1" + "')"
          statement.execute(sql)
          //statement.executeBatch()
          connection.commit()
          val jsonObj = new JSONObject()
          jsonObj.put("pid", pid)
          jsonObj.put("username", fields(3))
          jsonObj.put("level", 1)
          redis.publish("bmap_alarm_channel", jsonObj.toString)

        })


      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })

    println("输出时间戳：" + timeSpot)
    timeSpot
  }*/

  /**
    * 将认证成功的用户插入MySQL
    * @param dataFrame
    */
 /* def insertIndentificationSuccessIntoMysqlByJdbc(dataFrame: DataFrame): Unit = {

    dataFrame.foreachPartition(partitionsOfRecords => {

      var connection: jsc = null
      var statement: Statement = null

      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        val redis: Jedis = DBredis.getConnections()

        partitionsOfRecords.foreach(fields => {
          val time =System.currentTimeMillis()
          val pid = redis.incr("bmap_result_id")
          val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
            "values(" + pid + ",'" + fields(0) + "','" + fields(3) + "','" + "1" + "','" + fields(1) + "','" + fields(2) + "','" + "0" + "','" + "0" + "','" + "0" + "','" +time+ "','" + "0" + "')"

          statement.addBatch(sql)
          statement.executeBatch()
        })
        connection.commit()

      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }*/

/*
  def insertIntoRedisByJdbc(conf: SparkConf, dataFrame: DataFrame, long: Long): Unit = {

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val filter = dataFrame.where($"status" === "1" && $"update_time" > long ).filter($"user" !== "null")
    filter.foreachPartition(part =>{
    val redis: Jedis = DBredis.getConnections()
     part.foreach(row =>{
     val a = row.getAs[Any]("pid")
     val b = row.getAs[Any]("user")
     val c = row.getAs[Any]("level")

       //SUBSCRIBE bmap_alarm_channel
//     val map = Map[String, Any]("pid" -> a, "username" -> b, "level" -> c)
     //val d = JSON.toJSONString(map)
    // redis.publish("bmap_alarm_channel",d)

       val jsonObj = new JSONObject()
       jsonObj.put("pid", a)
       jsonObj.put("username", b)
       jsonObj.put("level", c)
       redis.publish("bmap_alarm_channel", jsonObj.toString)
     })
     redis.close()
   })
  }
*/

  def insertUserIpIntoRedis(dataFrame: DataFrame): Unit = {
    dataFrame.foreachPartition(part =>{
      val redis: Jedis = DBredis.getConnections()
      part.foreach(line =>{
        redis.set("ip_" + line(0),line(4).toString)
      })
      redis.close()
    })
  }
}