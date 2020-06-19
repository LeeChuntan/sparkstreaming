package dao

import bean.AnalsModelEnum
import com.alibaba.fastjson.JSON
import scalikejdbc._

import scala.collection.mutable.ListBuffer

object AnalsmodelDao {

  private var white: ListBuffer[String] = ListBuffer[String]();

  /**
    * 单条记录查询
    * @param id
    * @param s
    * @return
    */
  def findById(id: Long)(implicit s: DBSession = AutoSession): Option[Map[String, Any]] = {
    sql"SELECT model.mleve as level, json.jsonstr as jsonstr FROM t_analsmodel model LEFT JOIN t_analsmodel_json json ON model.id = json.modeId WHERE model.id = ${id} order by json.id desc limit 1 "
      .map { _.toMap()}.single.apply()
  }

  def loadAll(): Unit = {
     loadWhieList()
  }

  def loadWhieList() = {

    var listIp = ListBuffer[String]();

    val dataMap = findById(AnalsModelEnum.WHITE_LIST.value())
    val level = dataMap.get.get("level")
    val jsonStr = dataMap.get.get("jsonstr").getOrElse().toString
    print(jsonStr)

    val data = JSON.parseArray(jsonStr)

    val size: Int= data.size()
    for (i <- 0 to size-1) {
      val nObject = data.getJSONObject(i)
      val str = nObject.getString("ip")
      listIp += str;
    }

    white = listIp
  }

  def getWhieList():List[String] = {
    white.toList;
  }
}

