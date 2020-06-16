package conf

object MyConf {

  //kafka配置文件
  val kafka_topic: String = "sparkstreaming"

  val kafka_group: String = "packetbeat_test"

  val kafka_brokers: String = "s1:9092"

  val zookeeper: String = "s1:2181"

 //kafka的offset读取位置
  final val kafka_offset_position: String = "earliest"

  // mysql 配置
//  final val mysql_config: Map[String, String] = Map("url" -> "jdbc:mysql://localhost/test", "username" -> "root", "password" -> "")
  final val mysql_config: Map[String, String] = Map("url" -> "jdbc:mysql://10.107.42.150:3306/bmap?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useSSL=false", "username" -> "root", "password" -> "123456")

  final val mysql_table_whitelist: String = "t_analsmodel_json"

  final val mysql_table_status: String  = "status_list"

  final val mysql_table_area: String = "t_device"

  final val ResultTable: String  = "t_result"

  final val mysql_table_model: String = "t_analsmodel"

  final val mysql_table_sys: String = "t_monitor_info"

  final val mysql_table_usernameIp: String = "t_usernameIp"

  //异常状态配置 '类型1-异常ip认证2-异常时间认证3-认证失败4-越权访问5-访问超时',
  final val abnor_status: Int = 1             //异常状态值
  final val nor_status: Int = 0               //正常状态值
  final val nor: String = "0"                 //正常类型
  final val whiteList: String = "1"           //白名单异常类型
  final val abnor_Time: String = "2"          //异常时间段异常类型
  final val landingFail: String = "3"         //认证失败异常类型
  final val nor_level: Int = 0                //正常情况下异常等级

  //读取异常等级和规则配置分类设置
 final val modeld_white: Int = 1              //白名单异常模型编号
  final val modeld_time: Int = 3              //异常时间点异常模型编号
  final val modeld_landing: Int =4            //认证url配置模型编号


  //ES的host
  final val es_host:String = "s1"

  //ES的端口
  final val es_poot:String = "9200"

  //ES的index和type
  final val es_index_type:String = "sparkstreaming/lee"

  final val update_conf: Long = 10000L

  //redis配置
  final val REDIS_CONFIG: Map[String, String] = Map("host" -> "s3", "port" -> "6379", "timeout" -> "10000", "passwd" -> "123456")
}
