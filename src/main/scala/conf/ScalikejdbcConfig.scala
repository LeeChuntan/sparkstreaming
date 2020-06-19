package conf

import scalikejdbc.ConnectionPool

object ScalikejdbcConfig {

  /**
    * 配置数据库连接池
    */
 def config() = {
   Class.forName("com.mysql.cj.jdbc.Driver")
   ConnectionPool.singleton("jdbc:mysql://10.107.42.150:3306/bmap?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useSSL=false", "root", "123456")
 }
}
