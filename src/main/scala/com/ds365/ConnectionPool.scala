package com.ds365

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import org.apache.commons.dbcp2.BasicDataSource
import org.slf4j.LoggerFactory


case class Record(vtime:Long,muid:String,uid:String,item:String,types:String)
/**
 * Created by Administrator on 2016/9/12 0012.
 */

object ConnectionPool {

  val logger = LoggerFactory.getLogger(this.getClass)
  val prop = new Properties()
  prop.load(ConnectionPool.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))
  private val connectionPool = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
     val bs = new BasicDataSource()
      bs.setDriverClassName(prop.getProperty(Contants.DRIVER_NAME))
      bs.setUrl(prop.getProperty(Contants.DRIVER_JDBC_URL))
      bs.setUsername(prop.getProperty(Contants.USER_NAME))
      bs.setPassword(prop.getProperty(Contants.PASSWORD))
      bs.setInitialSize(10)
      Some(bs)
    } catch {
      case exception: Exception =>
        logger.warn("Error in creation of connection pool" + exception.printStackTrace())
        None
    }
  }
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }

  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) connection.close()
  }

  def process(conn:Connection,sql:String,data:Record): Unit ={
    try{
      val ps : PreparedStatement = conn.prepareStatement(sql)
      ps.setLong(1,data.vtime)
      ps.setString(2,data.muid)
      ps.setString(3,data.uid)
      ps.setString(4,data.item)
      ps.setString(5,data.types)
      ps.executeUpdate()
    }catch{
      case exception:Exception=>
        logger.warn("Error in execution of query"+exception.printStackTrace())
    }
  }

}