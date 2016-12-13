package com.ds365

import java.sql.{Connection, PreparedStatement}

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


/**
 * Created by Administrator on 2016/9/12 0012.
 * Verson 1.0
 * 实时统计分析用户搜索词
 */

case class Keyword(searchDate:String,userId:Int,keyowrd:String,searchCount:Int)

object UserKeyWord {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 本地测试用.setMaster("local[2]")开启两个线程
   * 上线发布删除此行代码
   * @param args
   */
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName(UserKeyWord.getClass.getSimpleName)
     .setMaster("local[2]")

    // val sc = new SparkContext(sparkConf)

    //开启StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(20))

    val messages =  kafkaInput(ssc)

    //解析每行日志，组成格式：<date,info>
    val searchInfo = messages.map(_._2).map(ele => {

      val searchDate = ele.split(" ")(0)
      val searchContent = ele.substring(ele.lastIndexOf("{"))
      (searchDate, searchContent)
    })

    //解析info,取得userId,keyword 映射成<date|userId|keyword,1>
    //进行同一时间，同一个用户，同一个搜索词的reduceBykey

    val userKeyword = searchInfo.map(ele => {
      val uid = 110
      val searchDate = ele._1
      val jsonbject = JSON.parseObject(ele._2)
      var userId = jsonbject.get("userId")
      var keyword = jsonbject.get("keyword")

      if (userId == null) {
        userId = uid.toString
      }

      if (keyword == null || "".equals(keyword.toString.trim) ){
        keyword = "None"
      }

      (searchDate + "_" + userId + "_" + keyword, 1)
    })

    //过滤掉搜索词为空的数据，并对每个用户同时间同搜索词的累加
    val keywordCount = userKeyword.filter(t => {
      val keyword = t._1.split("_")(2)

      if (keyword.equals("None")) false else true
    }).reduceByKey(_ + _)

    keywordCount.print()

    //映射成Keyword对象
    val keywordRDD = keywordCount.map(tuple => {
      val searchCount = tuple._2
      val infos = tuple._1.split("_")
      val searchDate = infos(0)
      val userId = infos(1).toInt
      val keyword = infos(2)
      Keyword(searchDate, userId, keyword, searchCount)
    })

    val sql = "insert into t_keyword(searchDate,userId,keyword,searchCount) values (?,?,?,?)"
    keywordRDD.foreachRDD { rdd =>
      //rdd.foreach(println)
      rdd.foreachPartition(partitionRecords => {
       /* val connection: Connection = Datasource.connectionPool.getConnection
        if (connection != null) {
          partitionRecords.foreach(record => process(connection, sql, record))
          ConnectionPool.closeConnection(connection)

        }*/

       Class.forName("com.mysql.jdbc.Driver");
        val  connection = ConnectionPool.getConnection.getOrElse(null)
        if (connection != null) {
          partitionRecords.foreach(record => process(connection, sql, record))
          ConnectionPool.closeConnection(connection)

        }
      })
    }
      ssc.start()
      ssc.awaitTermination()
    }


    def kafkaInput(ssc:StreamingContext): InputDStream[(String, String)] ={
      //创建kafka参数map
      val kafkaParams = Map[String, String]("metadata.broker.list" -> "ds3:9092,ds4:9092,ds5:9092")
      val topics = Array("u-keyword")
      val topicsSet = topics.toSet

      //创建Kafka输入流消息
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet)
      messages
    }


    def process(conn:Connection, sql: String, data: Keyword): Unit = {
      var ps: PreparedStatement = null
      try {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data.searchDate)
        ps.setInt(2, data.userId)
        ps.setString(3, data.keyowrd)
        ps.setInt(4, data.searchCount)
        ps.executeUpdate()
        logger.info(data +" insert success")
        println(data +" insert success")
      } catch {
        case exception: Exception =>
          logger.warn("Error in execution of query" + exception.printStackTrace())
      }finally {
        if(ps !=null){
          ps.close()
        }
      }
    }


    def jsonToRDD(json: String, ssc: StreamingContext): RDD[String] = {
      val contentJson = Array(json)
      val searchContentRDD: RDD[String] = ssc.sparkContext.parallelize(contentJson)
      searchContentRDD
    }
  }