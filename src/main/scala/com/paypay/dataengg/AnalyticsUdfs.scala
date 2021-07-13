package com.paypay.dataengg


import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

import scala.util.Try




object AnalyticsUdf {
  val PATTERN = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r

  case class LogLine(  timestamp: String,
                       loadbalancerName: String,
                       clientIp: String,
                       backendIp:String ,
                       rProcessingTime: String,
                       bProcessingTime: String,
                       responseTime:String,
                       lbStatusCode:String,
                       bkStatusCode:String,
                       bytesReceived:String ,
                       bytesSent:String,
                       operation:String,
                       userAgent:String,
                       sslCipher:String,
                       sslProtocol:String)


  def getEpochTime(format: String)(date: String): Long = {
    //val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    val dateFormat = new SimpleDateFormat(format)
    val dt = dateFormat.parse(date);
    val epoch = dt.getTime();
    (epoch)
  }

  //def addEpochTimeCol(format: String) = udf[Long, String](getEpochTime(format))

  def addEpochTimeCol(format : String) = udf((date:String) => {
    //val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    val dateFormat = new SimpleDateFormat(format)
    val dt = dateFormat.parse(date);
    val epoch = dt.getTime();
    (epoch)
  })





  def parseUDF = udf( (log : String) => {

    val PATTERN = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r

    try {
      val res = PATTERN.findFirstMatchIn(log)

      if (res.isEmpty) {
        LogLine("Empty", "-", "-", "", "",  "", "", "-", "-","","","","","","" )
      }
      else {
        val m = res.get
        LogLine(m.group(1),
          m.group(2),
          m.group(3),
          m.group(4),
          m.group(5),
          m.group(6),
          m.group(7),
          m.group(8),
          m.group(9),
          m.group(10),
          m.group(11),
          m.group(12),
          m.group(13),
          m.group(14),
          m.group(15))
//        Map("timestamp" ->  m.group(1),
//        "loadbalancerName" ->  m.group(2),
//        "clientIp" ->  m.group(3),
//        "backendIp" -> m.group(4) ,
//        "rProcessingTime" ->  m.group(5),
//        "bProcessingTime" ->  m.group(6),
//        "responseTime" -> m.group(7),
//        "lbStatusCode" -> m.group(8),
//        "bkStatusCode" -> m.group(9),
//        "bytesReceived" -> m.group(10) ,
//        "bytesSent" -> m.group(11),
//        "operation" -> m.group(12),
//        "userAgent" -> m.group(13),
//        "sslCipher" -> m.group(14),
//        "sslProtocol" -> m.group(15))
      }
    } catch {
      case e: Exception =>
//       Map(
//         "timestamp" ->  "",
//         "loadbalancerName" ->  "",
//         "clientIp" ->  "",
//         "backendIp" -> "" ,
//         "rProcessingTime" ->  "",
//         "bProcessingTime" ->  "",
//         "responseTime" -> "",
//         "lbStatusCode" -> "",
//         "bkStatusCode" -> "",
//         "bytesReceived" -> "" ,
//         "bytesSent" -> "",
//         "operation" -> "",
//         "userAgent" -> "",
//         "sslCipher" -> "",
//         "sslProtocol" -> ""
//       )
        LogLine(e.getMessage, "-", "-", "", "",  "", "", "-", "-","","","","","","" )
    }
  }
  )




  def transformLogLine(df: DataFrame): DataFrame = {
//    val spark = df.sparkSession
//    import spark.implicits._
//    val convertedDf = df.withColumn("property",parseUDF(col("value"))).drop("value")
//    val keysDF = convertedDf.select(explode(map_keys($"property"))).distinct()
//    val keys = keysDF.collect().map(f=>f.get(0))
//    val keyCols = keys.map(f=> col("property").getItem(f).as(f.toString))
//    convertedDf.select(keyCols:_*)
   df.withColumn("value", parseUDF(col("value"))).select("value.*")
  }

  def filterErrorLogs(df: DataFrame): DataFrame = {
    df.filter("backendIp != '-' AND rProcessingTime != -1")
  }

  def getErrorLines(df: DataFrame): DataFrame = {
    df.filter("backendIp = '-' OR rProcessingTime = -1")
  }

  def addEpochTime(format: String)(df: DataFrame): DataFrame = {
    //df.withColumn("epochTime",unix_timestamp(col("timestamp"),format))
    df.withColumn("epochTime", addEpochTimeCol(format)(col("timestamp")))
  }

  def webLog(cols: Seq[String])(df: DataFrame): DataFrame = {
    df.select(cols.head, cols.tail: _*)
  }

  def sortTransformer(colName: String)(df: DataFrame): DataFrame = {
    df.orderBy(asc(colName))
  }

  def sanitizeUrlAndClientIp(df: DataFrame): DataFrame = {
    df.withColumn("url", split(col("operation"), " ")(1))
      .withColumn("operation", split(col("operation"), " ")(0))
      .withColumn("clientPort", split(col("clientIp"), ":")(1))
      .withColumn("clientIp", split(col("clientIp"), ":")(0))

  }

  def sessionTransformer(orderedLogDf: DataFrame): DataFrame = {
    val duration : Long = Try(orderedLogDf.sparkSession.conf.get("session.duration").toLong).getOrElse(900000)
    val windowSpec = Window.partitionBy(col("clientIp")).orderBy("epochTime")

    val orderedLogsWithPrevNext = orderedLogDf
      .withColumn("prevEpochTime", lag("epochTime", 1).over(windowSpec))
      .withColumn("nextEpochTime", lead("epochTime", 1).over(windowSpec))


    val orderedLogsWithTimeGaps = orderedLogsWithPrevNext
      .select("timeStamp", "epochTime", "clientIP", "prevEpochTime", "nextEpochTime")
      .withColumn("inactiveSince", col("epochTime") - col("prevEpochTime"))
      .withColumn("inactiveUntil", col("nextEpochTime") - col("epochTime"))
      //.orderBy("timestamp").
      .na.fill(-1, Seq("inactiveSince")).na.fill(-1, Seq("inactiveUntil"))

    val sessionFirstAndLastActivity = orderedLogsWithTimeGaps
      .filter(col("inactiveSince").equalTo(-1) || col("inactiveSince") > duration ||
        col("inactiveUntil").equalTo(-1) || col("inactiveUntil") > duration)
    //.orderBy(asc("timestamp"))

    val allSessionsList = sessionFirstAndLastActivity
      .filter(col("inactiveSince") > duration || col("inactiveSince").equalTo(-1))
      .select("clientIp", "timestamp", "epochTime")
      .withColumn("lastActivity", lead("epochTime", 1).over(windowSpec))
      .withColumn("activityLength", col("lastActivity") - col("epochTime"))
      .withColumn("sessionLength", col("activityLength") + duration)
      .na.fill(duration, Seq("sessionLength"))

    val sessions = allSessionsList.orderBy("epochTime")
      .withColumn("sessionId", monotonically_increasing_id())
    sessions
      .withColumnRenamed("epochTime", "startTime")
      .withColumnRenamed("clientIp", "client")
      .select("sessionId", "client", "startTime", "sessionLength", "activityLength")


  }
}
