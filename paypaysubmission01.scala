// Databricks notebook source

import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions.{asc, avg, col, desc, sum}
  import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

  import scala.util.{Failure, Success, Try}


// COMMAND ----------



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



// COMMAND ----------



val PATTERN = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r

  def parseLogLine(log: String): LogLine = {
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
      }
    } catch
    {
      case e: Exception =>
        LogLine(e.getMessage, "-", "-", "", "",  "", "", "-", "-","","","","","","" )
    }
  }


  def getEpochTime(format : String)(date : String) : Long = {
    //val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    val dateFormat = new SimpleDateFormat(format)
    val dt = dateFormat.parse(date);
    val epoch = dt.getTime();
    (epoch)
  }

  def addEpochTimeCol(format : String) = udf[Long,String](getEpochTime(format))

  val parseUDF = udf[LogLine,String](parseLogLine)



  
  def transformLogLine(df : DataFrame) : DataFrame = {
    df.withColumn("value",parseUDF(col("value"))).select("value.*")
  }

  def filterErrorLogs(df : DataFrame) : DataFrame = {
    df.filter("backendIp != '-' AND rProcessingTime != -1")
  }

  def getErrorLines(df : DataFrame) : DataFrame = {
    df.filter("backendIp = '-' OR rProcessingTime = -1")
  }

  def addEpochTime(format :String)(df : DataFrame) : DataFrame = {
    //df.withColumn("epochTime",unix_timestamp(col("timestamp"),format))
    df.withColumn("epochTime", addEpochTimeCol(format)(col("timestamp")))
  }

  def webLog(cols : Seq[String])(df : DataFrame) : DataFrame = {
    df.select(cols.head, cols.tail: _*)
  }

  def sortTransformer(colName : String)(df : DataFrame) : DataFrame = {
    df.orderBy(asc(colName))
  }

  def sanitizeUrlAndClientIp(df : DataFrame) : DataFrame = {
    df.withColumn("url",split(col("operation")," ")(1))
      .withColumn("operation",split(col("operation")," ")(0))
      .withColumn("clientPort",split(col("clientIp"),":")(1))
      .withColumn("clientIp",split(col("clientIp"),":")(0))

  }

  def sessionTransformer(orderedLogDf : DataFrame) : DataFrame = {
    val windowSpec = Window.partitionBy(col("clientIp")).orderBy("epochTime")

    val orderedLogsWithPrevNext = orderedLogDf
      .withColumn("prevEpochTime",lag("epochTime",1).over(windowSpec))
      .withColumn("nextEpochTime" , lead("epochTime" , 1).over(windowSpec))


    val orderedLogsWithTimeGaps = orderedLogsWithPrevNext
      .select("timeStamp","epochTime" ,"clientIP", "prevEpochTime", "nextEpochTime")
      .withColumn("inactiveSince" , col("epochTime") - col("prevEpochTime"))
      .withColumn("inactiveUntil" , col("nextEpochTime") - col("epochTime"))
      //.orderBy("timestamp").
      .na.fill(-1,Seq("inactiveSince")).na.fill(-1,Seq("inactiveUntil"))

    val sessionFirstAndLastActivity = orderedLogsWithTimeGaps
      .filter(col("inactiveSince").equalTo(-1) || col("inactiveSince") > (900 * 1000) ||
        col("inactiveUntil").equalTo(-1) || col("inactiveUntil") > (900 * 1000))
    //.orderBy(asc("timestamp"))

    val allSessionsList = sessionFirstAndLastActivity
      .filter(col("inactiveSince") > (900 * 1000) || col("inactiveSince").equalTo(-1))
      .select( "clientIp" ,"timestamp","epochTime")
      .withColumn( "lastActivity" ,lead("epochTime" , 1).over(windowSpec))
      .withColumn("activityLength" , col("lastActivity") - col("epochTime"))
      .withColumn("sessionLength" , col("activityLength") + (900 * 1000))
      .na.fill((900 * 1000), Seq("sessionLength"))

    val sessions = allSessionsList.orderBy("epochTime")
      .withColumn("sessionId", monotonically_increasing_id())
    sessions
      .withColumnRenamed("epochTime","startTime")
      .withColumnRenamed("clientIp","client")
      .select("sessionId","client","startTime","sessionLength","activityLength")

  }



// COMMAND ----------




    val colNames = Seq("timestamp","clientIp","operation","userAgent","epochTime")
    val dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

    def writeResults(df: DataFrame, path: String, fileName : String, returnDf : Boolean): DataFrame = {
//      df.coalesce(1)
//        .write
//        .option("header", "true")
//        .mode(SaveMode.Overwrite)
//      .csv(path.concat(".csv"))
//        df.show(20,false)
      df.write.saveAsTable(fileName)
      if (returnDf)
        df
      else
        null
    }

    def writeResults(df: DataFrame, path: String, fileName : String): Unit = {
//      df.coalesce(1)
//        .write
//        .option("header", "true")
//        .mode(SaveMode.Overwrite)
//        .csv(path.concat(".csv"))
      df.write.saveAsTable(fileName)
    }

    def readFile(spark : SparkSession , filePath : String) : DataFrame = {
      val rawDf = spark.read.text(filePath)
      rawDf
    }

    def runTransformations(rawDf : DataFrame ) : (DataFrame,DataFrame) = {
      val parsedLogLines= rawDf.transform(transformLogLine)
        .transform(filterErrorLogs)
        .transform(addEpochTime(dateFormat))
        .transform(webLog(colNames))
        .transform(sanitizeUrlAndClientIp)
      val orderedLogDf = parsedLogLines.transform(sortTransformer("epochTime"))
      val sessionIdDf = orderedLogDf.transform(sessionTransformer)
      (orderedLogDf,sessionIdDf)
    }

    def orchestrate( inputPath : String, outPath : String, config : Map[String,Any]) = {
      val rawDf = readFile(spark,inputPath)
      val (orderedLogDf,sessionIdDf) = runTransformations(rawDf)
      analyticsPipelineOne(orderedLogDf,sessionIdDf).seq.foreach{
        case(taskOp, taskName, triggerPipeline) => {
          if (triggerPipeline) {
            Try(writeResults(taskOp,outPath,taskName,true)) match {
              case Success(value) => {
                //trigger second pipeline here
                analyticsPipelineTwo(value).seq.foreach{
                  case(taskOp,taskName) => {
                    Try(writeResults(taskOp,outPath,taskName)) match {
                      case Success(value) => {

                      }
                      case Failure(exception) => {

                      }
                    }
                  }
                }
              } case Failure(exception) => {

              }
            }
          } else {
            Try(writeResults(taskOp,outPath,taskName)) match {
              case Success(value) => {

              }
              case Failure(exception) => {

              }
            }
          }
        }

      }
    }


  


    val sessioniseLogs : (DataFrame,DataFrame) => DataFrame  = (orderedLogDf, sessionIdDf )  =>  {
      val logsSessionized = orderedLogDf.join(sessionIdDf ,
        orderedLogDf("clientIp") === sessionIdDf("client") &&
          (orderedLogDf("epochTime") === sessionIdDf("startTime") ||
            (orderedLogDf("epochTime") > sessionIdDf("startTime") && orderedLogDf("epochTime") - sessionIdDf("startTime") <= sessionIdDf("activityLength"))))
      logsSessionized
    }

    val averageSessionTime : DataFrame => DataFrame = sessionIdDf => {
      sessionIdDf.select(avg("sessionLength").alias("avgSessionTime"))
    }

    val mostEngagedUsers : DataFrame => DataFrame = sessionIdDf => {
      val mostEngagedUsers = sessionIdDf.groupBy("client").
        agg( sum("sessionLength").alias("totalSessionLength_ms") ,
          avg("sessionLength").alias("avgSessionLength_ms")).
        orderBy(desc("totalSessionLength_ms"))
      mostEngagedUsers
    }

    val uniqueUrlVisit : DataFrame => DataFrame = logsSessionized => {
      val uniqueUrlVisit = logsSessionized.groupBy("sessionId" , "url").count().alias("count").filter(col("count").equalTo(1))
        .select("sessionId" , "url")
      uniqueUrlVisit
    }

    val analyticsPipelineOne : (DataFrame,DataFrame) => List[(DataFrame,String,Boolean)] = (orderedLogDf, sessionIdDf) => {
      List(
        (sessioniseLogs(orderedLogDf,sessionIdDf),"logsessions",true),
        (averageSessionTime(sessionIdDf),"averagesessiontime",false),
        (mostEngagedUsers(sessionIdDf),"mostengagedusers",false)
      )
    }


    val analyticsPipelineTwo : DataFrame => List[(DataFrame,String)] = logsSessionized => {
      List(
        (uniqueUrlVisit(logsSessionized),"uniqueurlvisit")
      )
    }




// COMMAND ----------


val filePath = "/FileStore/tables/2015_07_22_mktplace_shop_web_log_sample_log-1.gz"
val outPath ="/FileStore/tables/"
orchestrate(filePath,outPath,Map("duration" -> 900000))

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from `logsessions`

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from `averagesessiontime`

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from `mostengagedusers` order by avgSessionLength_ms desc

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from `uniqueurlvisit`
