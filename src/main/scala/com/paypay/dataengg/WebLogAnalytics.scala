  package com.paypay.dataengg

  import com.paypay.dataengg.AnalyticsUdf.{addEpochTime, filterErrorLogs, sanitizeUrlAndClientIp, sessionTransformer, sortTransformer, transformLogLine, webLog}
  import org.apache.spark.sql.functions.{asc, avg, col, desc, sum}
  import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

  import scala.util.{Failure, Success, Try}

  case class Config(duration : Long)

  class WebLogAnalytics extends Logging {

    val colNames = Seq("timestamp","clientIp","operation","userAgent","epochTime")
    val dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

    /**
     * Method to write the dataframe to CSV file
     * @param df
     * @param path
     * @param returnDf
     * @return The input dataframe if required for further processing
     */
    def writeResults(df: DataFrame, path: String, returnDf : Boolean): DataFrame = {
      df.coalesce(1)
        .write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
      .csv(path.concat(".csv"))
      //df.show(20,false)
      if (returnDf)
        df
      else
        null
    }
    /**
     * Method to write the dataframe to CSV file
     * @param df
     * @param path
     * @return Unit
     */

    def writeResults(df: DataFrame, path: String): Unit = {
      df.coalesce(1)
        .write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv(path.concat(".csv"))
      //df.show(20,false)
    }

    /**
     * Reads the text file to a dataframe that contains line content in each row
     * @param sparkSession
     * @param filePath
     * @return DataFrame
     */
    def readFile(sparkSession : SparkSession , filePath : String) : DataFrame = {
      val rawDf = sparkSession.read.text(filePath)
      rawDf
    }

    /**
     * This is a pre-processing method which splits the data
     * @param rawDf
     * @return Sorted DataFrame and SessionID included data
     */
    def runTransformations(rawDf : DataFrame ) : (DataFrame,DataFrame) = {
      val parsedLogLines= rawDf.transform(transformLogLine)
        .transform(filterErrorLogs)
        .transform(addEpochTime(dateFormat))
        .transform(webLog(colNames))
        .transform(sanitizeUrlAndClientIp)
      val orderedLogDf = parsedLogLines.transform(sortTransformer("epochTime"))
      orderedLogDf.persist
      val sessionIdDf = orderedLogDf.transform(sessionTransformer)
      (orderedLogDf,sessionIdDf)
    }

    /**
     * This is the method that takes in the session and other data related inputs to run pipelines
     * @param sparkSession
     * @param pipelines
     * @param inputPath
     * @param outPath
     * @param config
     */
    def orchestrate(sparkSession: SparkSession, pipelines : Pipelines , inputPath : String, outPath : String, config : Map[String,Any]) = {

      val rawDf = readFile(sparkSession,inputPath)
      val (orderedLogDf,sessionIdDf) = runTransformations(rawDf)
      pipelines.analyticsPipelineOne(orderedLogDf,sessionIdDf).seq.foreach{
        case(taskOp, taskName, triggerPipeline) => {
          if (triggerPipeline) {
            Try(writeResults(taskOp,outPath.concat(taskName),true)) match {
              case Success(value) => {
                logger.info("The task %s completed successfully",taskName)
                //trigger second pipeline here
                pipelines.analyticsPipelineTwo(value).seq.foreach{
                  case(taskOp,taskName) => {
                    Try(writeResults(taskOp,outPath.concat(taskName))) match {
                      case Success(value) => {
                        logger.info("The task %s completed successfully",taskName)
                      }
                      case Failure(exception) => {
                        logger.error("Failed with exception " + exception.getMessage + "- Task Name : " + taskName)
                      }
                    }
                  }
                }
              } case Failure(exception) => {
                logger.error("Failed with exception " + exception.getMessage + "- Task Name : " + taskName)
              }
            }
          } else {
            Try(writeResults(taskOp,outPath.concat(taskName))) match {
              case Success(value) => {
                logger.info("The task %s completed successfully",taskName)
              }
              case Failure(exception) => {
                logger.error("Failed with exception " + exception.getMessage + "- Task Name : " + taskName)
              }
            }
          }
        }

      }
    }


  }

  /**
   * Class to define the processing pipelines
   */
  class Pipelines {
    /**
     * Function to sessionise the logs.
     * Join with the Original Data with the Session data to apply the session ID to each log
     *
     */
    val sessioniseLogs : (DataFrame,DataFrame) => DataFrame  = (orderedLogDf, sessionIdDf )  =>  {
      val logsSessionized = orderedLogDf.join(sessionIdDf ,
        orderedLogDf("clientIp") === sessionIdDf("client") &&
          (orderedLogDf("epochTime") === sessionIdDf("startTime") ||
            (orderedLogDf("epochTime") > sessionIdDf("startTime") && orderedLogDf("epochTime") - sessionIdDf("startTime") <= sessionIdDf("activityLength"))))
      logsSessionized
    }
    /**
     * Function to get the Average session time
     */
    val averageSessionTime : DataFrame => DataFrame = sessionIdDf => {
      sessionIdDf.select(avg("sessionLength").alias("avgSessionTime"))
    }
    /**
     * Function for getting the session length in MS against Client IP address
     */
    val mostEngagedUsers : DataFrame => DataFrame = sessionIdDf => {
      val mostEngagedUsers = sessionIdDf.groupBy("client").
        agg( sum("sessionLength").alias("totalSessionLength_ms") ,
          avg("sessionLength").alias("avgSessionLength_ms")).
        orderBy(desc("totalSessionLength_ms"))
      mostEngagedUsers
    }
    /**
     * Function to get the count of Unique URL visits
     */
    val uniqueUrlVisit : DataFrame => DataFrame = logsSessionized => {
      val uniqueUrlVisit = logsSessionized.groupBy("sessionId" , "url").count().alias("count").filter(col("count").equalTo(1))
        .select("sessionId" , "url")
      uniqueUrlVisit
    }
    /**
     * Pipeline One that contains Sessionise Logs, Average Session time, To find the most engaged users
     */
    val analyticsPipelineOne : (DataFrame,DataFrame) => List[(DataFrame,String,Boolean)] = (orderedLogDf, sessionIdDf) => {
      List(
        (sessioniseLogs(orderedLogDf,sessionIdDf),"log-sessions",true),
        (averageSessionTime(sessionIdDf),"average-session-time",false),
        (mostEngagedUsers(sessionIdDf),"most-engaged-users",false)
      )
    }

    /**
     *pipeline that finds the unique URL visits
     */
    val analyticsPipelineTwo : DataFrame => List[(DataFrame,String)] = logsSessionized => {
      List(
        (uniqueUrlVisit(logsSessionized),"unique-url-visit")
      )
    }


  }
