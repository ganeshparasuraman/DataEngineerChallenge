package com.paypay.dataengg

import org.apache.spark.sql.SparkSession

object Main {
  /**
   *
   * @param args
   * This is the main method.
   * This method runs commandline program and creates a Spark Session at the Local Host.
   * This method Invokes the orchestrate method with spark session, filepath , outpath for results, and Config map
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                .builder()
                 .config("session.duration", 900000L)
                .appName("DataEngg Challenge")
      .master("local[*]")
      .getOrCreate()
    val filePath = "/path/DataEngineerChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log"
    val outPath = "/path/DataEngineerChallenge/data/"
    val pipelines = new Pipelines()
    val webLogAnalyticsRunner = new WebLogAnalytics
    webLogAnalyticsRunner.orchestrate(spark,pipelines,filePath,outPath,Map("duration" -> 900000))

   }
}
