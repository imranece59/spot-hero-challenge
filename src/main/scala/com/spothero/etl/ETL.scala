package com.spothero.etl

import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import java.util.Date
import java.util.Calendar
import java.util.Iterator;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive._
import org.apache.spark.sql.SparkSession
import java.sql.{ Timestamp, Date }
import com.spothero.utils.DataframeReadWriteUtils
import com.spothero.constants.Constants
import com.spothero.helper.ProcessDataHelper._
import org.apache.log4j.{ Level, Logger }

object logger extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

object ETL {

  def main(args: Array[String]) {
    logger.log.info("Parsing Arguments...")
    if (args.isEmpty) {
      logger.log.error(Constants.NO_ARGUMENT_MSG)
      System.exit(0);
    }
    val params = args.map(_.split('=')).map {
      case Array(param, value) => (param, value)
    }.toMap

    var inputPath: String = ""
    var outputPath: String = ""
    var outPutSaveMode: Boolean = false
    if (params.contains("--input-file-path")) {
      inputPath = params.get("--input-file-path").get.asInstanceOf[String]
    } else {
      logger.log.error(Constants.INVALID_KEY)
      System.exit(0);
    }

    if (params.contains("--output-save-mode")) {
      outPutSaveMode = params.get("--output-save-mode").get.asInstanceOf[String].toBoolean
      if (outPutSaveMode) {
        logger.log.info("=== Output will be written to path as csv ==")
        if (params.contains("--output-file-path")) {
          outputPath = params.get("--output-file-path").get.asInstanceOf[String]
        } else {
          logger.log.error(Constants.INVALID_KEY)
          System.exit(0);
        }
      }else{
        logger.log.info("=== Output will be displayed in the console ==")
      }
    } else {
      logger.log.error(Constants.INVALID_KEY)
      System.exit(0);
    }

    logger.log.info("Arguments passed successfully")

    val sparkSession = SparkSession.builder
      .appName("ETL")
//      .master("local[*]")
      .getOrCreate

    try {
      logger.log.info("ETL Process starts ==>")
      import sparkSession.implicits._
      val sc = sparkSession.sparkContext
      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
      val cohortDf = DataframeReadWriteUtils.creatingDataframeFromParquet(sparkSession, inputPath + "/cohort_data")
      val clickStreamDf = DataframeReadWriteUtils.creatingDataframeFromParquet(sparkSession, inputPath + "/clickstream_data/*")
      val transacTimeDf = DataframeReadWriteUtils.creatingDataframeFromParquet(sparkSession, inputPath + "/accounting_data/*")
      DataframeReadWriteUtils.dataframepersist(Seq(cohortDf, clickStreamDf, transacTimeDf))
      val (noOfRecordsRead,noOfRecordsWritten,resultDf) = calculateTopPerformingCohort(Seq(cohortDf, clickStreamDf, transacTimeDf))
      if (outPutSaveMode) {
        DataframeReadWriteUtils.writeDataframeToCsv(resultDf, outputPath)
      } else {
        resultDf.show(noOfRecordsWritten.toInt, false)
      }
      DataframeReadWriteUtils.dataframeunpersist(Seq(cohortDf, clickStreamDf, transacTimeDf))
      logger.log.info(s"Total Number of records read : ${noOfRecordsRead} ,Total Number of records written : ${noOfRecordsWritten}")
      logger.log.info("====== ETL process ends =====")
      sparkSession.stop
    } catch {
      case e: Exception =>
        val builder = StringBuilder.newBuilder
        builder.append(e.getMessage)
        (e.getStackTrace.foreach { x => builder.append(x + "\n") })
        val err_message = builder.toString()
        println(err_message)
        sparkSession.stop()

    }
  }
}