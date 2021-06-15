package com.spothero.utils

import org.apache.spark.sql.hive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode

object DataframeReadWriteUtils {

  /**
   *  create dataframe from parquet
   *  param sparkSession
   *  type org.apache.spark.sql.SparkSession
   *  param filePath, "s3" --> s3://test-bucket/object/, "hdfs" --> hdfs://test/folder/, "local" --> file:///test/folder/
   */
  def creatingDataframeFromParquet(sparkSession: SparkSession, filePath: String): DataFrame = {
    sparkSession.read.option("mergeSchema", "true").parquet(filePath)
  }

  /**
   *  dataframe persist to cache/disk
   *  param dfSeqs , Seq of multiple dataframes
   *  type :Seq[DataFrame]
   */
  def dataframepersist(dfSeqs: Seq[DataFrame]) = {
    for (df <- dfSeqs) {
      df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }

  /**
   *  dataframe unpersist
   *  param dfSeqs , Seq of multiple dataframes
   *  type :Seq[DataFrame]
   */
  def dataframeunpersist(dfSeqs: Seq[DataFrame]) = {
    for (df <- dfSeqs) {
      df.unpersist()
    }
  }

  /**
   *  this method performs writing dataframe as csv to path
   *  param df
   *  type :DataFrame
   *  param path, output write path
   *  type : String
   */
  def writeDataframeToCsv(df: DataFrame, path: String) {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }
}



