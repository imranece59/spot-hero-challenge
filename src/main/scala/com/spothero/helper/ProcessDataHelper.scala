package com.spothero.helper

import org.apache.spark.sql.{ DataFrame, Dataset }
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.spothero.constants.Constants
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{ Level, Logger }

object logger extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
object ProcessDataHelper {

  /**
   * This method perform calculating top performing cohort in each markets
   * param dfSeqs , Seq of multiple dataframes
   * type :Seq[DataFrame]
   * return summaryDf
   * type :Dataframe
   */
  def calculateTopPerformingCohort(dfSeqs: Seq[DataFrame]): (Long,Long,DataFrame) = {
    logger.log.info("Calculating top performing cohorts ==>")
    val joinAllDfs = dfSeqs.apply(0).as('cohort)
      .join(dfSeqs.apply(1).as('clickStream), col("cohort.user_id") === col("clickStream.user_id"))
      .join(dfSeqs.apply(2).as('analytic), col("clickStream.facility_id") === col("analytic.facility_id")
        && col("analytic.transaction_timestamp").between(col("clickStream.event_start_timestamp"), col("clickStream.event_end_timestamp")), "leftouter")
      .select(
        col("cohort.user_name"),
        col("cohort.cohort_name"),
        col("cohort.marketing_id"),
        col("clickStream.event_type"),
        col("clickStream.facility_id"),
        col("clickStream.market_name"),
        col("clickStream.event_start_timestamp"),
        col("clickStream.event_end_timestamp"),
        col("analytic.transaction_timestamp"),
        col("analytic.transaction_amount"),
        col("analytic.revenue_amount"),
        col("analytic.transaction_type"))
      .filter(col("transaction_timestamp").isNotNull) //remove the data's where no transaction happened
      .withColumn("event_month", date_format(col("transaction_timestamp"), "MMM-yy")) //converting date to Mon-YY
      .withColumn("week_of_month", date_format(col("transaction_timestamp"), "W")) //taking week of month from timestamp
    val purchaseAggDf = joinAllDfs.filter(col("transaction_type").equalTo("PURCHASE"))
      .groupBy(
        col("event_month"),
        col("week_of_month"),
        col("cohort_name"),
        col("market_name"))
      .agg(sum("revenue_amount").alias("total_revenues")) // taking sum of revenues from purchased data
    val chargeBackAggDf = joinAllDfs.filter(col("transaction_type").equalTo("CHARGEBACK"))
      .groupBy(
        col("event_month"),
        col("week_of_month"),
        col("cohort_name"),
        col("market_name"))
      .agg(sum("transaction_amount").alias("total_chargeback")) // taking sum of amounts from chargeback data

    //calculate the top performing cohorts by revenue in each markets
    // *total revenues will be calculated by adjusting the chargeback amount from total revenue generated in all transactions
    val summaryDf = purchaseAggDf.alias('a).join(chargeBackAggDf.alias('b), col("a.event_month") === col("b.event_month") && col("a.week_of_month") === col("b.week_of_month")
      && col("a.cohort_name") === col("b.cohort_name") && col("a.market_name") === col("b.market_name"), "left")
      .select(
        col("a.event_month"),
        col("a.week_of_month"),
        col("a.cohort_name"),
        col("a.market_name"),
        round(col("a.total_revenues") - abs(coalesce(col("b.total_chargeback"), lit(0)))).alias("adjusted_total_revenue"))
      .withColumn("rank", rank.over(Window.partitionBy(
        col("a.market_name"),
        col("a.event_month"),
        col("a.week_of_month")).orderBy(col("adjusted_total_revenue").desc)))
      .filter("rank=1")
      .drop("rank")
      .orderBy(
        col("a.market_name"),
        col("a.event_month"),
        col("a.week_of_month"))
    (joinAllDfs.count,summaryDf.count(),summaryDf)
  }

}