package timeusage

import org.apache.spark.sql.*
import org.apache.spark.sql.types.*

/**
 * The interface used by the grading infrastructure. Do not change signatures
 * or your submission will fail with a NoSuchMethodError.
 */
trait TimeUsageInterface:
  /**
   * The SparkSession instance.
   */
  val spark: SparkSession
  /**
   * Given a list of column names, returns three lists of `Column` objects representing classified columns.
   *
   * @param columnNames The list of column names.
   * @return A tuple containing three lists of `Column` objects representing classified columns.
   */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column])
  /**
   * Converts a list of strings representing a line to a Row object.
   *
   * @param line The list of strings representing a line.
   * @return The Row object.
   */
  def row(line: List[String]): Row
  /**
   * Performs grouping and aggregation on a DataFrame to compute time usage statistics.
   *
   * @param summed The input DataFrame containing summed time usage values.
   * @return The DataFrame with time usage statistics.
   */
  def timeUsageGrouped(summed: DataFrame): DataFrame
  /**
   * Performs grouping and aggregation using SQL on a DataFrame to compute time usage statistics.
   *
   * @param summed The input DataFrame containing summed time usage values.
   * @return The DataFrame with time usage statistics.
   */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame
  /**
   * Generates a SQL query string for performing grouping and aggregation using SQL on a DataFrame.
   *
   * @param viewName The name of the SQL view representing the input DataFrame.
   * @return The SQL query string.
   */
  def timeUsageGroupedSqlQuery(viewName: String): String
  /**
   * Performs grouping and aggregation on a typed Dataset to compute time usage statistics.
   *
   * @param summed The input Dataset containing summed time usage values.
   * @return The Dataset with time usage statistics.
   */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow]
  /**
   * Computes time usage summary statistics based on classified columns.
   *
   * @param primaryNeedsColumns The list of classified columns representing primary needs.
   * @param workColumns The list of classified columns representing work-related activities.
   * @param otherColumns The list of classified columns representing other activities.
   * @param df The input DataFrame containing time usage data.
   * @return The DataFrame with time usage summary statistics.
   */
  def timeUsageSummary(primaryNeedsColumns: List[Column], workColumns: List[Column], otherColumns: List[Column], df: DataFrame): DataFrame
  /**
   * Computes time usage summary statistics based on classified columns using a typed Dataset.
   *
   * @param timeUsageSummaryDf The input DataFrame containing time usage summary statistics.
   * @return The Dataset with time usage summary statistics.
   */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow]
