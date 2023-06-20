package stackoverflow

import org.apache.spark.rdd.RDD
import Aliases.*

/**
 * The interface used by the grading infrastructure. Do not change signatures
 * or your submission will fail with a NoSuchMethodError.
 */
trait StackOverflowInterface:
  /**
   * Cluster the results based on the means and vectors.
   *
   * @param means   Array of means represented as tuples (Int, Int)
   * @param vectors RDD of vectors represented as tuples (LangIndex, HighScore)
   * @return Array of tuples containing cluster results as (String, Double, Int, Int)
   */
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)]
  /**
   * Group the postings by question ID (QID).
   *
   * @param postings RDD of Posting objects
   * @return RDD of tuples containing grouped postings as (QID, Iterable[(Question, Answer)])
   */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])]
  /**
   * Perform k-means clustering on the given vectors.
   *
   * @param means  Array of means represented as tuples (Int, Int)
   * @param vectors RDD of vectors represented as tuples (Int, Int)
   * @param iter   Number of iterations to perform (default: 1)
   * @param debug  Flag indicating whether to enable debugging output (default: false)
   * @return Array of tuples containing the updated means after clustering
   */
  def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)]
  /**
   * Parse the raw lines of text and convert them to Posting objects.
   *
   * @param lines RDD of raw text lines
   * @return RDD of Posting objects
   */
  def rawPostings(lines: RDD[String]): RDD[Posting]
  /**
   * Sample the vectors to retrieve a subset.
   *
   * @param vectors RDD of vectors represented as tuples (LangIndex, HighScore)
   * @return Array of tuples containing the sampled vectors as (Int, Int)
   */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)]
  /**
   * Calculate the scores for the grouped postings.
   *
   * @param grouped RDD of grouped postings as (QID, Iterable[(Question, Answer)])
   * @return RDD of tuples containing questions and their corresponding high scores as (Question, HighScore)
   */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)]
  /**
   * Convert the scored postings to vector representations.
   *
   * @param scored RDD of scored postings as (Question, HighScore)
   * @return RDD of vectors represented as tuples (LangIndex, HighScore)
   */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)]
  /**
   * Spread factor for the languages.
   */
  def langSpread: Int
  /**
   * List of supported languages.
   */
  val langs: List[String]
