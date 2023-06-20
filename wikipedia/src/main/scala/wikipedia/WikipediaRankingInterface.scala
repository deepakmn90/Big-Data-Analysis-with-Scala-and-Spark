package wikipedia

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD

/**
 * The interface used by the grading infrastructure. Do not change signatures
 * or your submission will fail with a NoSuchMethodError.
 *
 * The WikipediaRankingInterface trait provides a set of methods
 * for processing and analyzing Wikipedia articles.
 * The interface is designed to work with Apache Spark's RDD
 * (Resilient Distributed Datasets) for distributed processing.
 */
trait WikipediaRankingInterface:
  /**
   * Builds an index of Wikipedia articles grouped by language.
   *
   * @param langs The list of languages to include in the index.
   * @param rdd   The RDD containing WikipediaArticle objects.
   * @return An RDD of pairs where the key is a language and the value is an Iterable of WikipediaArticle objects
   *         in that language.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])]
  /**
   * Counts the number of occurrences of a specific language in the RDD of Wikipedia articles.
   *
   * @param lang The language to count occurrences of.
   * @param rdd  The RDD containing WikipediaArticle objects.
   * @return The number of occurrences of the specified language in the RDD.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int
  /**
   * Ranks the languages based on the total number of occurrences in the RDD of Wikipedia articles.
   *
   * @param langs The list of languages to rank.
   * @param rdd   The RDD containing WikipediaArticle objects.
   * @return A list of pairs where the key is a language and the value is the total number of occurrences in the RDD,
   *         sorted in descending order by occurrence count.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)]
  /**
   * Ranks the languages based on the total number of occurrences
   * in the RDD of Wikipedia articles using reduceByKey for aggregation.
   *
   * @param langs The list of languages to rank.
   * @param rdd   The RDD containing WikipediaArticle objects.
   * @return A list of pairs where the key is a language and the value is the total number of occurrences in the RDD,
   *         sorted in descending order by occurrence count.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)]
  /**
   * Ranks the languages based on the total number of occurrences
   * using an index previously built from an RDD of Wikipedia articles.
   *
   * @param index The RDD containing an index of Wikipedia articles grouped by language.
   * @return A list of pairs where the key is a language and the value is the total number of occurrences in the RDD,
   *         sorted in descending order by occurrence count.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)]
  /**
   * Returns the list of supported languages.
   *
   * @return A list of strings representing the supported languages.
   */
  def langs: List[String]
  /**
   * Returns the SparkContext used for distributed processing.
   *
   * @return The SparkContext object.
   */
  def sc: SparkContext
  /**
   * Returns the RDD of WikipediaArticle objects.
   *
   * @return The RDD containing WikipediaArticle objects.
   */
  def wikiRdd: RDD[WikipediaArticle]
