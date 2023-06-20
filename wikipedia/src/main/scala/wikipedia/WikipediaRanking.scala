package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.log4j.{Logger, Level}

import org.apache.spark.rdd.RDD
import scala.util.Properties.isWin

/**
 * A case class representing a Wikipedia article.
 *
 * @param title The title of the article.
 * @param text  The text content of the article.
 */
case class WikipediaArticle(title: String, text: String):
  /**
    * Checks whether the text of this article mentions the specified language `lang`.
    *
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)

/**
 * A singleton object implementing the WikipediaRankingInterface.
 * It provides functionality for ranking programming languages
 * based on the number of Wikipedia articles that mention each language.
 */
object WikipediaRanking extends WikipediaRankingInterface:
  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  // Set the 'hadoop.home.dir' system property for Windows to locate the necessary Hadoop binaries
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  // Define a list of programming languages
  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  // Create a SparkConf object with the local master URL and the application name
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wikipedia")
  // Create a SparkContext using the SparkConf object
  val sc: SparkContext = new SparkContext(conf)
  // Use a combination of `sc.parallelize`, `WikipediaData.lines`, and `WikipediaData.parse` to create an RDD of WikipediaArticle objects
  val wikiRdd: RDD[WikipediaArticle] =
    // Convert the WikipediaData.lines list into an RDD
    sc.parallelize(WikipediaData.lines)
      // Parse each line of Wikipedia data into a WikipediaArticle object
      .map(WikipediaData.parse)
      // Cache the RDD for faster access during subsequent operations
      .cache()

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   *
   * @param lang The language to count occurrences of.
   * @param rdd  The RDD containing WikipediaArticle objects.
   * @return The number of articles that mention the specified language at least once.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int =
    rdd.aggregate(0)(
      (count, article) => if (article.mentionsLanguage(lang)) count + 1 else count,
      _ + _
    )

  /** (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   *
   * @param langs The list of languages to rank.
   * @param rdd   The RDD containing WikipediaArticle objects.
   * @return A list of pairs where the key is a language and the value is the number of articles
   *         that mention the language, sorted in descending order by occurrence count.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    langs.map(lang => (lang, occurrencesOfLang(lang, rdd)))
      .sortBy(_._2)
      .reverse

  /** Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   *
   * @param langs The list of languages to include in the index.
   * @param rdd   The RDD containing WikipediaArticle objects.
   * @return An RDD of pairs where the key is a language and
   *         the value is an Iterable of WikipediaArticle objects in that language.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =
    val rawMap =
      for article <- rdd
          lang <- langs if article.mentionsLanguage(lang)
      yield (lang, article)
    rawMap.groupByKey()

  /** (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   *
   * @param index The RDD containing an index of Wikipedia articles grouped by language.
   * @return A list of pairs where the key is a language and the value is the number of articles
   *         that mention the language, sorted in descending order by occurrence count.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] =
    index.mapValues(article => article.size)
      .collect()
      .sortBy(_._2)
      .reverse
      .toList

  /** (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   *
   * @param langs The list of languages to rank.
   * @param rdd   The RDD containing WikipediaArticle objects.
   * @return A list of pairs where the key is a language and the value is the number of articles that mention the language, sorted in descending order by occurrence count.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    (for
      article <- rdd
      lang <- langs if article.mentionsLanguage(lang)
    yield (lang, 1))
      .reduceByKey(_ + _)
      .collect()
      .sortBy(_._2)
      .reverse
      .toList

  /**
   * The entry point of the program.
   *
   * @param args The command-line arguments.
   */
  def main(args: Array[String]): Unit =

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()

  /**
   * StringBuffer to store timing information.
   */
  val timing = new StringBuffer
  /**
   * Measures the execution time of a code block and appends the timing information to the `timing` buffer.
   *
   * @param label The label to identify the code block being timed.
   * @param code  The code block to be executed and timed.
   * @tparam T The return type of the code block.
   * @return The result of the code block.
   */
  def timed[T](label: String, code: => T): T =
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
