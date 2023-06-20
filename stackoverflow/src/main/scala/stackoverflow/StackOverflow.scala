package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Logger, Level}

import annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Properties.isWin
import scala.io.Source
import scala.io.Codec

object Aliases:
  type Question = Posting
  type Answer = Posting
  type QID = Int
  type HighScore = Int
  type LangIndex = Int
import Aliases.*

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object StackOverflow extends StackOverflow:

  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  // Set Hadoop home directory for Windows if running on Windows OS
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  // Initialize Spark configuration and context
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /**
   * Main function.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit =
    // Define the location of the input file
    val inputFileLocation: String = "/stackoverflow/stackoverflow-grading.csv"
    // Get the input file as an input stream
    val resource = getClass.getResourceAsStream(inputFileLocation)
    // Create a Source from the input stream using UTF-8 encoding
    val inputFile = Source.fromInputStream(resource)(Codec.UTF8)

    // Convert the lines of the input file to an RDD
    val lines   = sc.parallelize(inputFile.getLines().toList)
    // Extract the raw postings from the RDD of lines
    val raw     = rawPostings(lines)
    // Group the questions and answers together
    val grouped = groupedPostings(raw)
    // Compute the maximum score for each posting
    val scored  = scoredPostings(grouped)
    // Convert the scored postings to vectors
    val vectors = vectorPostings(scored)
//    assert(vectors.count() == 1042132, "Incorrect number of vectors: " + vectors.count())

    // Perform k-means clustering on the sampled vectors
    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    // Cluster the vectors using the computed means
    val results = clusterResults(means, vectors)
    // Print the clustering results
    printResults(results)

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable:

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /**
   * Load postings from the given file.
   *
   * @param lines RDD of lines representing postings
   * @return RDD of Posting objects
   */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if arr(2) == "" then None else Some(arr(2).toInt),
              parentId =       if arr(3) == "" then None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if arr.length >= 6 then Some(arr(5).intern()) else None)
    })


  /**
   * Group the questions and answers together.
   *
   * @param postings RDD of Posting objects
   * @return RDD of tuples containing the question ID (QID) and an iterable collection of (Question, Answer) pairs
   */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] =
    // Filter the postings to get only the questions
    val questions = postings.filter(_.postingType == 1)
    // Filter the postings to get only the answers
    val answers = postings.filter(_.postingType == 2)
    // Map the questions RDD to key-value pairs where the key is the question ID
    val questionsKeyed = questions.map(post => (post.id, post))
    // Map the answers RDD to key-value pairs where the key is the parent question ID
    val answersKeyed = answers.map(post => (post.parentId.get, post))
    // Perform an inner join between questions and answers based on the question ID and parent question ID
    val questionsWithAnswers = questionsKeyed.join(answersKeyed)
    // Group the joined RDD by question ID and return an RDD of tuples containing the question ID
    // and an iterable collection of (Question, Answer) pairs
    questionsWithAnswers.groupByKey()


  /**
   * Compute the maximum score for each posting.
   *
   * @param grouped RDD of tuples containing the question ID (QID)
   *                and an iterable collection of (Question, Answer) pairs
   * @return RDD of tuples containing the Question and its corresponding maximum score (HighScore)
   */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] =
    /**
     * Compute the maximum score among the given answers.
     *
     * @param as An array of Answer objects.
     * @return The highest score among the answers.
     */
    def answerHighScore(as: Array[Answer]): HighScore =
      var highScore = 0
      var i = 0
      while i < as.length do
        val score = as(i).score
        if score > highScore then
          highScore = score
        i += 1
      highScore

    // Map each grouped entry to the Question and its corresponding maximum score
    grouped.map {
      case (_, entries) =>
        val answers = entries.map(p => p._2).toArray
        val question = entries.head._1
        val score = answerHighScore(answers)
        (question, score)
    }


  /**
   * Compute the vectors for the kmeans.
   *
   * @param scored RDD of tuples containing the Question and its corresponding maximum score (HighScore)
   * @return RDD of tuples representing the vectors for the k-means algorithm,
   *         with LangIndex as the key and HighScore as the value
   */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] =
    /**
     * Return optional index of first language that occurs in `tags`.
     *
     * @param tag The optional tag (language) of the question.
     * @param ls  The list of languages to search for.
     * @return The optional index of the first language that occurs in `tags`.
     */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] =
      tag match
        case None => None
        case Some(lang) =>
          val index = ls.indexOf(lang)
          if (index >= 0) Some(index) else None

    scored.flatMap { (question, score) =>
      firstLangInTag(question.tags, langs) match
        case Some(index) => (index * langSpread, score) :: Nil
        case None => Nil
    }.persist()


  /**
   * Sample the vectors.
   *
   * @param vectors RDD of tuples representing the vectors for the k-means algorithm,
   *                with LangIndex as the key and HighScore as the value
   * @return Array of tuples representing the sampled vectors,
   *         with Int as the first element and Int as the second element
   */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] =

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    /**
     * Perform reservoir sampling on the iterator to obtain a fixed-size sample.
     *
     * @param lang The language index.
     * @param iter The iterator of integers.
     * @param size The desired sample size.
     * @return An array of integers representing the sampled elements.
     */
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] =
      // Create an array to store the sampled elements
      val res = new Array[Int](size)
      // Create a random number generator with the specified language index
      val rnd = new util.Random(lang)

      // Iterate 'size' number of times
      for i <- 0 until size do
        assert(iter.hasNext, s"iterator must have at least $size elements")
        // Add the next element from the iterator to the sample array
        res(i) = iter.next()

      // Initialize a counter for the total number of elements seen
      var i = size.toLong
      // Iterate through the remaining elements in the iterator
      while iter.hasNext do
        // Get the next element from the iterator
        val elt = iter.next()
        // Generate a random index between 0 and 'i'
        val j = math.abs(rnd.nextLong()) % i
        // If the index is less than 'size', replace the element at that index in the sample array
        if j < size then
          res(j.toInt) = elt
        // Increment the counter for the total number of elements seen
        i += 1

      // Return the sampled elements as an array
      res

    // Collect the same number of samples for each language.
    val res =
      if langSpread < 500 then
        // sample the space regardless of the language
        vectors.distinct.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey().flatMap(
          (lang, vectors) => reservoirSampling(lang, vectors.iterator.distinct, perLang).map((lang, _))
        ).collect()

    assert(res.length == kmeansKernels, res.length)
    res


  //
  //
  //  Kmeans method:
  //
  //

  /**
   * Main kmeans computation.
   *
   * @param means   The current means array.
   * @param vectors The RDD of vectors to be clustered.
   * @param iter    The iteration count (default: 1).
   * @param debug   A flag indicating whether to print debug information (default: false).
   * @return The final means array after convergence or reaching the maximum iterations.
   */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] =
    // TODO: Compute the groups of points that are the closest to each mean,
    // and then compute the new means of each group of points. Finally, compute
    // a Map that associate the old `means` values to their new values
    // Group vectors by the closest mean and compute the new means
    val newMeansMap: scala.collection.Map[(Int, Int), (Int, Int)] =
      vectors.groupBy(p => findClosest(p, means))
        .mapValues(averageVectors)
        .collectAsMap()
    // Compute the new means array
    val newMeans: Array[(Int, Int)] = means.map(oldMean => newMeansMap(oldMean))
    // Compute the distance between the old means and the new means
    val distance = euclideanDistance(means, newMeans)

    // Debugging output
    if debug then
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for idx <- 0 until kmeansKernels do
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")

    if converged(distance) then
      // Clustering has converged, return the new means
      newMeans
    else if iter < kmeansMaxIterations then
      // Clustering has not converged, continue to the next iteration
      kmeans(newMeans, vectors, iter + 1, debug)
    else
      // Reached the maximum number of iterations
      if debug then
        println("Reached max iterations!")
      newMeans




  //
  //
  //  Kmeans utilities:
  //
  //

  /**
   * Decide whether the kmeans clustering converged.
   *
   * @param distance The distance between the previous means and the updated means.
   * @return `true` if the clustering has converged, `false` otherwise.
   */
  def converged(distance: Double) =
    distance < kmeansEta


  /**
   * Return the euclidean distance between two points.
   *
   * @param v1 The first point.
   * @param v2 The second point.
   * @return The Euclidean distance between `v1` and `v2`.
   */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double =
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2

  /**
   * Return the euclidean distance between two points.
   *
   * @param a1 The first array of points.
   * @param a2 The second array of points.
   * @return The Euclidean distance between `a1` and `a2`.
   */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double =
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while idx < a1.length do
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    sum

  /**
   * Return the center that is closest to the given point `p`.
   *
   * @param p       The point for which to find the closest center.
   * @param centers The array of centers to compare with.
   * @return The center from the `centers` array that is closest to `p`.
   */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): (Int, Int) =
    var bestCenter: (Int, Int) = null
    var closest = Double.PositiveInfinity
    for center <- centers do
      val tempDist = euclideanDistance(p, center)
      if tempDist < closest then
        closest = tempDist
        bestCenter = center
    bestCenter


  /**
   * Average the vectors.
   *
   * @param ps The Iterable containing the vectors to be averaged.
   * @return The average vector computed from the elements in `ps`.
   */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) =
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while iter.hasNext do
      val item = iter.next()
      comp1 += item._1
      comp2 += item._2
      count += 1
    ((comp1 / count).toInt, (comp2 / count).toInt)




  //
  //
  //  Displaying results:
  //
  //
  /**
   * Compute cluster results.
   *
   * @param means   The means representing the clusters.
   * @param vectors The RDD of vectors.
   * @return An array of cluster results, each containing the language label, language percentage,
   *         cluster size, and median score.
   */
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] =
    // Pair each vector with its closest mean
    val closest = vectors.map(p => (findClosest(p, means), p))
    // Group vectors by their closest means
    val closestGrouped = closest.groupByKey()

    // Compute cluster results for each group of vectors
    val median = closestGrouped.mapValues { vs =>
      // Find the most common language in the cluster
      val lang = vs.groupBy(_._1).view.mapValues(_.size).maxBy(_._2)
      // most common language in the cluster
      val langLabel: String   = langs(lang._1 / langSpread)
      // percent of the questions in the most common language
      val langPercent: Double = lang._2 * 100 / vs.size
      val clusterSize: Int    = vs.size
      val (firstHalf, secondHalf) = vs.map(_._2).toArray.sorted.splitAt(vs.size / 2)
      val medianScore: Int =
        if firstHalf.length == secondHalf.length then
          (firstHalf.last + secondHalf.head) / 2
        else
          secondHalf.head

      (langLabel, langPercent, clusterSize, medianScore)
    }

    // Collect and sort the cluster results based on the median score
    median.collect().map(_._2).sortBy(_._4)

  /**
   * Print the cluster results.
   *
   * @param results The array of cluster results.
   */
  def printResults(results: Array[(String, Double, Int, Int)]): Unit =
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for (lang, percent, size, score) <- results do
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
