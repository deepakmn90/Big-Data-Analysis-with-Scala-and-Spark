package wikipedia

import scala.io.Source
import scala.io.Codec

/**
 * A utility object providing functions to access and parse the Wikipedia dataset.
 */
object WikipediaData:

  /**
   * Retrieves the lines of the Wikipedia dataset.
   *
   * @return A list of strings representing the lines of the dataset.
   */
  private[wikipedia] def lines: List[String] =
    Option(getClass.getResourceAsStream("/wikipedia/wikipedia-grading.dat")) match
      case None => sys.error("Please download the dataset as explained in the assignment instructions")
      case Some(resource) => Source.fromInputStream(resource)(Codec.UTF8).getLines().toList

  /**
   * Parses a line of the Wikipedia dataset and constructs a `WikipediaArticle` object.
   *
   * @param line The line to parse.
   * @return A `WikipediaArticle` object constructed from the parsed line.
   */
  private[wikipedia] def parse(line: String): WikipediaArticle =
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    WikipediaArticle(title, text)
