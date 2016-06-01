package hu.sztaki.workshop.spark.d07.e1

import hu.sztaki.workshop.spark.d07.e1.AdvancedRDD.RichRDD
import org.apache.spark.{SparkConf, SparkContext}

object BigramAnalysis{
  def main(args: Array[String]){
    /**
      * @todo[0] Setup Spark, read from text file. Any text file.
      */
    val sc = new SparkContext(new SparkConf())
    val lines = sc.textFile(args(0), 5)

    /**
      * @todo[1] Transform to `Bigram`s and filter invalid items.
      * @hint Use the companion object of `model.Bigram`.
      */
    val bigrams = lines
      .flatMap(Bigram(_))
      .filter(_.isValidBigram)

    /**
      * @todo[2] Total number of bigrams.
      */
    val pleaseSparkDoThe = bigrams
    val totalNumberOfBigrams = pleaseSparkDoThe.count()
    println(s"Total number of bigrams: $totalNumberOfBigrams.")

    /**
      * @todo[3] Cache bigrams.
      */
    val cachedBigrams = bigrams.cache()

    /**
      * @todo[4] How many unique bigrams do we have?
      */
    val uniqueBigrams = cachedBigrams.distinct().count()
    println(s"Unique bigrams: $uniqueBigrams.")

    /**
      * @todo[5] Count each element.
      * @hint Use the AdvancedRDD (implicitly).
      */
    val bgOccurCount = bigrams.countEachElement
    println(s"Each element has the count $bgOccurCount.")

    /**
      * @todo[6] Number of bigrams that appear only once.
      * @hint Use the AdvancedRDD (some method).
      */
    println(
      bgOccurCount.countWhere(_._2 == 1)
    )

    /**
      * @todo[7] List the top ten most frequent bigrams and their counts.
      */

    /**
      * @todo[8] What fraction of all bigrams occurrences does the top ten bigrams account for?
      *          That is, what is the cumulative frequency of the top ten bigrams?
      */

    /**
      * @todo[9*] Determine the frequency of bigrams with the same start.
      * @hint Use `BigramsWithSameStart` and aggregateByKey also.
      */

    //  [(String, ((String, Int)), Int)]
    //  startWord - BG           - BG count    - bgs starting with word
    //  a._1      - a._2._1._1   - a._2._1._2  - a._2._2

    /**
      * @todo[10] What are the five most frequent words following the word "for"?
      * @todo[11] What is the frequency of observing each word?
      */

    /**
      * If there are a total of N words in your vocabulary,
 *
      * then there are a total of N^2 possible values for F(Wn|Wn-1)—in theory,
      * every word can follow every other word (including itself).
      * @todo[12] What fraction of these values are non-zero?
      * In other words, what proportion of all possible events are actually observed?
      * To give a concrete example, let's say that following the word "happy",
      * you only observe 100 different words in the text collection.
      * This means that N-100 words are never seen after "happy".
      * (Perhaps the distribution of happiness is quite limited?).
      */

  }
}



