package hu.sztaki.workshop.spark.d05.e2

import org.apache.spark._

object Wordcount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input =  sc.textFile(args(0))
    /**
      * @todo [1] Complete the word count:
      *           count the frequencies of all words in the text file.
      */

    /**
      * @todo [2] Save the counts back to a file.
      */

    /**
      * @todo [3] Print out the counts.
      */
  }
}