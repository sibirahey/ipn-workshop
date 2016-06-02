package hu.sztaki.workshop.spark.d08.e1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @exercise[8/1] Compute the PageRank of nodes (URLs).
  * Input file should
  * be in format of:
  * URL         neighbor URL
  * URL         neighbor URL
  * URL         neighbor URL
  * ...
  * where URL and their neighbors are separated by space(s).
  */
object Pagerank{
  def main(args: Array[String]) {
    /**
      * @todo[1] Setup Spark & read from text file.
      */
    val iterations = ???

    /**
      * @todo[2] Collect all the outlinks for each URL together.
      */

    /**
      * @todo[3] Setup initial ranks for each URL.
      */

    /**
      * @todo[4] Do a few iterations.
      */
    for (i <- 1 to iterations) {
      /**
        * @todo[5] Calculate the contributions from each URL to its outlinks (URLs).
        * @hint It should contribute its rank, divided by the number of outlinks it has.
        */

      /**
        * @todo[6] Reduce the contributions and cosmetic the new ranks with the
        *          following formula: (newRank * 0.85 + 0.15)
        */
    }

    /**
      * @todo[7] Print out the top 10 URL based on their ranks.
      */
  }
}