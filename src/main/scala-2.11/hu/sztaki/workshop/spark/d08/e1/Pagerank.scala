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
object Pagerank {
  def main(args: Array[String]) {
    /**
      * @todo[1] Setup Spark & read from text file.
      */
    val sc = new SparkContext(new SparkConf())
    val rawData = sc.textFile(args(0))

    val iterations = args(1).toInt

    /**
      * @todo[2] Collect all the outlinks for each URL together.
      * @hint One RDD for the graph.
      */
    val graph = rawData.map {
      line =>
        val outlink = line.split("\t")
        (outlink(0), outlink(1))
    }.distinct().groupByKey().cache()

    /**
      * @todo[3] Setup initial ranks for each URL.
      * @hint Another RDD for the ranks.
      */
    var ranks = graph.map(x => (x._1, 1.0))

    /**
      * @todo[4] Do a few iterations.
      */
    for (i <- 1 to iterations) {
      /**
        * @todo[5] Calculate the contributions from each URL to its outlinks (URLs).
        * @hint It should contribute its rank, divided by the number of outlinks it has.
        */
      val votes = graph.join(ranks).values.flatMap {
        case (neighbors, rank) =>
          val numberOfNeighbors = neighbors.size
          neighbors.map { neighbor =>
            (neighbor, rank / numberOfNeighbors)
          }
      }
      /**
        * @todo[6] Reduce the contributions and cosmetic the new ranks with the
        *          following formula: (newRank * 0.85 + 0.15)
        */
      ranks = votes
        .reduceByKey(_ + _)
        .mapValues(rank => rank * 0.85 + 0.15)
    }

    /**
      * @todo[7] Print out the top 10 URL based on their ranks.
      */
    ranks sortBy(_._2, ascending = false) take 10 foreach println
  }
}