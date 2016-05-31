package hu.sztaki.workshop.spark.d05.e2

import org.apache.spark._

object Wordcount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input =  sc.textFile(args(0), 30)
    /**
      * @todo [1] Complete the word count:
      *           count the frequencies of all words in the text file.
      */
    val frequencies = input
      .flatMap(_.split(" "))
      .map(word => new Tuple2(word, 1))
      .reduceByKey(_ + _)
        .sortBy(_._2,false)
          .take(2)
    /*
    .reduceByKey((x, y) => x + y)
    .groupByKey()
    .map {
      case (key : String, iter : Iterable[Int])
        => (key, iter.size)
    }
    .map(tuple => (tuple._1, tuple._2.size))
    */
    /**
      * @todo [2] Save the counts back to a file.
      */
    // frequencies.saveAsTextFile(args(1))

    /**
      * @todo [3] Print out the counts.
      */
    //val results = frequencies.collect()

    frequencies foreach println

    Thread.sleep(1000 * 60 * 60)
  }
}