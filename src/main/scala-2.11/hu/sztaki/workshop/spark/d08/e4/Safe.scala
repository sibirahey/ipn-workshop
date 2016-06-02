package hu.sztaki.workshop.spark.d08.e4

import org.apache.spark.{SparkConf, SparkContext}

object Safe {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName(""))

    /**
      * @todo[1] Explain this use-case for me. What happens here?
      * @todo[2] Fix it!
      * @hint Use safe.
      */
    val cleanData = sc
      .parallelize((1 to 100000000).toSeq, 2)
      .map(x => Math.random())
      .map(dirtyBlackBox)
      .repartition(5)

    println(cleanData.count())
  }

  /**
    * Ugly, dirty good for nothing function that can throw some sort of
    * exception.
    */
  def dirtyBlackBox(x : Double): Double = {
    if(x < 0.9)
      x
    else
      throw new RuntimeException
  }
}