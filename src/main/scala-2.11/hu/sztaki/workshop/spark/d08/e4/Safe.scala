package hu.sztaki.workshop.spark.d08.e4

import hu.sztaki.workshop.spark.d08.e4.SafeRDD.chickenProofRDDWhatever
import org.apache.spark.{SparkConf, SparkContext}

object Safe {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf())

    /**
      * @todo[1] Explain this use-case for me. What happens here?
      * @todo[2] Fix it!
      * @hint Use safe.
      */
    val safeData = sc
      .parallelize((1 to 100000000).toSeq, 2)
      .safe()
      .map((x : Int) => Math.random())
      .map((d : Double) => dirtyBlackBox(d))
      .cache()

    println(safeData.dirty().count())
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