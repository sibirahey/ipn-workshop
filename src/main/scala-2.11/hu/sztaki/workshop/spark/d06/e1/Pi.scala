package hu.sztaki.workshop.spark.d06.e1

import org.apache.spark.{SparkConf, SparkContext}

import scala.math._

object Pi {
  def main(args: Array[String]) {
    /**
      * @todo[1] Approximate Pi with the Monte Carlo method.
      *          Learn about the method at:
      *          http://polymer.bu.edu/java/java/montepi/MontePi.html
      */
    val sc = new SparkContext(new SparkConf())
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(1000000L * slices, Int.MaxValue).toInt

    val throws = sc.parallelize(1 until n, slices)
    val thrown = throws.map { iDontCare =>
      // Throw with the dart.
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }
    val counts = thrown.reduce(_ + _)

    println(s"Pi is roughly ${4.0 * counts / n}.")
  }
}
