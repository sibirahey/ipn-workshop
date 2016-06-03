package hu.sztaki.workshop.spark.d10.e1

//import breeze.linalg.{squaredDistance, DenseVector, Vector}

import org.apache.spark.{SparkConf, SparkContext}
import scala.math._

/**
  * K-means clustering.
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.clustering.KMeans.
  */
object KMeans {

  def generateData: Array[Vector[Double]] = {
    def generatePoint(i: Int): Vector[Double] = {
      Vector.fill(2) {random * 1000}
    }
    Array.tabulate(10000)(generatePoint)
  }

  /**
    * @todo[5] Define the function that computes the distance of 2 vectors.
    */
  def squaredDistance(a: Vector[Double], b: Vector[Double]): Double = {
    ???
  }

  /**
    * @todo[8] Define the function that adds two vectors.
    */
  def addVec(a: Vector[Double], b: Vector[Double]): Vector[Double] = {
    ???
  }

  /**
    * @todo[9] Define the function that multiplies a vector by a scalar.
    */
  def multVec(v: Vector[Double], s: Double): Vector[Double] = {
    ???
  }

  /**
    * @todo[6] Define the function that computes the closest vector from an array to a vector.
    */
  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    ???
  }

  def main(args: Array[String]) {

    /**
      * @todo[1] Setup Spark.
      */

    /**
      * @todo[2] Get the data and cache it.
      */

    /**
      * @todo[3] Parse the arguments K and convergeDist.
      */
    val K = ???
    val convergeDist = ???

    /**
      * @todo[4] Get K random vector from the vectors.
      * @hint: use the takeSample function of the RDD
      */
    val kPoints = ???
    var tempDist = 1.0

    while(tempDist > convergeDist) {
      /**
        * @todo[7] Get the closest center to all points.
        */
      val closest = ???

      /**
        * @todo[10] Update the centers.
        */
      val newPoints = ???

      /**
        * @todo[11] Calculate the sum of the squared distance change.
        */

      /**
        * @todo[12] Update kPoints.
        */
      println("Finished iteration (delta = " + tempDist + ")")
    }

    /**
      * @todo[13] Print results, stop Spark.
      */
    println("Final centers:")
  }
}