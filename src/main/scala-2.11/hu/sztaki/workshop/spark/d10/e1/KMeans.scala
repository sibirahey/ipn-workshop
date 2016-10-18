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
    sqrt(a.zip(b).map(a => pow(a._1-a._2, 2)).sum)
  }

  /**
    * @todo[8] Define the function that adds two vectors.
    */
  def addVec(a: Vector[Double], b: Vector[Double]): Vector[Double] = {
    a.zip(b).map(a => a._1 + a._2)
  }

  /**
    * @todo[9] Define the function that multiplies a vector by a scalar.
    */
  def multVec(v: Vector[Double], s: Double): Vector[Double] = {
    v.map(_ * s)
  }

  /**
    * @todo[6] Define the function that computes the closest vector from an array to a vector.
    */
  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    val distance = centers.map(squaredDistance(_, p))
    distance.indexOf(distance.min)
  }

  def main(args: Array[String]) {

    /**
      * @todo[1] Setup Spark.
      */
    val sc = new SparkContext(new SparkConf())

    /**
      * @todo[2] Get the data and cache it.
      */
    val data = sc.parallelize(generateData).cache()

    /**
      * @todo[3] Parse the arguments K and convergeDist.
      */
    val K = args(0).toInt
    val convergeDist = args(1).toDouble

    /**
      * @todo[4] Get K random vector from the vectors.
      * @hint: use the takeSample function of the RDD
      */
    val kPoints = data.takeSample(false,K)
    var tempDist = 1.0

    while(tempDist > convergeDist) {
      /**
        * @todo[7] Get the closest center to all points.
        */
      val closest = data.map(x => (closestPoint(x,kPoints),(x,1)))

      /**
        * @todo[10] Update the centers.
        */
      val newPoints = closest.reduceByKey((a, b) => (addVec(a._1, b._1), a._2 + b._2))
        .map(a => (a._1,multVec(a._2._1, (1/a._2._2) ))

      /**
        * @todo[11] Calculate the sum of the squared distance change.
        */
      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      /**
        * @todo[12] Update kPoints.
        */
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println("Finished iteration (delta = " + tempDist + ")")
    }

    /**
      * @todo[13] Print results, stop Spark.
      */
    println("Final centers:")
    kPoints foreach println
    sc.stop()
  }
}