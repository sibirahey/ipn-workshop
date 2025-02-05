package hu.sztaki.workshop.spark.d08.e2

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * @todo Create similar function to Iterator.padTo & test it.
 * @todo Create similar function to Iterator.path & test it.
 */
class AdvancedRDDFunctions[T](self: RDD[T])
  (implicit t: ClassTag[T], ord: Ordering[T] = null)
extends Logging
with Serializable {
  /**
    * @todo[1] Implement a function that drops the first n elements
    *          from each partition.
    */
  def dropPartition(n: Int): RDD[T] = ???

  /**
    * @todo[2] Implement a function that drops elements from each partition
    *          while a certain condition applies.
    */
  def dropWhilePartition(f: T => Boolean): RDD[T] = ???

  /**
    * @todo[3] Implement the `filterNot` function.
    */
  def filterNot(f: T => Boolean): RDD[T] = ???

  /**
    * @todo[4] Implement a function that takes elements from each partition
    *          while a certain condition applies.
    */
  def takeWhile(f: T => Boolean): RDD[T] = ???
}

object AdvancedRDD {
  def main (args: Array[String]) {
    val sc = new SparkContext(
      new SparkConf()
        .setAppName("AdvancedRDD")
        .setMaster("local")
    )

    val numbersRDD = sc.parallelize((1 to 100).toList, 2)

    println("Drop partition result:")
    numbersRDD.dropPartition(5).collect foreach println

    println("Drop while partition result:")
    numbersRDD.dropWhilePartition(_ < 10).collect foreach println

    println("Filter not result:")
    numbersRDD.filterNot(_ > 10).collect foreach println

    println("Take while result:")
    numbersRDD.takeWhile(_ < 10).collect foreach println
  }

  /**
    * @todo[5] Implement the implicit conversion that converts a simple RDD
    *          to our AdvancedRDDFunctions.
    */
  implicit def rddToAdvancedRDD[T](rdd: RDD[T])
    (implicit t: ClassTag[T], ord: Ordering[T] = null): AdvancedRDDFunctions[T] = ???

  object RDDImplicits {
    implicit class RichRDD[T: ClassTag](rdd: RDD[T]) {
      /**
        * @todo[6.1] Implement the logic of wordcount in a generic way.
        */
      def countEachElement = ???

      /**
        * @todo[6.2] Implement a function that counts each element on which
        *          a certain condition applies.
        */
      def countWhere(f: T => Boolean): Long = ???
    }
  }
}