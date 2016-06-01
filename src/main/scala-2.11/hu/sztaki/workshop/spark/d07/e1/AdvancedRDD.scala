package hu.sztaki.workshop.spark.d07.e1

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object AdvancedRDD {
  implicit class RichRDD[T: ClassTag](rdd: RDD[T]) {
    /**
      * @todo Encapsulate the logic of wordcount.
      */
    def countEachElement = {
      rdd.map((_,1)).reduceByKey(_+_)
    }

    /**
      * @todo Count elements where the boolean function is true.
      */
    def countWhere(f: T => Boolean): Long = {
      rdd.filter(f).count()
    }

    /**
      * @todo Sort by descending.
      */
    def sortByDesc[K : Ordering: ClassTag](f: T => K): RDD[T] = {
      rdd.sortBy(f, ascending = false)
    }

    /**
      * @todo[HARD] Complete the function.
      */
    def explode[U](f: T => TraversableOnce[U]): RDD[(U, T)] = ???
  }
}
