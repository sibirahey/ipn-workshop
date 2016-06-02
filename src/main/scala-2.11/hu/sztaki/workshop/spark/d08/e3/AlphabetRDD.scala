package hu.sztaki.workshop.spark.d08.e3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}

class AlphabetRDD(prev:RDD[String]) extends RDD[String](prev){
  /**
    * @todo[3] Decide if a string is an alphabet.
    */
  def checkAlpha(str:String): Boolean = ???

  /**
    * @todo[4] Override the compute method of the RDD.
    *           Split the string and filter out the non-alphabets.
    */
  override def compute(split: Partition, context: TaskContext): Iterator[String] = ???

  override protected def getPartitions: Array[Partition] = firstParent[String].partitions
  override val partitioner = firstParent[String].partitioner

}


object AlphabetRDD {
  def main(args: Array[String]): Unit = {
    /**
      * @todo[1] Setup again! Also, read from text file.
      */

    /**
      * @todo[2] Count the records (line of the text file).
      */

    /**
      * @todo[5] Create the AlphabetRDD from the general RDD
      *           and count the elements again.
      */
  }
}