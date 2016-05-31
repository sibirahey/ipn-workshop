package hu.sztaki.workshop.spark.d06.e2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

class Transactions(sc: SparkContext) {
  def run(t: String, u: String) : RDD[(String, String)] = {
    /**
      * @todo[4] Read and parse the transactions into an RDD of pairs.
      *          Transaction information
      *          (transaction-id, product-id, user-id, purchase-amount, item-description)
      */

    /**
      * @todo[5] Read and parse the users into an RDD of pairs.
      *          User information (id, email, language, location)
      * @hint Use `processData` method.
      */

    /**
      * @todo[7] Load the result back to the cluster.
      */
    ???
  }

  /**
    * @todo[6] Find out for each product, that how many distinct locations was it
    *          ordered from.
    */
  def processData (t: RDD[(Int, Int)], u: RDD[(Int, String)]) : Map[Int,Long] = ???
}

object Transactions {
  /**
    * In this main method do the following.
    * @todo[1][OK] Read the input and output arguments from command line.
    * @todo[2][OK] Create and configure SparkContext.
    * @todo[3][OK] Run the configured job: it should be a `new Transactions`
    * @todo[8] Save the results as a text file.
    * @todo[9] Stop the SparkContext.
    */
  def main(args: Array[String]): Unit = {
    val transactionsInput = args(1)
    val usersInput = args(0)
    val outputPath = args(2)

    val sc = new SparkContext(new SparkConf())

    val job = new Transactions(sc)
    val result = job.run(transactionsInput, usersInput)
  }
}