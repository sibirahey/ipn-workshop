package hu.sztaki.workshop.spark.d06.e2

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

class Transactions(sc: SparkContext) {
  def run(t: String, u: String): RDD[(String, String)] = {
    /**
      * @todo[4] Read and parse the transactions into an RDD of pairs.
      *          Transaction information
      *          (transaction-id, product-id, user-id, purchase-amount, item-description)
      */
    val transactions = sc.textFile(t)
    val usersWithProducts = transactions.map {
      t =>
        val tComponents = t.split("\t")
        // (user-id, product-id)
        (tComponents(2).toInt, tComponents(1).toInt)
    }
    /**
      * @todo[5] Read and parse the users into an RDD of pairs.
      *          User information (id, email, language, location)
      * @hint Use `processData` method.
      */
    val users = sc.textFile(u)
    val usersWithLocations = users.map {
      u =>
        val uComponents = u.split("\t")
        // (user-id, location)
        (uComponents(0).toInt, uComponents(3))
    }

    val productFrequency = processData(usersWithProducts, usersWithLocations)

    /**
      * @todo[7] Load the result back to the cluster.
      */
    sc.parallelize(productFrequency.toSeq).map {
      p =>
        (p._1.toString, p._2.toString)
    }
  }

  /**
    * @todo[6] Find out for each product, that how many distinct locations was it
    *          ordered from.
    * @hint Use `join`.
    */
  def processData (t: RDD[(Int, Int)], u: RDD[(Int, String)]) : Map[Int, Long] = {
    val distinctProductsWithLocations =
    t.leftOuterJoin(u).values.distinct
      distinctProductsWithLocations.countByKey
  }
}

object Transactions {
  /**
    * In this main method do the following.
    * @todo[1][OK] Read the input and output arguments from command line.
    * @todo[2][OK] Create and configure SparkContext.
    * @todo[3][OK] Run the configured job: it should be a `new Transactions`
    * @todo[8][OK] Save the results as a text file.
    * @todo[9][OK] Stop the SparkContext.
    */
  def main(args: Array[String]): Unit = {
    val transactionsInput = args(1)
    val usersInput = args(0)
    val outputPath = args(2)

    val sc = new SparkContext(new SparkConf())

    val job = new Transactions(sc)
    val result = job.run(transactionsInput, usersInput)

    result.saveAsTextFile(outputPath)

    Thread.sleep(60 * 60 * 1000)

    sc.stop()
  }
}