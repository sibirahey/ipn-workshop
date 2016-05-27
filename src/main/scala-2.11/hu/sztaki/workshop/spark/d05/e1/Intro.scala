package hu.sztaki.workshop.spark.d05.e1

import org.apache.spark.{SparkConf, SparkContext}

import scala.runtime.RichInt

object Intro {
  def main(args: Array[String]) {
    println("This is NOT just a test.")

    val sparkConfiguration =
      new SparkConf()
        .setMaster("local[2]")
        .setAppName("Not just a test.")
    val sparkContext = new SparkContext(sparkConfiguration)

    val data = 1 to 1000

    val dataRDD = sparkContext.parallelize(data)
    val filteredRDD = dataRDD.filter(_ < 10)

    val output = filteredRDD.collect()

    output foreach println
  }
}
