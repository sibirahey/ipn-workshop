package hu.sztaki.workshop.spark.d06.e3

import hu.sztaki.workshop.spark.d06.e3.SalesRDDFunctions.{beefConversion}
import org.apache.spark.{SparkConf, SparkContext}

object Sales {
  def main(args: Array[String]) {
    /**
      * @todo[1] Create Spark context, configuration, read from text file.
      */
    val sc = new SparkContext(new SparkConf())
    val dataRDD = sc.textFile(args(0), 5)

    /**
      * @todo[2] Transform lines to SalesRecord.
      */
    val salesRDD = dataRDD.map { row =>
      val colValues = row.split(",")
      new SalesRecord(colValues(0), colValues(1),
        colValues(2), colValues(3).toDouble)
    }

    /**
      * @todo[3] Calculate the total sales.
      */
    val totalSales = salesRDD.totalSales
    println(s"Total sales is $totalSales.")

    /**
      * @todo[4] Set up a discount of  a0.1nd print out
      *          the new records.
      */
    val discountRDD = salesRDD.discount(0.1)
    println(discountRDD.collect().toList)
  }
}
