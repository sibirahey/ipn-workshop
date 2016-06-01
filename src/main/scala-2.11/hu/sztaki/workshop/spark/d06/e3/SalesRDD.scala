package hu.sztaki.workshop.spark.d06.e3

import org.apache.spark.rdd.RDD

class SalesRDDFunctions(rdd: RDD[SalesRecord]) {
  def totalSales = rdd.map(_.itemValue).sum
  def discount(discountPercentage: Double) = new DiscountRDD(rdd, discountPercentage)
}

/**
  * Class that represents a sales record.
  */
class SalesRecord(val transactionId: String,
                  val customerId: String,
                  val itemId: String,
                  val itemValue: Double)
  extends Comparable[SalesRecord]
  with Serializable {
  /**
    * Compares two sales record.
    */
  override def compareTo(o: SalesRecord): Int = transactionId.compareTo(o.transactionId)

  /**
    * @todo[homework] Override toString method for a pretty print.
    */
}

object SalesRDDFunctions {
  /**
    * @todo[5] Create our own implicit function that takes an RDD of SalesRecord
    *          and creates our own SalesRDDFunctions type.
    */
  implicit def beefConversion(rdd: RDD[SalesRecord]): SalesRDDFunctions = {
    new SalesRDDFunctions(rdd)
  }
}