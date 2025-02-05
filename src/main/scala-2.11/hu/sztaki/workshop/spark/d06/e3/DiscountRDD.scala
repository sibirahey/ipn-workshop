package hu.sztaki.workshop.spark.d06.e3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

class DiscountRDD(prev: RDD[SalesRecord], discountPercentage: Double)
  extends RDD[SalesRecord](prev) {

  /**
    * @todo[6] The compute should compute a SalesRecord by applying
    *          the specified disccount.
    */
  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] = {
    firstParent[SalesRecord].iterator(split, context).map {
      salesRecord =>
        val discount = salesRecord.itemValue * discountPercentage
        new SalesRecord(
          salesRecord.transactionId,
          salesRecord.customerId,
          salesRecord.itemId,
          discount)
    }
  }

  override protected def getPartitions: Array[Partition] =
    firstParent[SalesRecord].partitions
}