// User Defined aggregated function 

val dataFrame = sc.parallelize(Seq(
	(425, "black"),
	(510, "black"),
	(600, "grey"),
	(720, "grey"),
	(810, "white"),
	(920, "white"),
	(1025, "white"),
	(1700, "white"),
	(1845, "dusk"),
	(1915, "dusk"),
	(1945, "dusk"),
	(2015, "black"),
	(2230, "black")
)).toDF("time","name")

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class AverageTime extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = StructType(StructField("time", IntegerType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(StructField("elements", ArrayType(LongType, false)) :: StructField("count", LongType) :: Nil)

  // output of aggregatation function.
  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  // init 
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Long]()
    buffer(1) = 0
  }

  // next element in group
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Array[Long]](0) :+ input.getAs[Long](0)
    buffer(1) = buffer.getAs[Integer](1) + 1
  }

  // merge with another row ( from another RDD )
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Array[Long]](0) ++ buffer1.getAs[Array[Long]](0)
    buffer1(1) = buffer1.getAs[Integer](1) + buffer2.getAs[Integer](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Long = {
  	buffer.getAs[Array[Long]](0).sum/buffer.getAs[Integer](1)
  }

}
val groupping = new AverageTime
dataFrame.groupBy("name").agg(groupping(col("time"))).show()

