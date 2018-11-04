// https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html

val dataFrame = sc.parallelize(Seq(
	(425, "black"),
	(510, "black"),
	(600, "grey"),
	(720, "grey"),
	(810, "white"),
	(920, "white"),
	(1025, "white"),
	(1200, "white"),
	(1700, "white"),
	(1845, "dusk"),
	(1915, "dusk"),
	(1945, "dusk"),
	(2015, "black"),
	(2140, "black"),
	(2215, "black")
)).toDF("time","name")

// another approach to create DataFrame from Seq
import spark.implicits._
case class NumberName(id:Int, name:String)
val dataFrame2 = Seq( new NumberName(1, "one"), new NumberName(2, "two"), new NumberName(3, "three") ).toDF("number", "string-value")

// create DataFrame from RDD<-Seq with predefined schema
import org.apache.spark.sql._
import org.apache.spark.sql.types._
val row1 = Row.fromSeq(Seq(425, "black"))
val row2 = Row.fromSeq(Seq(720, "grey"))
val row3 = Row.fromSeq(Seq(810, "white"))

val rdd = spark.sparkContext.makeRDD(List(row1, row2, row3))
val schema = List(
	StructField("time", IntegerType, nullable = false),
	StructField("name", StringType, nullable = false)
)
val dataFrame3 = spark.createDataFrame(rdd, StructType(schema))
