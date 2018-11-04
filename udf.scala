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

// additional column with the same name 
val dataFrameWithCopyColumn = dataFrame.withColumn("copy-name", col("name"))

// additional column with custom function 
import org.apache.spark.sql.functions.udf
val customHash = udf[Int, Int, String](
	(columnTime, columnName)=> columnTime+columnName.length	
)

val dataFrameWithHash = dataFrameWithCopyColumn.withColumn("customHashCode", customHash($"time", $"name"))

