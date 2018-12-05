// example of Scala program
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

import org.apache.spark.sql.functions._
val funcLightCode = udf[Int, String]( name=> name match{ case "black"=>0;case "grey"=>1;case "white"=>2;case "dusk"=>3; } )

val changedDataFrame = dataFrame.withColumn("lightCode", funcLightCode(dataFrame("name"))).withColumnRenamed("name","amountOfLight").drop("time")
// .withColumnRenamed("name","amountOfLight")
// .drop("name")
// val changedDataFrame2 = changedDataFrame.withColumnRenamed("name","amountOfLight").drop("time")