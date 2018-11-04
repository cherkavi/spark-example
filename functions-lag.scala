case class TimeName(id:Int, name:String)

val dataFrame = Seq(
	TimeName( 510, "black"),
	TimeName( 720, "grey" ),
	TimeName( 810, "white"),
	TimeName( 920, "white")
).toDF("id", "name")

val window = org.apache.spark.sql.expressions.Window.orderBy("id")

import org.apache.spark.sql.functions.lag

val lagFrame = dataFrame.withColumn("before", lag("name",1,0).over(window))
