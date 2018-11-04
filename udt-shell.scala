// TODO 
import spark.implicits._
import org.apache.spark.sql.myexample._

val data = Seq(
	TimeName( 510, "black"),
	TimeName( 720, "grey" ),
	TimeName( 810, "white"),
	TimeName( 920, "white")
)

val schema = new StructType().add("timeName-custom", new TimeNameUDT(), false);
val datasetFromRdd = spark.createDataFrame(data, schema);
datasetFromRdd.show(false);