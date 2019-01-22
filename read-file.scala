// parquet
val parkuetDF = spark.read.load("/path/to/file/users.parquet")
parkuetDF.select("name", "color").write.save("new_file.parquet")

// json
spark.read.format("json").load("examples/src/main/resources/people.json")

// csv
val csvDF = spark.read.format("csv")
.option("sep", ";")
.option("inferSchema", "true")
.option("header", "true")
.load("/path/to/file/people.csv")

// orc
sqlContext.read.format("orc").load(orcfile)
spark.read.option("inferSchema", true).orc("filepath")
spark.read.format("org.apache.spark.sql.execution.datasources.orc")

// orc alternative
val path = "/user/nafochir/labeled_data/preprocessedLabels"
dfval df = spark.read.orc(path)
df.head(10).toJSON
val df3 = df.toJSON.take(10)

// also accessible next formats spark-sql_*.jar/org/apache/spark/sql/execution/datasources:
// * csv
// * jdbc
// * json
// * orc
// * parquet
// * text
// * v2
