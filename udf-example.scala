  def filterData(spark: SparkSession, data: DataFrame): DataFrame = {
    val filteredData = data
      .filter(col("message") === "GPS1")
      .select("column_1", "column_2", "column_3", "column_4")

    // define UDF for extracting latitude and longitude
    val getLat =
      udf[Option[Double], Map[String, String]](x => Try(x("LAT_NAVI").toDouble).toOption)
    val getLng =
      udf[Option[Double], Map[String, String]](x => Try(x("LONG_NAVI").toDouble).toOption)

    // extract latitude and longitude and keep only predefined columns
    val columnsToKeep =
      Seq[String]("column_1", "column_2", "column_3")

    filteredData
      .withColumn("latitude", getLat(filteredData.col("column_4")))
      .withColumn("longitude", getLng(filteredData.col("column_4")))
      .na
      .drop(Seq("lat", "lng"))
      .select(columnsToKeep.map(x => col(x)): _*)
  }
