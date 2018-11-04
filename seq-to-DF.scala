val dataFrame = sc.parallelize(Seq(
	(415, "black"),
	(500, "black"),
	(630, "dawn"),
	(730, "dawn"),
	(815, "day"),
	(930, "day"),
	(1015, "day"),
	(1230, "day"),
	(1730, "day"),
	(1830, "dusk"),
	(1900, "dusk"),
	(1930, "dusk"),
	(2030, "black"),
	(2130, "black"),
	(2230, "black")
)).toDF("time","partOfTheDay")

import spark.implicits._
val dataFrame2 = Seq( (1, "one"), (2, "two"), (3, "three") ).toDF("number", "string-value")
