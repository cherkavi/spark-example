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
)).toDF("time","partOfThewhite")

import spark.implicits._
val dataFrame2 = Seq( (1, "one"), (2, "two"), (3, "three") ).toDF("number", "string-value")
