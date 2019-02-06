val dataFrame = sc.parallelize(Seq(
	("425", 1),
	("425", 2),
	("425", 3),
	("500", 5),
	("500", 6),
	("500", 7),
	("500", 8),
	("750", 5),
	("750", 6),
	("750", 7),
	("750", 8)
)).toDF("session_id","value1")


val result = dataFrame.groupBy("session_id").agg(avg("value1"))
result.show
