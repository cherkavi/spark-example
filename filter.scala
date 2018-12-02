// example of Scala program
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

dataFrame.filter("time > 2000 and ( name == 'black' or name like '%lac%' ) and time is not null")
dataFrame.filter($"time".isNotNull)
dataFrame.filter( not(dataFrame("name").rlike("hite")) && not(dataFrame("name").rlike("usk")))
