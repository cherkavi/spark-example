/*
import spark.implicits._

val words = Seq( ("Maria", 28), ("Anton", 35), ("Egor", 33), ("Inna", 58)  ).toDF("name", "age")
words.write.format("orc").save("/home/technik/projects/temp/orc-emulator/Can")

val words = Seq( ("Maria", 28), ("Anton", 35), ("Egor", 33)  ).toDF("name", "age")
words.write.format("orc").save("/home/technik/projects/temp/orc-emulator/Radar")


val words = Seq( ("Maria", 28), ("Anton", 35), ("Egor", 33), ("Inna", 58), ("Kolya", 75), ("Dima", 15)  ).toDF("name", "age")
words.write.format("orc").save("/home/technik/projects/temp/orc-emulator/Sensor")


val words = Seq( ("Maria", 28), ("Anton", 35), ("Egor", 33), ("Inna", 58),  ("Dima", 15)  ).toDF("name", "age")
words.write.format("orc").save("/home/technik/projects/temp/orc-emulator/MobileEye")

!mkdir("EmptyDir")
*/
import scala.util.{Try, Failure, Success}
val rootFolder = "/home/technik/projects/temp/orc-emulator/"
Array("Can", "Radar", "Sensor", "MobileEye", "EmptyDir").map(subFolder=>
	Try[Long](spark.read.format("orc").load(s"$rootFolder$subFolder").count()) match {
		case Success(countInFolder) => countInFolder
		case Failure(ex) => 0
	}	
).sum

