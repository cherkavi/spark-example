// TODO
package org.apache.spark.sql.myexample

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}

// locate *spark*.jar
// scalac -classpath "/home/technik/.sdkman/candidates/spark/2.3.0/jars/*" udt.scala -d udt.jar
// :require udt.jar

/**
 * User-defined type for [[TimeName]].
 */
private[sql] class TimeNameUDT extends UserDefinedType[TimeName] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  // name of the class with full package
  override def pyUDT: String = "org.apache.spark.sql.myexample.TimeNameUDT"

  override def serialize(p: TimeName): GenericArrayData = {
    val output = new Array[Any](2)
    output(0) = p.id
    output(1) = p.title
    new GenericArrayData(output)
  }

  override def deserialize(datum: Any): TimeName = {
    datum match {
      case values: ArrayData =>
        new TimeName(values.getInt(0), values.getUTF8String(1).toString)
    }
  }

  override def userClass: Class[TimeName] = classOf[TimeName]

  private[spark] override def asNullable: TimeNameUDT = this
}


/**
 * An example class to demonstrate UDT in Scala, Java, and Python.
 */
@SQLUserDefinedType(udt = classOf[TimeNameUDT])
private[sql] class TimeName(val id: Int, val title: String) extends Serializable {

  override def hashCode(): Int = 31 * (31 * id.hashCode()) + title.hashCode()

  override def equals(other: Any): Boolean = other match {
    case that:TimeName  => this.id == that.id && this.title == that.title
    case _ => false
  }

  override def toString(): String = s"($id, $title)"
}

