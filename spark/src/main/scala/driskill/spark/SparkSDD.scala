package driskill.spark

import driskill.core._
import spark.RDD

class SparkSDD[T: ClassManifest](val rdd: RDD[T]) extends SDD[T] {
  import spark.SparkContext._

  def flatMap(f: T => TraversableOnce[String]) = {
    new SparkSDD[String](rdd.flatMap(f))
  }

  def countByValue[U]()(implicit ev: T <:< U, cm: ClassManifest[U], ord: Ordering[U]) = {
    new SparkSDD[(U, Long)](rdd.map(t => (t.asInstanceOf[U], 1L)).reduceByKey( _ + _))
  }

  def saveAsTextFile(path: String) {
    rdd.saveAsTextFile(path)
  }  
}
