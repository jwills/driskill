package driskill.spark

import driskill.core._
import spark.SparkContext

class SparkDriskillContext(val sc: SparkContext) extends DriskillContext {
  def readTextFile(path: String) = new SparkSDD[String](sc.textFile(path))

  def done() { sc.stop() }
}
