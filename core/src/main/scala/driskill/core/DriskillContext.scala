package driskill.core

trait DriskillContext {
  def readTextFile(path: String): SDD[String]

  def done(): Unit
}
