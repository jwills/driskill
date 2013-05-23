package driskill.crunch

import driskill.core._
import org.apache.crunch.Pipeline

class CrunchContext(private val p: Pipeline) extends DriskillContext {
  def readTextFile(path: String) = new CrunchSDD[String](p.readTextFile(path))

  def done() { p.done() }
}
