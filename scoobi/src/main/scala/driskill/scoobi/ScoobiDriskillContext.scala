package driskill.scoobi

import driskill.core._
import com.nicta.scoobi.Scoobi._

case class ScoobiDriskillContext(implicit sc: ScoobiConfiguration) extends DriskillContext {
  def readTextFile(path: String) = ScoobiSDD(fromTextFile(path))

  def done() {}
}
