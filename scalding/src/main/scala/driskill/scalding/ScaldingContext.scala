package driskill.scalding

import driskill.core._

import cascading.flow._
import com.twitter.scalding.{Mode, TypedTsv}

class ScaldingContext(val mode: Mode) extends DriskillContext {
  import com.twitter.scalding.Dsl._
  import com.twitter.scalding.TDsl._

  lazy implicit val flowDef = new FlowDef()

  def readTextFile(path: String) = {
    new ScaldingSDD[String](TypedTsv[String](path), this)
  }

  def done() {
    // no-op for now
  }
}
