package driskill.scalding

import driskill.core.SDD
import com.twitter.scalding.{TypedPipe, Tsv}

class ScaldingSDD[T](val pipe: TypedPipe[T], val sc: ScaldingContext) extends SDD[T] {
  import com.twitter.scalding.Dsl
  import com.twitter.scalding.Mode
  import cascading.flow.FlowDef

  def flatMap(f: T => TraversableOnce[String]) = {
    val fp = f.asInstanceOf[Function1[T, Iterable[String]]]
    new ScaldingSDD[String](pipe.flatMap(fp), sc)
  }

  def countByValue[U]()(implicit ev: T <:< U, cm: ClassManifest[U], ord: Ordering[U]) = {
    new ScaldingSDD[(U, Long)](pipe
      .map { t => (t.asInstanceOf[U], 1L) }
      .group
      .sum, sc)
  }

  def saveAsTextFile(path: String) {
    // no-op
  }
}
