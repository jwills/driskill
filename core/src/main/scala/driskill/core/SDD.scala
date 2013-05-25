package driskill.core

trait SDD[T] {
  
  def flatMap(f: T => TraversableOnce[String]): SDD[String]

  def countByValue[U]()(implicit ev: T <:< U, ord: Ordering[U]): SDD[(U, Long)]

  def saveAsTextFile(path: String): Unit

}
