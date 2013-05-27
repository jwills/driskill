package driskill.core

trait SDD[T] {
  
  def flatMap(f: T => TraversableOnce[String]): SDD[String]

  def countByValue: SDD[(T, Long)]

  def saveAsTextFile(path: String): Unit

}
