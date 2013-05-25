package driskill.crunch

import driskill.core.SDD

import org.apache.crunch.{PCollection, DoFn, MapFn, Emitter, Pair => CPair}
import org.apache.crunch.types.PType
import org.apache.crunch.io.To
import org.apache.crunch.types.avro.Avros
import java.lang.{Long => JLong}

class CrunchSDD[T](val pc: PCollection[T]) extends SDD[T] {
  import CrunchSDD._

  def flatMap(f: T => TraversableOnce[String]) = {
    new CrunchSDD[String](pc.parallelDo(flatMapFn(f), Avros.strings()))
  }

  def countByValue[U]()(implicit ev: T <:< U, cm: ClassManifest[U], ord: Ordering[U]) = {
    val count = pc.count()
    val map = mapFn((x: CPair[T, JLong]) => (x.first().asInstanceOf[U], x.second().longValue()))
    new CrunchSDD[(U, Long)](count.parallelDo(map, tuple2(pc.getPType().asInstanceOf[PType[U]], longs)))
  }

  def saveAsTextFile(path: String) {
    pc.write(To.textFile(path))
  }
}

// Utility Do and MapFns
class SDoFn[S, T](private val f: S => TraversableOnce[T]) extends DoFn[S, T] {
  override def process(input: S, emitter: Emitter[T]) {
    for (v <- f(input)) {
      emitter.emit(v)
    }
  }
}

class SMapFn[S, T](private val f: S => T) extends MapFn[S, T] {
  override def map(input: S) = f(input)
}

object CrunchSDD {
  def flatMapFn[S, T](f: S => TraversableOnce[T]) = new SDoFn[S, T](f)
  def mapFn[S, T](f: S => T) = new SMapFn[S, T](f)

  def derived[S, T](cls: java.lang.Class[T], in: S => T, out: T => S, pt: PType[S]) = {
    pt.getFamily().derived(cls, new SMapFn[S, T](in), new SMapFn[T, S](out), pt)
  } 

  def tuple2[T1, T2](p1: PType[T1], p2: PType[T2]) = {
    val in = (x: CPair[T1, T2]) => (x.first(), x.second())
    val out = (x: (T1, T2)) => CPair.of(x._1, x._2)
    derived(classOf[(T1, T2)], in, out, Avros.pairs(p1, p2))
  }

  val longs = {
    val in = (x: JLong) => x.longValue()
    val out = (x: Long) => new JLong(x)
    derived(classOf[Long], in, out, Avros.longs())
  }
}
