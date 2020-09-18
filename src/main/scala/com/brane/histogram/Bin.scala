package com.brane.histogram

case class Edge(var value: Double, var inclusive: Boolean = false)

object Edge {
  implicit def orderingByValue[A <: Edge]: Ordering[A] =
    Ordering.by(e => (e.value, !e.inclusive))
}

case class Bin(lower: Edge, upper: Edge, var freq: Int, var checkFreq: Boolean = false) {
  override def toString() = {
    s"${
      if (lower.inclusive) "["
      else "("
    }${lower.value},${upper.value}${if (upper.inclusive) "]" else ")"}:$freq${if (checkFreq) "?" else ""} "
  }

  def mergeInto(other: Bin) = {
    other.copy(lower = other.lower.copy(other.lower.value, other.lower.inclusive),
      upper = other.upper.copy(upper.value, upper.inclusive), freq = this.freq + other.freq)
  }

  def isEquality: Boolean = lower.value == upper.value

  def distance: Double = upper.value - lower.value

}

object Bin {
  implicit def orderingByLowerEdge[A <: Bin]: Ordering[A] =
    Ordering.by(e => e.lower)

  def orderingByFreq[A <: Bin]: Ordering[A] =
    Ordering.by(e => e.freq)

  def volume(bin: Bin): Double = midValue(bin) * bin.freq

  def midValue(bin: Bin): Double = (bin.upper.value - bin.lower.value).toDouble / 2

  def domainCoverageRatio(bin: Bin): Double = if (bin.freq == 0) 0 else (bin.upper.value - bin.lower.value) / bin.freq
}
