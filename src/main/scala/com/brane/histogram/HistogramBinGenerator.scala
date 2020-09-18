package com.brane.histogram

import scala.annotation.tailrec
import com.typesafe.scalalogging.StrictLogging


object HistogramBinGenerator extends StrictLogging {

  def createBins(percentileValues: List[Long], maxNumberOfBins: Int = 7, minSupport: Float = 0.05f): Seq[Bin] = {
    assert(percentileValues.size < 100)
    val valByFreq = percentileValues.groupBy(identity).map { case (k, v) => (k, v.size) }.toList.sortBy(_._1)
    val discreteByFreq = highFrequentValues(percentileValues, maxNumberOfBins)
    val bins = createBinsFromValuesWithFreq(valByFreq)
    val candidateBins = incorporateHighFrequentValuesAsEqualityBins(bins, discreteByFreq)
    logger.info(s"DropThreshold: ${dropTreshold(candidateBins, minSupport)}. Candidate ${candidateBins.size} bins: $candidateBins \n")
    evaluatePartitionPlans(candidateBins, maxNumberOfBins, minSupport)
  }

  private def highFrequentValues(values: List[Long], numberOfBins: Int) = {
    assert(numberOfBins > 0)
    val minRatioForDiscrete = math.min(1, 1f / numberOfBins)
    val valByFreq = values.groupBy(identity).map { case (k, v) => (k, v.size) }.toList.sortBy(_._1)
    logger.info(s"\n\n\nnValues:${values.size} required number of items per discrete:${values.size * minRatioForDiscrete} max freq:${valByFreq.head} numberOfBins: $numberOfBins")
    valByFreq.filter(_._2 >= values.size * minRatioForDiscrete)
  }

  private def createBinsFromValuesWithFreq(valueByFreq: List[(Long, Int)]) = {
    val bins: List[Bin] = valueByFreq.sliding(2).map {
      case List(l, u) =>
        Bin(Edge(l._1, true), Edge(u._1, false), l._2)
    }.toList
    val lastBin = bins.last
    lastBin.upper.inclusive = true
    lastBin.freq += valueByFreq.last._2
    bins
  }

  @tailrec
  private def incorporateHighFrequentValuesAsEqualityBins(bins: List[Bin], valueByFreq: List[(Long, Int)], acc: List[Bin] = List.empty): List[Bin] =
    (valueByFreq, bins) match {
      case (Nil, Nil) => acc.reverse.sorted
      case (_, Nil) => (createEqualityBins(valueByFreq) ++ acc).sorted
      case (Nil, _) => (bins ++ acc).sorted
      case (frequentValue :: remaining, head :: tail) =>
        var accumulated = acc
        if (head.lower.value == frequentValue._1) {
          head.lower.inclusive = true
          head.freq = frequentValue._2
          if (!head.upper.inclusive || head.upper.value != frequentValue._1) {
            if (head.upper.value != frequentValue._1) {
              val newBin = Bin(Edge(frequentValue._1), Edge(head.upper.value, head.upper.inclusive), 0, true)
              head.upper.value = frequentValue._1
              accumulated = newBin :: acc
            }
            head.upper.inclusive = true
          }
          incorporateHighFrequentValuesAsEqualityBins(tail, remaining, head :: accumulated)
        }
        else
          incorporateHighFrequentValuesAsEqualityBins(tail, valueByFreq, head :: accumulated)
    }

  private def createEqualityBins(discreteValByFreq: List[(Long, Int)]): List[Bin] =
    discreteValByFreq.map { case (value, freq) => Bin(Edge(value, true), Edge(value, true), freq) }

  private def totalNumberOfRangeDifferences(bins: Seq[Bin]): Int = bins.map(_.distance).groupBy(identity).size - 1

  private def dropTreshold(bins: Seq[Bin], minSupport: Float): Float = (totalSize(bins) * minSupport).toFloat

  private def totalSize(bins: Seq[Bin]) = bins.foldLeft(0.0)(_ + _.freq)

  private def partitionBins(partitionPlan: PartitionPlan, minSupport: Float): Seq[Bin] = {
    val mergedBins = mergeBinsWithPlan(partitionPlan)
    val resultWithoutSmallBins = dropTooSmallBins(mergedBins, partitionPlan.maxNumberOfBins, minSupport)
    logger.info(s"K=${partitionPlan.nTopDensityDifference}, with ${partitionPlan.sliceActions.size} slice actions ${mergedBins.size} bins from signals:[${partitionPlan.signal.mkString(",")}]: $mergedBins")
    if (resultWithoutSmallBins.size != mergedBins.size) logger.info(s"After dropping small bins remain ${resultWithoutSmallBins.size}: $resultWithoutSmallBins")
    resultWithoutSmallBins
  }

  private def evaluatePartitionPlans(candidateBins: Seq[Bin], maxNumberOfBins: Int, minSupport: Float): Seq[Bin] = {
    val (nTopDensityDifference, result) = evaluatePartitionPlansByDensity(candidateBins, maxNumberOfBins, minSupport, totalNumberOfRangeDifferences(candidateBins))
    if (result.size == maxNumberOfBins)
      result
    else
      evaluatePartitionPlansBySize(
        PartitionPlan(candidateBins, nTopDensityDifference, maxNumberOfBins).oneMorePartitionBySize(),
        minSupport,
        result)
  }

  @tailrec
  private def evaluatePartitionPlansByDensity(candidateBins: Seq[Bin], maxNumberOfBins: Int, minSupport: Float, totalNumberOfDifferences: Int, nTopDensityDifference: Int = 1, prevResult: Seq[Bin] = Seq.empty[Bin]): (Int, Seq[Bin]) = {
    if (nTopDensityDifference != 1 && nTopDensityDifference >= totalNumberOfDifferences)
      (nTopDensityDifference, prevResult)
    else {
      val partitionPlan = PartitionPlan(candidateBins, nTopDensityDifference, maxNumberOfBins)
      val result = partitionBins(partitionPlan, minSupport)
      if (result.size > maxNumberOfBins)
        (nTopDensityDifference - 1, prevResult)
      else evaluatePartitionPlansByDensity(candidateBins, maxNumberOfBins, minSupport, totalNumberOfDifferences, nTopDensityDifference + 1, result)
    }
  }

  @tailrec
  private def evaluatePartitionPlansBySize(partitionPlanOption: Option[PartitionPlan], minSupport: Float, prevResult: Seq[Bin]): Seq[Bin] = {
    partitionPlanOption match {
      case Some(plan) =>
        if (prevResult.size == plan.maxNumberOfBins)
          prevResult
        else {
          evaluatePartitionPlansBySize(plan.oneMorePartitionBySize(), minSupport, partitionBins(plan, minSupport))
        }
      case _ => prevResult
    }
  }

  private def mergeBinsWithPlan(plan: PartitionPlan): List[Bin] = mergeBinsWithPlanSignals(plan.candidateBins, plan.signal.toSeq)

  private def dropTooSmallBins(candidateBins: Seq[Bin], maxNumberOfBins: Int, minSupport: Float): Seq[Bin] = {
    assert(minSupport >= 0 && minSupport < 0.5 && minSupport < 1f / maxNumberOfBins)
    val binsToDrop = candidateBins.sorted(Bin.orderingByFreq).filter(_.freq <= dropTreshold(candidateBins, minSupport))
    candidateBins.filter(!binsToDrop.contains(_))
  }


  @tailrec
  private def mergeBinsWithPlanSignals(candidateBins: Seq[Bin], signals: Seq[Int], acc: List[Bin] = List.empty): List[Bin] = {
    def skipMerge(current: Bin, top: Bin) = top.isEquality || signals(candidateBins.indexOf(current)) != 0 || current.isEquality
    (acc, candidateBins) match {
      case (_, Nil) => acc.reverse.sorted
      case (top :: tailAcc, bin :: tail) if (!skipMerge(bin, top)) =>
        mergeBinsWithPlanSignals(tail, signals.tail, bin.mergeInto(top) :: tailAcc)
      case (_, bin :: tail) =>
        mergeBinsWithPlanSignals(tail, signals.tail, bin :: acc)
    }
  }
}

case class SliceAction(block: Block, numberOfSlices: Int) {
  def resultingBlockSize = (block.size.toFloat / numberOfSlices).toInt
}

case class Block(begin: Int, end: Int) {
  def size = end - begin + 1

  def isIn(other: Block) = other.includes(begin) && other.includes(end)

  def includes(index: Int) = begin <= index && index <= end
}

case class PartitionPlan(candidateBins: Seq[Bin], signal: Array[Int], maxNumberOfBins: Int, nTopDensityDifference: Int, sliceActions: List[SliceAction] = List.empty) {

  def tooSparseBinsToDrop(): Seq[Bin] = {
    val signal = PartitionPlan.signal(candidateBins, nTopDensityDifference)
    FindSparseBins.fromNegativeDensitySignals(signal).map { case (_, index) => candidateBins(index) }
  }

  def oneMorePartitionBySize(): Option[PartitionPlan] = {
    val result = if (sliceActions.isEmpty) {
      findLargestBlock(signal) match {
        case Some(block) =>
          val sliceCurrentLargestBlock = SliceAction(block, 2)
          val newSignal = slice(signal, sliceCurrentLargestBlock)
          Some(PartitionPlan(candidateBins, newSignal, maxNumberOfBins, nTopDensityDifference, sliceCurrentLargestBlock :: Nil))
        case _ => None
      }
    }
    else {
      val modifiedLastAction = sliceActions.last.copy(numberOfSlices = sliceActions.last.numberOfSlices + 1)
      val baseActions = sliceActions.reverse.tail.reverse
      val baseSignal = baseActions.foldLeft(signal)((signal, action) => slice(signal, action))
      findLargestBlock(signal) match {
        case None => Some(copy(signal = slice(baseSignal, modifiedLastAction), sliceActions = modifiedLastAction :: baseActions))
        case Some(currentLargestBlock) if ((currentLargestBlock isIn sliceActions.last.block) // same block or next largest is smaller
          || currentLargestBlock.size < modifiedLastAction.resultingBlockSize) =>
          Some(copy(signal = slice(baseSignal, modifiedLastAction), sliceActions = modifiedLastAction :: baseActions))
        case Some(currentLargestBlock) if currentLargestBlock.size > modifiedLastAction.resultingBlockSize =>
          val sliceCurrentLargestBlock = SliceAction(currentLargestBlock, 2)
          Some(copy(signal = slice(signal, sliceCurrentLargestBlock), sliceActions = sliceCurrentLargestBlock :: sliceActions))
        case _ => None // block of size 1
      }
    }
    result match {
      case Some(plan) if plan.signal.mkString("") != signal.mkString("") => result
      case _ => None
    }
  }

  private def slice(signal: Array[Int], action: SliceAction): Array[Int] = {
    val blockSize = action.block.size
    if (action.numberOfSlices > math.ceil(blockSize / 2))
      signal
    else {
      val increment = math.ceil(blockSize.toFloat / action.numberOfSlices).toInt
      val indexesToSignal = (action.block.begin to action.block.end by increment).toList.tail
      val indexesToSignal2 = if (action.block.begin != 0 && signal(action.block.begin - 1) == 0) action.block.begin :: indexesToSignal else indexesToSignal
      signal.zipWithIndex.map { case (value, idx) =>
        if (!action.block.includes(idx)) value else if (indexesToSignal2.contains(idx)) 2 else 0
      }
    }
  }

  private def findLargestBlock(signal: Array[Int]): Option[Block] = {
    (FindLongestConsecutive.sameValue(signal), FindLongestConsecutive.increasingByOne(signal)) match {
      case (Some(uniform), Some(exponential)) => if (uniform._1 > exponential._1)
        Some(Block(uniform._2, uniform._3)) else Some(Block(exponential._2, exponential._3))
      case (Some(uniform), None) => Some(Block(uniform._2, uniform._3))
      case _ => None
    }
  }
}

object PartitionPlan {

  def apply(candidateBins: Seq[Bin], nTopDensityDifference: Int, maxNumberOfBins: Int): PartitionPlan = {
    PartitionPlan(candidateBins, signal(candidateBins, nTopDensityDifference), maxNumberOfBins, nTopDensityDifference)
  }

  def signal(candidateBins: Seq[Bin], nTopDensityDifference: Int): Array[Int] = {
    assert(nTopDensityDifference > 0 && nTopDensityDifference <= candidateBins.size)
    val distances = candidateBins.map(_.distance)
    val orderedDistances = candidateBins.map(_.distance).sorted.reverse
    val threshold = orderedDistances(nTopDensityDifference)
    val topKDistanceSignal = distances.map { case value => if (value <= threshold) 0 else -(orderedDistances.indexOf(value) + 1) }.toArray
    topKDistanceSignal.zipWithIndex.map { case (value, index) => if (candidateBins(index).isEquality) 1 else value }
  }
}

object FindLongestConsecutive {

  def sameValue(data: Array[Int]): Option[(Int, Int, Int)] = findLongestConsecutiveSeq(data)((a: Int, b: Int) => a == b)

  def findLongestConsecutiveSeq(data: Array[Int])(compare: (Int, Int) => Boolean): Option[(Int, Int, Int)] = {
    if (data.nonEmpty) {
      val result = data.zipWithIndex.foldLeft(Result(data.head, 0, compare))((result, valueAndIndex) => result.process(valueAndIndex))
      if (result.max > 0)
        if (result.prevIndex - result.max < 0)
          Some(result.max, 0, result.prevIndex)
        else
          Some((result.max + 1, result.prevIndex - result.max, result.prevIndex))
      else None
    }
    else None
  }

  def increasingByOne(data: Array[Int]): Option[(Int, Int, Int)] = findLongestConsecutiveSeq(data)((a: Int, b: Int) => a == b + 1)

  case class Result(prevVal: Int, prevIndex: Int = (0), patternMatch: (Int, Int) => Boolean, curMax: Int = 0, max: Int = 0) {
    def process(valueAndIndex: (Int, Int)) = {
      val newRunningMax = if (patternMatch(valueAndIndex._1, prevVal)) curMax + 1 else 0
      val newIndex = if (newRunningMax + 1 > max) valueAndIndex._2 else prevIndex
      // println(s"prev $prevVal, preIdx $prevIndex, newIdx $newIndex, currMax $curMax, max $max")
      Result(valueAndIndex._1, newIndex, patternMatch, newRunningMax, Math.max(max, newRunningMax))
    }
  }

}

object FindSparseBins {

  def fromNegativeDensitySignals(data: Array[Int]): List[(Int, Int)] = findStartingIndexesOfConsecutiveIncreasingNegativeNumbers(data)

  def findStartingIndexesOfConsecutiveIncreasingNegativeNumbers(data: Array[Int]): List[(Int, Int)] = {
    if (data.nonEmpty) {
      val result = data.zipWithIndex.reverse.foldLeft(Result(data.head, 0, List.empty[(Int, Int)]))((result, valueAndIndex) => result.process(valueAndIndex))
      result.acc.reverse
    }
    else List.empty
  }

  case class Result(prevVal: Int, prevIndex: Int = (0), acc: List[(Int, Int)] = List.empty) {

    def process(valueAndIndex: (Int, Int)) = {
      val (currValue, currIndex) = valueAndIndex
      if (currIndex != 0 && currValue < 0 && acc.isEmpty) Result(currValue, currIndex, valueAndIndex :: acc)
      else if (currIndex != 0 && currValue < 0 && currValue != prevVal - 1) Result(currValue, currIndex, valueAndIndex :: acc)
      else Result(currValue, currIndex, acc)
    }
  }

}

