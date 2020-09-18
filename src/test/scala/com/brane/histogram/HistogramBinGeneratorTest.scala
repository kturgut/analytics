package com.brane.histogram

import org.scalatest.flatspec.AnyFlatSpec


case class HistogramTestCase (data:List[Long], message:String, expected:String , maxNumberOfBins:Int =  7, minSupport:Float = 0.05f)

class HistogramBinGeneratorTest extends AnyFlatSpec {

  assert (FindSparseBins.fromNegativeDensitySignals(Array(0,0,0,0,0,-4,-3,-2,0,-1)) == List((-1,9), (-2,7)))

  private val histogramBinGeneratorUseCases = Seq(
    HistogramTestCase(List(1,1,1,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6,8,8,20,20,1000,1001,10001),
      "partition data first where density is low and then evenly distribute uniform distributed data when no high frequency values exist",
      "[1.0,4.0):9 , [4.0,6.0):6 , [6.0,8.0):3 , [8.0,20.0):2 , [20.0,1001.0):3 , [1001.0,10001.0]:2")   ,
     HistogramTestCase(List(1,1,1,1,1,2,2,3,3,3,4,4,5,5,5,6,6,7,7,1000,1000,1000,1000,110001),
      "drop bins with less than minSupport and create single equality bins high frequency values when maxBins = 7",
      "[1.0,1.0]:5 , (1.0,3.0):2? , [3.0,5.0):5 , [5.0,7.0):5 , [7.0,1000.0):2 , [1000.0,1000.0]:4") ,
    HistogramTestCase(List(1,1,1,1,1,2,2,3,3,3,4,4,5,5,5,6,6,7,7,1000,1000,1000,1000,110001),
      "drop bins with less than minSupport and create single equality bins high frequency values when maxBins = 5 and minSupport is 5%",
      "[1.0,1.0]:5 , (1.0,4.0):5? , [4.0,7.0):7 , [7.0,1000.0):2 , [1000.0,110001.0]:5", 5) ,
    HistogramTestCase(List(1,1,1,1,1,2,2,3,3,3,4,4,5,5,5,6,6,7,7,1000,1000,1000,1000,110001),
       "drop bins with less than minSupport and create single equality bins high frequency values when maxBins = 5 and minSupport is 10%",
       "[1.0,1.0]:5 , [3.0,5.0):5 , [5.0,7.0):5 , [1000.0,110001.0]:5", 5, 0.1f)  ,
     HistogramTestCase(List(0,0,0,0,0,0,0,0,0,0,0,0,0,10,10,50,100,200,200,300,300,500,500,700,700,1000),
     "when fifty percent is one value, the rest is uniformly distributed",
     "[0.0,0.0]:13 , (0.0,100.0):3? , [200.0,300.0):2 , [300.0,500.0):2 , [500.0,700.0):2 , [700.0,1000.0]:3") ,
     HistogramTestCase(List(1,2,2,2,2,2,3,4,4,4,4,4,5,6,6,6,6,6,7,8,8,8,8,8,9,10,10,10,10,10,11,12,12,12,12,12,13,14,14,14,14,14,15,16,16,16,16,16),
      "when too many high frequency values that are below equality bin threshold small gap between",
      "[1.0,4.0):7 , [4.0,7.0):11 , [7.0,10.0):7 , [10.0,13.0):11 , [13.0,16.0]:12",5,0.1f),
      HistogramTestCase(List(2,2,2,2,2,4,4,4,4,4,6,6,6,6,6,8,8,8,8,8,10,10,10,10,10,12,12,12,12,12,14,14,14,14,14,16,16,16,16,16),
          "many high frequency values no gap between",
          "[2.0,8.0):15 , [8.0,14.0):15 , [14.0,16.0]:10", 5,0.1f)  ,
      HistogramTestCase(List(1,1,1,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6,8,8,9,9,100,505,1000,1001),
          "80 percent below 10 20 percent exponential",
          "[1.0,4.0):9 , [4.0,6.0):6 , [6.0,9.0):5 , [9.0,505.0):3 , [505.0,1001.0]:3",5,0.1f),
    HistogramTestCase(List(1,2,3,4,5,6,7,8,9,10,100,101,102,103,104,105),
      "divide biggest groups evenly",
      "[1.0,4.0):3 , [4.0,7.0):3 , [7.0,10.0):3 , [10.0,103.0):4 , [103.0,105.0]:3",5), // ,
    HistogramTestCase((1 to 20).map(math.pow(2, _).toLong ).toList,
      "2 power of n up to 20",
      "[4.0,32.0):3 , [32.0,256.0):3 , [256.0,2048.0):3 , [2048.0,16384.0):3 , [16384.0,131072.0):3 , [131072.0,1048576.0]:4"),
    HistogramTestCase((fibs take 25).toList,
      "25 fibonacci",
      "[0.0,3.0):4 , [3.0,34.0):5 , [34.0,377.0):5 , [377.0,4181.0):5 , [4181.0,46368.0]:6",5,0.1f)
  )

  lazy val fibs: Stream[Long] = 0l #:: 1l #:: fibs.zip(fibs.tail).map { n => n._1 + n._2 }

  histogramBinGeneratorUseCases.foreach { testCase =>
    "HistogramBinGenerator" should testCase.message in {
      val actualBins = HistogramBinGenerator.createBins(testCase.data, testCase.maxNumberOfBins, testCase.minSupport)
      //println ("Input data:" + testCase.data)
      if (actualBins.toString!=s"List(${testCase.expected} )" ) {
        println(s"Actual: $actualBins")
      }
      assert(actualBins.toString == s"List(${testCase.expected} )")
    }
  }

  it should "find longest consecutive value ranges " in {
    assert (
      FindLongestConsecutive.sameValue(Array(0,0,0,2,0,2,0,0,0,-1,0,0,0,0)) == Some((4, 10, 13)) &&
    FindLongestConsecutive.sameValue(Array(0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1)) == Some((5, 0, 4)) &&
    FindLongestConsecutive.sameValue(Array(1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1)) == Some((4, 1, 4)) &&
    FindLongestConsecutive.sameValue(Array(1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 1)) == Some((3, 8, 10)) &&

    FindLongestConsecutive.sameValue(Array(0, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0, 1)) == Some((3, 2, 4)) &&
    FindLongestConsecutive.increasingByOne(Array(0, 1, 2, 0, -5, -4, -3, -2, 0, -1)) == Some((4, 4, 7)) &&
    FindLongestConsecutive.increasingByOne(Array(9, 1, 2, 0, -5, -4, -3, -2, 0, -1)) == Some((4, 4, 7)) &&
    FindLongestConsecutive.increasingByOne(Array(0, 1, 2, 0, -5, -4, 0, -2, 0, -1)) == Some((3, 0, 2)) &&
    FindLongestConsecutive.increasingByOne(Array(10, 0, 1, 2, 0, -5, -4, 0, -2, 0, -1)) == Some((3, 1, 3)) &&
    FindLongestConsecutive.increasingByOne(Array(10, 0, 7, 3, 0, -5, 6, 0, -2, 0, -1)) == None &&
    FindLongestConsecutive.sameValue(Array(10, 0, 7, 3, 0, -5, 6, 0, -2, 0, -1)) == Some(1, 0, 0) )
  }

  it should "find index of sparse bins" in {
    assert (
    FindSparseBins.fromNegativeDensitySignals(Array(0,0,0,0,0,-4,-3,-2,0,-1)) == List((-1,9), (-2,7)) &&
      FindSparseBins.fromNegativeDensitySignals(Array(0,0,0,0,0,-4,-3,-2,0,1)) == List((-2,7)) &&
      FindSparseBins.fromNegativeDensitySignals(Array(-4,-3,-2,0,-1)) == List((-1,4), (-2,2))
    )

  }

}