package org.sparkstats

import org.apache.spark.rdd.RDD
import org.sparkstats.test.AndersonTest



object Statistics {

  /**
    * Description
    * For more information on Anderson:
    * @see <a href="https://en.wikipedia.org/wiki/Anderson%E2%80%93Darling_test">
    * Anderson–Darling test test (Wikipedia)</a>
    *
    * @param data an `RDD[Double]` containing the sample of data to test
    * @return [[org.sparkstats.AndersonTestResult]] object containing test
    *         p-value, statistic, and null hypothesis.
    */
  def adnersonTest(data: RDD[Double])
  : AndersonTestResult = {
    AndersonTest.testOneSample(data)
  }

}
