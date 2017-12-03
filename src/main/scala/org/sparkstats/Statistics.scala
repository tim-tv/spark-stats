package org.sparkstats
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.sparkstats.test.{ShapiroWilkTest, Wilcoson}

object Statistics {
  /**
    * Description
    * For more information about Wilcoxon signed-rank test:
    * @see <a href="http://en.wikipedia.org/wiki/Wilcoxon_signed-rank_test">
    * Wilcoxon signed-rank test(Wikipedia)</a>
    *
    * @param data an `RDD[Double]` containing the sample of data to test
    * @return [[org.sparkstats.WilcosonTestResult]] object containing test
    *         p-value, statistic, and null hypothesis.
    */
  def wilcoxonTest(data: RDD[Double],sc: SparkContext, correction:Boolean = false)
  : WilcosonTestResult = {
    Wilcoson.testOneSample(data,sc,correction)
  }
}
