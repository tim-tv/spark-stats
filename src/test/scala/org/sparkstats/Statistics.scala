
object Statistics {

  /**
    * Convenience function to conduct a one-sample, two-sided Kolmogorov-Smirnov test for probability
    * distribution equality. Currently supports the normal distribution, taking as parameters
    * the mean and standard deviation.
    * (distName = "norm")
    * @param data an `RDD[Double]` containing the sample of data to test
    * @param distName a `String` name for a theoretical distribution
    * @param params `Double*` specifying the parameters to be used for the theoretical distribution
    * @return [[org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult]] object containing test
    *        statistic, p-value, and null hypothesis.
    */
  @Since("1.5.0")
  @varargs
  def kolmogorovSmirnovTest(data: RDD[Double], distName: String, params: Double*)
  : KolmogorovSmirnovTestResult = {
    KolmogorovSmirnovTest.testOneSample(data, distName, params: _*)
  }
}