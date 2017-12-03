
package org.sparkstats

trait TestResult[DF] {

  /**
    * The probability of obtaining a test statistic result at least as extreme as the one that was
    * actually observed, assuming that the null hypothesis is true.
    */
  def pValue: Double

  /**
    * Returns the degree(s) of freedom of the hypothesis test.
    * Return type should be Number(e.g. Int, Double) or tuples of Numbers for toString compatibility.
    */
  def degreesOfFreedom: DF

  /**
    * Test statistic.
    */
  def statistic: Double

  /**
    * Null hypothesis of the test.
    */
  def nullHypothesis: String

  /**
    * String explaining the hypothesis test result.
    * Specific classes implementing this trait should override this method to output test-specific
    * information.
    */
  override def toString: String = {

    // String explaining what the p-value indicates.
    val pValueExplain = if (pValue <= 0.01) {
      s"Very strong presumption against null hypothesis: $nullHypothesis."
    } else if (0.01 < pValue && pValue <= 0.05) {
      s"Strong presumption against null hypothesis: $nullHypothesis."
    } else if (0.05 < pValue && pValue <= 0.1) {
      s"Low presumption against null hypothesis: $nullHypothesis."
    } else {
      s"No presumption against null hypothesis: $nullHypothesis."
    }

    s"degrees of freedom = ${degreesOfFreedom.toString} \n" +
      s"statistic = $statistic \n" +
      s"pValue = $pValue \n" + pValueExplain
  }
}

class WilcosonTestResult private[sparkstats](
                                              override val pValue: Double,
                                              override val statistic: Double,
                                              override val nullHypothesis: String) extends TestResult[Int] {

  override val degreesOfFreedom = 0

  override def toString: String = {
    "Wilcoson test summary:\n" + super.toString
  }
}