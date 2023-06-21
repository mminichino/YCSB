package site.ycsb.generator;

import org.apache.commons.math3.distribution.BetaDistribution;

/**
 * Beta Distribution Generator.
 */
public class BetaGenerator {
  private final long max;
  private final BetaDistribution dist;

  public BetaGenerator betaGeneratorFromMean(long mean, long variance, long maxV) {

    double v1 = mean * (1 - mean);
    double v2 = v1 / variance;
    double alpha = mean * (v2 - 1);
    double k = (double) (1 - mean) / mean;
    double beta = k * alpha;
    long alphaL = Double.valueOf((alpha < 0 ? -alpha : alpha)).longValue();
    long betaL = Double.valueOf((beta < 0 ? -beta : beta)).longValue();
    return new BetaGenerator(alphaL, betaL, maxV);
  }

  public BetaGenerator(long alpha, long beta, long max) {
    this.max = max;
    this.dist = new BetaDistribution((double) alpha, (double) beta);
  }

  public double getMean() {
    return this.dist.getNumericalMean();
  }

  public double getVariance() {
    return this.dist.getNumericalVariance();
  }

  long nextLong() {
    double x = Math.random();
    double b = dist.inverseCumulativeProbability(x);
    double v = b * max;

    return Double.valueOf(v).longValue();
  }

  public Long nextValue() {
    return nextLong();
  }

  public double mean() {
    return getMean();
  }
}
