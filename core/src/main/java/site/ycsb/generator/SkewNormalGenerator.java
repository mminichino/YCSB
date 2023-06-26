package site.ycsb.generator;

/**
 * Skew Normal Generator.
 */
public class SkewNormalGenerator extends NumberGenerator {

  private final double stdev;
  private final double mean;
  private final long alpha;
  private final double loc;
  private final double scale;
  private final long max;
  private static final double DEFAULT_LOC = 0.5;
  private static final double DEFAULT_SCALE = 0.5;
  private static final long DEFAULT_ALPHA = 10;

  public SkewNormalGenerator(long alpha, double loc, double scale, long max) {
    this.alpha = alpha;
    this.loc = loc;
    this.scale = scale;
    this.max = max;
    this.mean = (double) max / 2;
    this.stdev = (double) max / 300;
  }

  public SkewNormalGenerator(long meanFactor, long stdevFactor, long max) {
    this.alpha = DEFAULT_ALPHA;
    this.loc = DEFAULT_LOC;
    this.scale = DEFAULT_SCALE;
    this.max = max;
    this.mean = (double) max / meanFactor;
    this.stdev = (double) max / stdevFactor;
  }

  public SkewNormalGenerator(long stdevFactor, long max) {
    this.alpha = DEFAULT_ALPHA;
    this.loc = DEFAULT_LOC;
    this.scale = DEFAULT_SCALE;
    this.max = max;
    this.mean = (double) max / 2;
    this.stdev = (double) max / stdevFactor;
  }

  public SkewNormalGenerator(long max) {
    this.alpha = DEFAULT_ALPHA;
    this.loc = DEFAULT_LOC;
    this.scale = DEFAULT_SCALE;
    this.max = max;
    this.mean = (double) max / 2;
    this.stdev = (double) max / 300;
  }

  private double randn() {
    double s;
    double v1;
    double v2;

    do {
      double u1 = Math.random();

      v1 = 2 * u1 - 1;
      s = v1 * v1;
    } while (s == 0 || s >= 1);

    double sqrt = Math.sqrt(-2 * Math.log(s) / s);
    v2 = stdev * v1 * sqrt + mean;
    if (v2 >= 0) {
      return v2;
    }
    return -v2;
  }

  private double generate() {
    double u0 = randn();
    double v = randn();
    double sigma = alpha / Math.sqrt(1 + Math.pow(alpha, 2));
    double u1 = (sigma * u0 + Math.sqrt(1 - Math.pow(sigma, 2)) * v);

    return u1 * scale + loc;
  }

  long nextLong() {
    double ret;
    do {
      ret = generate();
    } while (ret < 1 || ret > max);

    return Double.valueOf(ret).longValue();
  }

  public Long nextValue() {
    return nextLong();
  }

  public double mean() {
    return mean;
  }
}
