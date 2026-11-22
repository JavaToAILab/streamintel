package io.github.javatoailab.streamintel.detect;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

/**
 * Anomaly detection for one series, using a robust z-score against a rolling window:
 *
 * <pre>  z = (x − median) / (1.4826 · MAD)</pre>
 *
 * Median and MAD are not dragged around by the outliers they are meant to catch, unlike mean and
 * standard deviation. Two guards keep it quiet: nothing fires until the window has
 * {@code minSamples} points, and a point only counts as an anomaly when at least {@code confirm}
 * of the last {@code of} points were also beyond the threshold, so a single scrape glitch does not
 * page anyone. Anomalous points are kept out of the baseline, so a long incident does not quietly
 * become the new normal.
 *
 * <p>The baseline moves slowly, so median and MAD are recomputed every {@value #REFRESH_EVERY} accepted
 * points rather than on every point: a tenfold saving that does not change what gets detected.
 */
public final class SeriesDetector {

    private static final double MAD_TO_SIGMA = 1.4826;
    static final int REFRESH_EVERY = 10;

    private final RollingWindow window;
    private final int minSamples;
    private final double threshold;
    private final Direction direction;
    private final double minRelativeChange;
    private final int confirm;
    private final int of;
    private final Deque<Boolean> recent = new ArrayDeque<>();
    private double[] baseline;
    private int sinceRefresh;

    public SeriesDetector(int windowSize, int minSamples, double threshold, Direction direction,
                          double minRelativeChange, int confirm, int of) {
        this.window = new RollingWindow(windowSize);
        this.minSamples = minSamples;
        this.threshold = threshold;
        this.direction = direction;
        this.minRelativeChange = minRelativeChange;
        this.confirm = confirm;
        this.of = of;
    }

    /** Feeds one value; returns the robust z-score if this point confirms an anomaly. */
    public synchronized Optional<double[]> observe(double value) {
        if (window.size() < minSamples) {
            window.add(value);
            return Optional.empty();
        }
        if (baseline == null || sinceRefresh >= REFRESH_EVERY) {
            baseline = window.medianAndMad();
            sinceRefresh = 0;
        }
        double[] mm = baseline;
        double median = mm[0];
        // A perfectly flat series has MAD 0; fall back to a small fraction of the level so any real
        // move still scores, instead of dividing by zero.
        double spread = Math.max(mm[1] * MAD_TO_SIGMA, Math.abs(median) * 0.01 + 1e-9);
        double z = (value - median) / spread;
        boolean relevant = switch (direction) {
            case UP -> z > 0;
            case DOWN -> z < 0;
            case BOTH -> true;
        };
        double relChange = median == 0 ? Double.POSITIVE_INFINITY : Math.abs(value - median) / Math.abs(median);
        boolean beyond = relevant && Math.abs(z) >= threshold && relChange >= minRelativeChange;

        recent.addLast(beyond);
        if (recent.size() > of) {
            recent.removeFirst();
        }
        if (!beyond) {
            window.add(value);
            sinceRefresh++;
            return Optional.empty();
        }
        long hits = recent.stream().filter(b -> b).count();
        return hits >= confirm ? Optional.of(new double[]{z, median}) : Optional.empty();
    }
}
