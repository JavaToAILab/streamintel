package io.github.javatoailab.streamintel.detect;

import java.util.Arrays;

/** Fixed-size ring buffer of recent values with median and median absolute deviation. */
final class RollingWindow {

    private final double[] values;
    private int size;
    private int next;

    RollingWindow(int capacity) {
        this.values = new double[capacity];
    }

    void add(double v) {
        values[next] = v;
        next = (next + 1) % values.length;
        size = Math.min(size + 1, values.length);
    }

    int size() {
        return size;
    }

    /** Returns {median, MAD} of the current contents. O(n log n), n = window size (hundreds). */
    double[] medianAndMad() {
        double[] copy = Arrays.copyOf(values, size);
        double median = median(copy);
        for (int i = 0; i < copy.length; i++) {
            copy[i] = Math.abs(copy[i] - median);
        }
        return new double[]{median, median(copy)};
    }

    private static double median(double[] a) {
        Arrays.sort(a);
        int n = a.length;
        return n % 2 == 1 ? a[n / 2] : (a[n / 2 - 1] + a[n / 2]) / 2.0;
    }
}
