package io.github.javatoailab.streamintel.detect;

/** Which deviations matter for a metric: latency and errors only matter when they go up. */
public enum Direction {
    UP,
    DOWN,
    BOTH
}
