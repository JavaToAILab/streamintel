package io.github.javatoailab.streamintel.detect;

import io.github.javatoailab.streamintel.ingest.MetricPoint;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** One detector per series, created on first sight with the settings for that metric name. */
public final class DetectorRegistry {

    private final Map<String, SeriesDetector> detectors = new ConcurrentHashMap<>();
    private final Function<String, SeriesDetector> factory;

    public DetectorRegistry(Function<String, SeriesDetector> factoryByMetric) {
        this.factory = factoryByMetric;
    }

    public Optional<Anomaly> observe(MetricPoint p) {
        SeriesDetector d = detectors.computeIfAbsent(p.seriesKey(), k -> factory.apply(p.metric()));
        return d.observe(p.value()).map(r -> new Anomaly(p.service(), p.metric(), p.value(), r[1], r[0], p.timestamp()));
    }

    public int seriesCount() {
        return detectors.size();
    }
}
