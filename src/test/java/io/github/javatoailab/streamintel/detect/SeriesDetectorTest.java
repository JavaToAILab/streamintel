package io.github.javatoailab.streamintel.detect;

import io.github.javatoailab.streamintel.ingest.MetricPoint;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class SeriesDetectorTest {

    private static SeriesDetector detector(Direction d) {
        return new SeriesDetector(240, 60, 6.0, d, 0.25, 3, 5);
    }

    @Test
    void ordinaryNoiseNeverAlerts() {
        SeriesDetector d = detector(Direction.BOTH);
        Random r = new Random(1);
        int alerts = 0;
        for (int i = 0; i < 20_000; i++) {
            if (d.observe(200 + r.nextGaussian() * 15).isPresent()) {
                alerts++;
            }
        }
        assertThat(alerts).isZero();
    }

    @Test
    void aSustainedSpikeIsConfirmedOnItsThirdPoint() {
        SeriesDetector d = detector(Direction.UP);
        Random r = new Random(2);
        for (int i = 0; i < 100; i++) {
            d.observe(200 + r.nextGaussian() * 15);
        }
        assertThat(d.observe(1600)).isEmpty();
        assertThat(d.observe(1650)).isEmpty();
        double[] hit = d.observe(1620).orElseThrow();
        assertThat(hit[0]).isGreaterThan(6.0);           // z-score
        assertThat(hit[1]).isBetween(190.0, 210.0);      // baseline is still ~200
    }

    @Test
    void aSingleGlitchDoesNotPage() {
        SeriesDetector d = detector(Direction.BOTH);
        Random r = new Random(3);
        for (int i = 0; i < 100; i++) {
            d.observe(200 + r.nextGaussian() * 15);
        }
        assertThat(d.observe(5000)).isEmpty();
        for (int i = 0; i < 10; i++) {
            assertThat(d.observe(200 + r.nextGaussian() * 15)).isEmpty();
        }
    }

    @Test
    void directionIsRespected() {
        SeriesDetector up = detector(Direction.UP);
        for (int i = 0; i < 100; i++) {
            up.observe(200 + (i % 7));
        }
        for (int i = 0; i < 5; i++) {
            assertThat(up.observe(20)).isEmpty();   // latency dropping is good news
        }
    }

    @Test
    void aLongIncidentDoesNotBecomeTheNewNormal() {
        SeriesDetector d = detector(Direction.UP);
        Random r = new Random(4);
        for (int i = 0; i < 100; i++) {
            d.observe(200 + r.nextGaussian() * 15);
        }
        int flagged = 0;
        for (int i = 0; i < 300; i++) {
            if (d.observe(1500 + r.nextGaussian() * 50).isPresent()) {
                flagged++;
            }
        }
        assertThat(flagged).isGreaterThan(295);
    }

    @Test
    void aFlatSeriesStillDetectsARealMove() {
        SeriesDetector d = detector(Direction.BOTH);
        for (int i = 0; i < 100; i++) {
            d.observe(50);
        }
        d.observe(80);
        d.observe(80);
        assertThat(d.observe(80)).isPresent();
    }

    @Test
    void throughputIsComfortablyHigh() {
        DetectorRegistry registry = new DetectorRegistry(m -> detector(Direction.BOTH));
        Random r = new Random(5);
        Instant t = Instant.parse("2026-11-22T00:00:00Z");
        int n = 500_000;
        long start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            registry.observe(new MetricPoint("svc" + (i % 50), "latency", 200 + r.nextGaussian() * 15, t));
        }
        double perSecond = n / ((System.nanoTime() - start) / 1e9);
        System.out.printf("detector throughput: %.0f points/s on one thread, 50 series%n", perSecond);
        assertThat(perSecond).isGreaterThan(20_000);
    }
}
