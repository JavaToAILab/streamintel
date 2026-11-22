package io.github.javatoailab.streamintel.alert;

import io.github.javatoailab.streamintel.incident.Incident;
import io.github.javatoailab.streamintel.incident.IncidentCorrelator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/** The deterministic part of every alert: one line per affected series, worst first. */
public final class AlertFacts {

    private AlertFacts() {
    }

    public static List<String> lines(Incident incident) {
        List<String> out = new ArrayList<>();
        incident.series().stream()
                .sorted(Comparator.comparingDouble((Incident.SeriesImpact s) -> -Math.abs(s.peakScore())))
                .forEach(s -> out.add(String.format(Locale.ROOT,
                        "%s %s: peak %s vs baseline %s (%+.1f%%, z=%.1f), first seen %s, %d anomalous points",
                        s.service(), s.metric(), num(s.peakValue()), num(s.baseline()), s.changePercent(),
                        s.peakScore(), s.firstSeen(), s.anomalies())));
        return out;
    }

    public static String title(Incident i, IncidentCorrelator.Change change) {
        String what = String.join(", ", i.services());
        return switch (change) {
            case RESOLVED -> "[RESOLVED] " + i.id() + " " + what;
            case ESCALATED -> "[" + i.severity() + "] " + i.id() + " escalated: " + what;
            default -> "[" + i.severity() + "] " + i.id() + " anomaly in " + what;
        };
    }

    /** Used whenever the LLM is unavailable or its summary fails validation. */
    public static String template(Incident i, IncidentCorrelator.Change change) {
        List<String> lines = lines(i);
        if (change == IncidentCorrelator.Change.RESOLVED) {
            return "All affected series have been back within normal range since " + i.lastAnomalyAt() + ". "
                    + "Affected: " + String.join(", ", i.services()) + ".";
        }
        return "Detected " + lines.size() + " anomalous series across " + i.services().size() + " service(s). "
                + "Worst: " + lines.getFirst() + ".";
    }

    static String num(double v) {
        if (Math.abs(v) >= 100 || v == Math.rint(v)) {
            return String.format(Locale.ROOT, "%.0f", v);
        }
        return String.format(Locale.ROOT, Math.abs(v) >= 1 ? "%.2f" : "%.4f", v);
    }
}
