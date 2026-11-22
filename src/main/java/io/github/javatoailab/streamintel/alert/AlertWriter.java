package io.github.javatoailab.streamintel.alert;

import io.github.javatoailab.streamintel.incident.Incident;
import io.github.javatoailab.streamintel.incident.IncidentCorrelator;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Writes the human part of an alert. The LLM gets only the structured facts and is asked for a short
 * on-call summary: what is happening, how the affected services likely relate, what to check first.
 * Its reply is then checked before anyone sees it:
 * <ul>
 *   <li><b>services:</b> it must name at least one affected service and no known service that is not
 *       part of this incident;</li>
 *   <li><b>numbers:</b> every number above 10 must match a number in the facts (within 2%), so
 *       a made-up latency or error rate cannot reach a pager.</li>
 * </ul>
 * If the model fails, times out or does not pass, the deterministic template is used. An alert is
 * never delayed or lost because of the model.
 */
public final class AlertWriter {

    static final String SYSTEM = """
            You write incident summaries for on-call engineers. Use only the facts given.
            In 2 to 4 sentences: say what is happening, how the affected services likely relate
            (for example one depending on another), and the first thing to check.
            Do not invent services, metrics or numbers. Do not use markdown.""";

    private static final Pattern NUMBER = Pattern.compile("(?<![\\w.])[-+]?\\d+(?:[.,]\\d+)*(?:\\.\\d+)?");

    private final LlmClient llm;
    private final Supplier<Set<String>> knownServices;
    private final Clock clock;

    public AlertWriter(LlmClient llm, Supplier<Set<String>> knownServices, Clock clock) {
        this.llm = llm;
        this.knownServices = knownServices;
        this.clock = clock;
    }

    public Alert write(Incident incident, IncidentCorrelator.Change change) {
        List<String> facts = AlertFacts.lines(incident);
        String title = AlertFacts.title(incident, change);
        String summary = null;
        String by = "template";
        if (llm != null && change != IncidentCorrelator.Change.RESOLVED) {
            try {
                String candidate = llm.complete(SYSTEM, "Incident " + incident.id() + " (" + change + ", "
                        + incident.severity() + ")\nFacts:\n- " + String.join("\n- ", facts)).strip();
                if (grounded(candidate, incident, facts)) {
                    summary = candidate;
                    by = "llm";
                }
            } catch (RuntimeException e) {
                // Fall through to the template.
            }
        }
        if (summary == null) {
            summary = AlertFacts.template(incident, change);
        }
        return new Alert(incident.id(), change.name(), incident.severity().name(), title, summary, by, facts,
                clock.instant());
    }

    boolean grounded(String text, Incident incident, List<String> facts) {
        if (text.isEmpty() || text.length() > 1200) {
            return false;
        }
        String lower = text.toLowerCase(Locale.ROOT);
        Set<String> affected = incident.services();
        if (affected.stream().noneMatch(s -> lower.contains(s.toLowerCase(Locale.ROOT)))) {
            return false;
        }
        for (String other : knownServices.get()) {
            if (!affected.contains(other) && Pattern.compile("\\b" + Pattern.quote(other.toLowerCase(Locale.ROOT)) + "\\b")
                    .matcher(lower).find()) {
                return false;
            }
        }
        List<Double> allowed = numbers(String.join(" ", facts));
        for (double n : numbers(text)) {
            if (Math.abs(n) <= 10) {
                continue; // small counts and ordinals ("2 services", "first")
            }
            boolean matches = allowed.stream().anyMatch(a -> Math.abs(a - n) <= Math.max(0.5, Math.abs(a) * 0.02));
            if (!matches) {
                return false;
            }
        }
        return true;
    }

    static List<Double> numbers(String s) {
        List<Double> out = new ArrayList<>();
        Matcher m = NUMBER.matcher(s.replaceAll("(\\d),(\\d{3})", "$1$2"));
        while (m.find()) {
            try {
                out.add(Double.parseDouble(m.group().replace(",", "")));
            } catch (NumberFormatException ignored) {
                // not a number after all
            }
        }
        return out;
    }
}
