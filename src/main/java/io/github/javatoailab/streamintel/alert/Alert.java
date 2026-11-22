package io.github.javatoailab.streamintel.alert;

import java.time.Instant;
import java.util.List;

/**
 * @param writtenBy "llm" when the model's summary passed the grounding checks, otherwise "template"
 * @param facts     the structured lines the summary was written from, always included
 */
public record Alert(String incidentId, String change, String severity, String title, String summary,
                    String writtenBy, List<String> facts, Instant at) {
}
