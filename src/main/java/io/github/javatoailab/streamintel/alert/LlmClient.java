package io.github.javatoailab.streamintel.alert;

/** Minimal chat port. Implementations must respect their own timeout and throw on failure. */
public interface LlmClient {

    String complete(String system, String user);
}
