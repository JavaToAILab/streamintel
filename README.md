# StreamIntel

**Real-time anomaly detection on Kafka metric streams, with related anomalies grouped into incidents and plain-English alerts written by an LLM. If the LLM is down or wrong, the alert still goes out.**

Services publish metrics to Kafka. StreamIntel watches every series and learns what normal looks like. When something breaks, it decides which deviations belong to the same problem and sends *one* alert:

> **[CRITICAL] INC-7 escalated: payments, checkout**
> Payments p99 latency is at 1620 ms against a 180 ms baseline, and checkout, which calls payments, rose to 704 ms four seconds later. Check the payments database and its connection pool first.
> • payments latency_p99_ms: peak 1620 vs baseline 180 (+800.0%, z=41.2), first seen 14:02:10Z, 9 anomalous points
> • checkout latency_p99_ms: peak 704 vs baseline 220 (+220.0%, z=9.8), first seen 14:02:14Z, 6 anomalous points

The structured lines are always there. The prose on top is the model's, and only after it has been checked.

It is part of [JavaToAILab](https://github.com/JavaToAILab).

## Pipeline

```
services ──▶ Kafka streamintel.metrics (keyed service/metric: one series per partition, in order)
                │  batch consumer, 3 threads
                ▼
         per-series detector ── robust z = (x − median) / (1.4826 · MAD) over the last 240 points
                │                 · ≥ 60 points of history · |z| ≥ 6 and ≥ 25% from the median
                │                 · 3 of the last 5 points beyond the threshold · direction-aware
                ▼
         incident correlator ── anomalies within 2 min join one incident, across services
                │                 OPENED · ESCALATED (new service or severity) · RESOLVED (quiet 5 min)
                ▼
         alert writer ───────── LLM summary → grounding checks → or deterministic template
                │
                ▼
         Kafka streamintel.alerts · Slack-compatible webhook · GET /alerts · Prometheus
```

## Design decisions

**Robust statistics, not mean and standard deviation.** The median and median absolute deviation aren't pulled around by the outliers they're meant to catch. Anomalous points are kept out of the baseline, so a 30-minute outage doesn't become the new normal halfway through, and in the tests a 300-point incident is still flagged on more than 295 of its points. A completely flat series (MAD of 0) falls back to 1% of its level, so a real move still registers.

**Quiet by default.**
- **No false alarms on noise:** 20,000 points of plain noise produce zero alerts (a test).
- **No paging on one glitch:** a single bad scrape doesn't page, because 3 of the last 5 points must agree.
- **Direction matters:** latency falling or traffic rising is not an incident (`directions:` in config).

**One incident, few messages.** Anomalies across services within the correlation window form one incident. Notifications go out only when it opens, when it spreads to another service or gets worse, and when it resolves. Services are listed in event-time order, so the one that degraded first, usually closest to the cause, comes first.

**The LLM explains; it never decides.** Detection, correlation and severity are deterministic. The model only gets the structured facts and is asked for a 2–4 sentence summary. Its reply is checked before anyone sees it:
- it must name an affected service, and must not name any known service that isn't part of this incident;
- every number above 10 must match a number in the facts (within 2%), so an invented latency can't reach a pager.

If the model fails, times out (8 seconds) or fails a check, a deterministic template is used. `writtenBy` on each alert says which, and Prometheus counts both.

**Never blocks the stream.** Alerts are written on virtual threads, off the consumer. Each incident's alerts are chained, so OPENED is always delivered before ESCALATED, while different incidents proceed in parallel. Malformed records are counted and skipped.

## Run it

```bash
docker compose up --build              # StreamIntel + Kafka, with demo traffic for 3 services
curl -X POST localhost:8080/demo/incident   # slow the payments database for 2 minutes
curl -s localhost:8080/alerts | jq
curl -s localhost:8080/incidents | jq
curl -s localhost:8080/actuator/prometheus | grep streamintel_
```

For model-written summaries, point it at any OpenAI-compatible endpoint (Ollama, OpenAI, or [LLM-Gateway](https://github.com/JavaToAILab/llm-gateway)):

```bash
STREAMINTEL_LLM_ENABLED=true STREAMINTEL_LLM_BASE_URL=http://host.docker.internal:11434 docker compose up
```

To post alerts to Slack, set `STREAMINTEL_WEBHOOK_URL` to an incoming-webhook URL.

### Publishing your own metrics

Send JSON to `streamintel.metrics`, keyed by `service/metric`:

```json
{"service": "payments", "metric": "latency_p99_ms", "value": 182.4, "timestamp": "2026-11-22T14:00:00Z"}
```

## Metrics

| Metric | |
|---|---|
| `streamintel_points_total` / `_points_rejected_total` | Ingested and malformed points |
| `streamintel_anomalies_total` | Confirmed anomalous points |
| `streamintel_incidents_open` | Open incidents |
| `streamintel_alerts_total{change,written_by}` | Alerts sent, by type and by whether the LLM or the template wrote them |
| `streamintel_alert_failures_total` | Delivery failures |
| `streamintel_series` | Series being tracked |
| `streamintel_batch_seconds` | Time per consumed batch |

## Performance

The detector recomputes median and MAD every 10 accepted points rather than on every point (the baseline moves slowly). The throughput test measured about **420,000 points/second on one thread** across 50 series in this build environment, and consumption scales with partitions and `consumer-concurrency`. This is a single-machine figure, not a cluster benchmark.

## Limits and roadmap

- **State is in memory.** After a restart, each series needs `min-samples` points before it can alert again. Kafka Streams state stores would make baselines durable.
- **No seasonality yet.** A daily traffic dip will look anomalous if it's sharp enough. Next: per-hour-of-week baselines.
- **Correlation is time-based only.** A service dependency graph would sharpen "which caused which".
- **Read API has no authentication.** Run it behind your gateway.

## Build and test

```bash
mvn verify
```

17 tests, none needing Docker. Unit tests cover:
- the detector: zero false positives over 20,000 noisy points, spike confirmation on the third point, single-glitch suppression, direction, a long incident staying anomalous, flat series, and throughput;
- correlation: grouping, escalation, resolution, peak tracking;
- the grounding checks: an invented number, an unaffected service, a failing model, a missing model.

The end-to-end test runs an **in-process Kafka broker** with a fake LLM and webhook. It publishes baseline traffic for three services, malformed records, then a payments slowdown that spreads to checkout, and checks:
- exactly one OPENED and one ESCALATED alert, both written by the LLM;
- services in event-time order, and nothing about the healthy inventory service;
- webhook delivery;
- the open incident;
- the Prometheus counters.
