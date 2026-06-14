# CEP Imbalance – Java (Maven) Project

## Requirements
- JDK 17+
- Maven 3.8+

## Build & Run
```bash
mvn -q -DskipTests package
java -jar target/cep-imbalance-1.0.0-shaded.jar
```

## Run Unit Tests
```bash
mvn -q test
```

This project contains three pipeline implementations:
- Disruptor (LMAX Disruptor)
- Agrona-style SPSC (demo SPSC ring buffer)
- Chronicle Queue (persisted queue)

The load generator publishes to all pipelines and the App prints a p50/p95/p99/p99.9 latency summary after the run.
