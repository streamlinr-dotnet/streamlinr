# Streamlinr Constitution

## Mission

Streamlinr is a native stream processing framework for .NET.

It is inspired by the capabilities of Kafka Streams, but is not an implementation or port of Kafka Streams. Streamlinr exists to provide first-class stream processing primitives that feel natural to modern .NET developers.

Every design decision should favour idiomatic .NET development over compatibility with Java APIs, Java implementation details, or existing Kafka Streams abstractions.

Streamlinr is first and foremost a distributed systems framework. Stream processing is merely the domain in which it operates.

---

## Core Principles

### 1. Native .NET

Streamlinr should embrace modern .NET development practices and platform capabilities.

Examples include:

* Generic Host
* Dependency Injection
* OpenTelemetry
* Async/Await
* Nullable Reference Types
* Records
* Source Generators
* Modern Metrics APIs

Compatibility with Java APIs is not a goal.

A Streamlinr application should feel like a .NET application that happens to process streams, not a Java application translated into C#.

---

### 2. Operational Correctness

Stream processing is fundamentally a distributed systems problem.

The primary engineering concerns of Streamlinr are:

* State recovery
* Partition assignment
* Rebalancing
* Fault tolerance
* Backpressure
* Event-time processing
* Delivery guarantees

These concerns take precedence over API elegance, feature count, or compatibility with existing frameworks.

A feature is not complete when it works under ideal conditions. A feature is complete when its behaviour under failure is understood, observable, documented, and testable.

---

### 3. Explicit Design

Framework behaviour should be visible and understandable.

Avoid:

* Hidden side effects
* Reflection-heavy runtime magic
* Implicit topology construction
* Surprising lifecycle behaviour

APIs should be:

* Consistent
* Predictable
* Difficult to misuse

If two designs provide equivalent capabilities, prefer the one that is easier to understand and reason about.

---

### 4. Observability

Operational visibility is a core capability, not an afterthought.

Every significant component should expose appropriate diagnostics, including:

* Metrics
* Structured logs
* Traces
* Health information

A production operator should be able to understand what the system is doing, why it is doing it, and how it is behaving under failure.

---

### 5. Testability

Public abstractions should be designed with testing in mind.

Developers should be able to:

* Test topologies
* Test stateful processors
* Simulate time
* Simulate failures
* Verify outputs deterministically

without requiring a running Kafka cluster.

---

### 6. Stream Processing, Not Kafka

Streamlinr models stream processing concepts rather than Kafka internals.

Prefer concepts such as:

* Streams
* Tables
* Windows
* Aggregations
* State Stores

Avoid exposing broker implementation details, protocol concerns, or internal topic mechanics unless there is a compelling operational reason to do so.

Architecture tests enforce this boundary: public Streamlinr APIs must model stream processing concepts and must not expose Kafka client types or private runtime implementation details.

The conceptual model should remain portable wherever practical.

---

### 7. Learn From Rx, Do Not Become Rx

The compositional style of Rx.NET is a valuable source of inspiration.

Streamlinr should favour:

* Fluent composition
* Declarative processing pipelines
* Familiar LINQ-style concepts
* Strong typing
* Composable operators

However, Streamlinr is not an in-memory reactive framework.

Operational correctness takes precedence over API elegance. A simple abstraction that behaves correctly under failure is preferable to an elegant abstraction that obscures distributed system behaviour.

When evaluating API designs, ask:

> How would an experienced .NET developer expect this capability to be expressed?

before asking:

> How does Kafka Streams expose this capability?

---

## Non-Goals

Streamlinr is not:

* A line-for-line Kafka Streams clone
* A Java compatibility layer
* A research project
* A framework built around runtime magic
* A framework that prioritises cleverness over maintainability

---

## Guidance for AI Contributors

When proposing changes:

1. Prefer idiomatic .NET patterns.
2. Prioritise clarity over abstraction.
3. Prioritise operational correctness over feature completeness.
4. Consider failure modes before adding functionality.
5. Avoid introducing Java concepts unless they solve a genuine problem.
6. Explain trade-offs explicitly.
7. Challenge designs that exist solely for Kafka Streams compatibility.
8. Treat architecture test failures as design feedback, not test noise. Public APIs must not expose `Streamlinr.Core` implementation types, Kafka client types, actor protocols, mailboxes, discriminated unions, or other runtime details. Fix leaks by introducing Streamlinr-owned public abstractions and translating internally.

The goal is not to recreate Kafka Streams.

The goal is to build the stream processing framework that .NET developers would design today if Kafka Streams had never existed.

A polished API cannot compensate for unreliable behaviour. Streamlinr succeeds or fails based on its correctness, recoverability, and operational characteristics under production workloads.
