# Streamlinr Technical Design

This document turns the principles in `CONSTITUTION.md` into an initial technical direction. It is not an API contract. Some names and package boundaries will change once code exists, but the major constraints here are intentional.

## Product Shape

Streamlinr is a Kafka-native stream processing framework for .NET. Kafka is the production execution substrate, not one interchangeable adapter among many. Topics, partitions, offsets, consumer groups, rebalances, changelog topics, repartition topics, producer durability, and eventually transactions all matter to the runtime design.

That does not mean application code should look like low-level Kafka code. The public programming model should be streams, tables, windows, processors, state stores, and topologies. Kafka details should surface only when they affect behavior, operations, or configuration.

Non-Kafka broker support is not a project goal. Kafka Connect is the expected bridge for systems outside Kafka.

Streamlinr is inspired by Kafka Streams capabilities, but it is not a Kafka Streams port and does not aim for Java API compatibility.

## Operational Goal

The project should be judged primarily by how it behaves during incidents. A pleasant demo API is useful, but it does not prove the runtime can be operated safely.

At 2:00AM, an operator who has never touched the application should be able to answer four questions quickly:

- What failed?
- What state is the runtime in?
- Is restart or retry safe?
- What action restores service without making the problem worse?

That requirement affects the design of almost every subsystem. Failure behavior, logs, metrics, health state, and recovery procedures are part of the feature, not follow-up documentation.

## Container Runtime Constraint

Streamlinr should work naturally in containerized production environments. It must not assume that a persistent volume is available.

A container may restart on a different node with an empty filesystem. That should be a normal recovery path, not a special disaster case. Persistent volumes can still be useful when available, but they are an optimization for faster local recovery rather than the source of truth.

Kafka-backed durability is the correctness mechanism. Local state is a materialization that may be rebuilt.

## Implementation Split

The project is split into two implementation units.

```text
src/Streamlinr.Core/  # F# private runtime implementation
src/Streamlinr/       # C# public API
```

`Streamlinr.Core.fsproj` contains the runtime. It is written in F# and uses Erlang-style in-process actors. These are local actors with explicit messages, ownership, supervision, and lifecycle boundaries. They are not virtual actors.

`Streamlinr.Core` ships with `Streamlinr` as an implementation assembly, but it is not a supported user-facing API. Its internal types may change between releases.

`Streamlinr.csproj` contains the public API. It is written in C# and owns the declarative topology and configuration surface. Its job is to collect and validate user declarations, then compile them into a private runtime plan consumed by the F# core.

The dependency direction is:

```text
Application code
  -> Streamlinr public C# API
      -> private runtime bridge
          -> Streamlinr.Core F# actor runtime
              -> Confluent.Kafka
```

Two boundaries are non-negotiable:

- F# runtime details must not leak into public APIs. That includes records, discriminated unions, modules, actor messages, actor addresses, mailboxes, and state machine types.
- `Confluent.Kafka` details must not leak into public APIs. That includes config objects, exceptions, records, headers, topic partitions, offsets, handles, and result types.

Public compatibility applies to `Streamlinr`, not `Streamlinr.Core`.

## Kafka Client Boundary

`Streamlinr.Core` is built on `Confluent.Kafka`. It owns Kafka consumers, producers, admin operations, offset handling, rebalance handling, transaction support when added, and error translation.

The C# API should expose Streamlinr-owned configuration and error types. Internally those can be translated to `Confluent.Kafka` configuration, exceptions, and result models. If an advanced Kafka escape hatch becomes necessary, prefer validated key-value settings over public `Confluent.Kafka` objects.

Integration tests may use `Confluent.Kafka` directly for setup or verification when the public API is not sufficient. Product code outside `Streamlinr.Core` should not.

## Serialization And Message Values

Kafka topics often contain more than one logical message type. Streamlinr should model that directly instead of assuming every record on a stream has the same CLR value type. A stream is keyed by `TKey`, but its value is represented by a result-like `StreamValue` case that preserves what happened while interpreting the Kafka record.

Key serialization and value serialization are separate public concepts. Key serializers are typed because Kafka keys are usually simple scalar values used for partitioning, grouping, and joins. Value serializers are non-generic and deserialize using a resolved CLR `Type`, because the value type may vary from record to record within the same topic.

Message type resolution is explicit and metadata-driven. The default resolver may use headers such as `message-type`, but the public API should expose only Streamlinr-owned concepts: `IMessageTypeResolver`, `MessageTypeResolution`, `MessageHeaders`, key serializers, and value serializers. `Confluent.Kafka` headers and serializer types must remain behind the runtime boundary.

Failures at runtime boundaries are explicit policy decisions. Serializer failures and processor failures must declare their behavior at the boundary where the programmer has the relevant domain context.

The first implemented value failure policy is `ValueFailure.ContinueAsDeadLetter()`. It keeps the record in the stream as `StreamValue.DeadLetter` so later processors can inspect, split, route, or eventually write it to a dead-letter topic. This does not mean Streamlinr immediately writes to a Kafka dead-letter topic. It means the failure is represented as stream data. Future value failure policies may include skip, pause partition, fail topology, and external dead-letter output where those behaviors are well defined.

`StreamValue` should distinguish at least these cases:

- `Resolved`: the message type was resolved and deserialization succeeded.
- `Tombstone`: the Kafka value was null.
- `DeadLetter`: value processing failed at a boundary configured to continue as dead-letter data.

The `DeadLetter` payload should include the original key data when available, the failed value data, headers, reason, and optional exception. Its value data should be `System.Object` because different failure boundaries have different available representations. During deserialization failure, the value data will usually be raw `Byte[]`. During later processor failure, the value data may already be a deserialized CLR object.

Unknown message types and deserialization failures are data when the stream declares `ValueFailure.ContinueAsDeadLetter()`. They remain in the stream as explicit `StreamValue.DeadLetter` cases so future split, tee, and dead-letter processors can route them deliberately.

This is intentionally different from exception-driven stream processing. Exceptions are still appropriate for programmer errors, invalid serializer configuration, duplicate type mappings, or other cases where startup or topology construction is wrong. Unknown message types, tombstones, and malformed payloads are ordinary Kafka stream conditions, but continuing after them must still be an explicit value failure policy choice.

## Processor Failure Policy

User-defined processor code is another runtime failure boundary. Processor declarations must explicitly state what should happen if user callback code throws. Processor failure policy describes the terminal outcome after any retry, timeout, or resilience behavior the callback itself applies. If a processor needs Polly or another retry strategy around an external dependency, the callback should own that strategy before allowing an exception to escape to Streamlinr.

The first implemented processor failure policies are `ProcessorFailure.FailTopology()`, `ProcessorFailure.Skip()`, `ProcessorFailure.ContinueAsDeadLetter()`, and `ProcessorFailure.PausePartition()`.

Failing the topology is the safest behavior when correctness is uncertain because it avoids silently advancing offsets past failed processing, hiding bugs, or continuing after a partial state mutation. Skipping a record, continuing as dead-letter data, or pausing a partition are explicit terminal choices for processors whose domain semantics make those outcomes safe. Continuing after user-code failure must always be an explicit choice, never hidden default behavior.

## Public Programming Model

Application developers should work with a small set of concepts:

- Keyed streams whose values are represented by `StreamValue`.
- `Table<TKey, TValue>` for materialized latest-value views.
- `Topology` for the processing graph.
- `Processor` as a user-visible processing concept, not an actor abstraction.
- `StateStore` for named local state backed by Kafka recovery mechanisms.
- `Window` for event-time grouping.
- Key serializers, value serializers, and message type resolvers for explicit serialization boundaries.

An early API could look like this:

```csharp
builder.Services.AddStreamlinr(streams =>
{
    var resolver = new DefaultMessageTypeResolver("message-type") {
        ["widget"] = typeof(Widget)
    };

    streams.Stream<String>(
            "widgets",
            WidgetValueSerializer.Instance,
            resolver,
            failure: ValueFailure.ContinueAsDeadLetter())
        .Peek(async (record, cancellationToken) => {
            if (record.Value is StreamValue.Resolved { Value: Widget widget }) {
                await Console.Out.WriteLineAsync(widget.Name, cancellationToken);
            }
        }, failure: ProcessorFailure.FailTopology());
});
```

The fluent API builds an inspectable topology description. It should not start background work, connect to Kafka, or hide lifecycle behavior during registration.

## Runtime Model

The F# core should make ownership explicit. A task should own a specific Kafka assignment and the state needed to process it. Rebalances should translate into visible lifecycle transitions: pause, checkpoint or flush when required, revoke ownership, assign new work, restore state, then resume.

Likely internal actors include:

- `RuntimeSupervisor`
- `TopologySupervisor`
- `TaskActor`
- `KafkaConsumerActor`
- `KafkaProducerActor`
- `StateStoreActor`
- `CheckpointActor`
- `RebalanceActor`
- `BackpressureActor`

These names are placeholders, not public concepts. The important part is the ownership model: each actor has a narrow responsibility, communicates by explicit messages, and reports state changes in a way tests and diagnostics can observe.

Startup should follow this shape:

1. The host starts.
2. Public topology and configuration are validated.
3. The C# API compiles declarations into a private runtime plan.
4. The F# runtime supervisor starts.
5. Kafka clients connect.
6. Kafka group assignment is acquired.
7. Required state is restored from changelog topics.
8. Task actors begin processing assigned partitions.

Shutdown and rebalance paths are as important as startup. No task should process records before required state recovery completes. Unsafe commit or recovery states should appear in logs, metrics, health checks, and tests.

## Delivery Guarantees

The first implementation should target at-least-once processing. Exactly-once semantics should be treated as a later design phase based on Kafka transactions and idempotent producers, not as an accidental promise created by API wording.

For each supported mode, the project must document the order of:

- output writes
- changelog writes
- local state flushes
- checkpoints
- Kafka offset commits

The unsafe windows between those operations need tests. Examples include output write success followed by offset commit failure, offset commit success followed by a crash before local state flush, and restart on a node with no preserved local state.

## State And Recovery

State needs its own lifecycle and recovery model. Aggregation operators can use that model, but they should not hide it.

Stateful operators should use Kafka changelog topics as the durable recovery source. Local stores are disposable materializations of Kafka-backed state. They may live on a persistent volume, an ephemeral container filesystem, or in memory depending on environment and configuration.

Missing local state after restart should be normal. The runtime should support warm restore from existing local state when a volume is present, cold restore from Kafka changelog topics when local state is missing, and safe rebuild after detecting corrupt local state.

The first production store can still be simple, but the lifecycle needs to be designed around restore, replay, flush, checkpoint, rebalance, container rescheduling, and crash recovery from the beginning.

The state design needs early tests for:

- interrupted restore
- corrupted or missing local state
- duplicate changelog replay
- restart with an empty local filesystem
- process crash during flush
- rebalance during state access
- versioned serialization changes

The first durable local store remains an open choice. RocksDB, SQLite, LMDB, or another .NET-friendly option should be evaluated against restore behavior, operational complexity, and testability.

## Time, Windows, And Repartitioning

Time semantics should be settled before the operator set grows.

The initial model should distinguish event time from processing time. Event time can come from record metadata or a configured extractor. Processing time should use an injectable time source so tests can control it.

Tumbling windows are enough for the first windowing milestone. Hopping, sliding, and session windows should wait until restore, late-event behavior, and failure semantics are proven.

Kafka repartition topics are part of the runtime model when key-changing operations or windowed grouping require them. They should be visible operationally, but they should not dominate the application API.

## Observability

Observability work should be tied to service recovery.

The runtime should expose metrics, structured logs, OpenTelemetry traces, and health checks. Diagnostic events should include useful context when available: topology name, task id, topic, partition, offset, state store, checkpoint id, actor role, and operation.

Health should distinguish at least these states:

- starting
- restoring
- running
- degraded
- draining
- stopped
- fatal

Early metrics should cover record counts, processing latency, Kafka consumer lag, restore progress, checkpoint failures, offset commit failures, rebalance count and duration, retries, dead-letter records, actor failures, and backpressure.

State recovery diagnostics should identify whether the runtime is doing a warm restore from existing local state, a cold restore from Kafka changelogs, or a rebuild after detecting missing or corrupt local state. Operators need to see restore progress and understand whether a slow startup is expected recovery work or a stuck runtime.

Tests should assert diagnostics for failure scenarios. A test that proves the final output is correct but leaves an operator blind is incomplete for this project.

## Testing Strategy

Failure testing belongs in the first working slices of the runtime.

There is still value in a deterministic in-memory harness. It is useful for topology validation, simple operator behavior, fake time, and tightly controlled runtime tests. It is not evidence that Streamlinr behaves correctly with Kafka.

Kafka integration tests using Testcontainers are core infrastructure, not a late-stage suite. Rebalances, offset commits, changelog recovery, broker restarts, producer failures, and partition assignment must be tested against real Kafka brokers.

Test projects should follow the implementation language where practical:

```text
tests/Streamlinr.Core.Tests/         # F# unit tests for runtime internals
tests/Streamlinr.Tests/              # C# unit tests for public API
tests/Streamlinr.IntegrationTests/   # C# Testcontainers Kafka integration tests
```

The early failure suite should include:

- broker restart during processing
- rebalance during in-flight records
- producer timeout or retriable failure
- output write succeeds but offset commit fails
- offset commit succeeds but the process crashes before local state flush
- changelog write succeeds but output write fails
- local state corruption or missing local state
- container restart or reschedule with no persistent volume
- local state directory deleted between runs
- interrupted changelog restore
- value failure and dead-letter handling
- slow output or state store causing backpressure
- shutdown during checkpoint
- network interruption between the app and Kafka
- duplicate records after restart
- misconfiguration with actionable startup failure

Every non-trivial feature should answer these questions before it is considered done:

- What happens when it fails?
- How does an operator know?
- Is retry safe?
- Is restart safe?
- Can state be restored?
- Can output be duplicated?
- Is the adverse condition covered by a test?

## Configuration

Configuration should use normal .NET options and configuration binding.

```csharp
builder.Services.Configure<StreamlinrOptions>(
    builder.Configuration.GetSection("Streamlinr"));
```

Important configuration areas include application identity, Kafka bootstrap and security settings, input and output topics, state directory, local state storage mode, checkpoint interval, delivery guarantee, backpressure limits, retry policy, value failure handling, dead-letter handling, metrics names, tracing names, and shutdown timeout.

The state directory configures the local materialization location only. It must not imply durable ownership of state unless persistence is explicitly available and verified.

Avoid hidden defaults that change correctness. If a default affects recovery, ordering, durability, or loss/duplication behavior, it should be visible in configuration or documentation.

## Versioning And Compatibility

The supported public surface is the C# `Streamlinr` assembly. Compatibility policy should cover public APIs, public configuration, serialization formats, state store formats, topology evolution, changelog compatibility, and internal Kafka topic naming.

`Streamlinr.Core` is an implementation assembly. Its internal F# types, actor messages, and state machine representations may change between releases.

State compatibility needs special care because a bad state migration can become an outage.

## Source Generation

Source generators may be useful later for topology metadata, serialization helpers, compile-time validation hints, or analyzers. They should not be required for the initial runtime unless they clearly improve correctness.

## Analyzers

A future `Streamlinr.Analyzers` project should provide compile-time diagnostics for topology declarations where static analysis can improve reliability. One early analyzer should detect topology cycles and warn or fail when a declared topology is not a directed acyclic graph.

Keeping runtime topologies as DAGs makes validation, scheduling, offset behavior, backpressure, observability, and failure reasoning simpler. Retry feedback loops should be modeled explicitly, such as through Kafka retry topics, rather than by introducing in-memory topology cycles.

## Initial Project Layout

```text
src/Streamlinr.Core/               # F# private runtime implementation assembly
src/Streamlinr/                    # C# public API assembly
src/Streamlinr.Analyzers/          # future C# analyzer assembly for topology diagnostics
tests/Streamlinr.Core.Tests/       # F# runtime unit tests
tests/Streamlinr.Tests/            # C# public API unit tests
tests/Streamlinr.IntegrationTests/ # C# Testcontainers Kafka integration tests
docs/
```

`Streamlinr.Core` ships alongside `Streamlinr`, but application developers reference `Streamlinr`. `Confluent.Kafka` is encapsulated in `Streamlinr.Core` and limited test infrastructure.

## Open Questions

- Is exactly-once a v1 goal or a post-v1 feature?
- Which local store should be used first, given that persistent volumes are optional?
- How much LINQ syntax should be supported versus stream-specific fluent operators?
- What is the smallest operator set that can prove the runtime under failure?

## Implementation Roadmap

### Phase 0: Repository Foundation

Create the mixed C#/F# solution with `Streamlinr.Core.fsproj`, `Streamlinr.csproj`, and the three test projects. Add formatting, analyzers, nullable reference types for C#, CI, and documented build/test commands once executable project files exist.

This phase should also add a small set of architectural tests or build checks that protect dependency boundaries, especially `Confluent.Kafka` and F# runtime types not leaking from the public API.

### Phase 1: Failure-Oriented Test Infrastructure

Before building a polished API, add the testing hooks needed to create adverse conditions. This includes deterministic cancellation, timeouts, fake time, failure injection, diagnostic capture, and assertions over logs, metrics, traces, and health state.

The goal is to make failure tests cheap enough that they appear in the first runtime features rather than after the design has hardened.

### Phase 2: Public API And Private Runtime Plan

Build the first C# topology API and compile it into a private runtime plan for the F# core. Add validation with useful error messages. At this point the runtime plan can be small; the important part is proving the public/private boundary.

Exit criteria: a user can declare a simple topology, invalid topologies fail before startup, and public signatures expose neither F# runtime types nor `Confluent.Kafka` types.

### Phase 3: F# Actor Runtime Skeleton

Implement the supervisor and actor lifecycle primitives in `Streamlinr.Core`. Cover startup, shutdown, cancellation, timeout, failure propagation, and diagnostic reporting with F# unit tests.

This phase should establish how actor ownership and supervision work before Kafka logic makes failures harder to reason about.

### Phase 4: Local Test Harness

Add enough deterministic in-memory execution to test topology behavior and simple operators without Kafka. Include fake time, in-memory sources and sinks, in-memory state, and failure hooks.

This harness is for focused tests. It should not become an alternate production runtime.

### Phase 5: Testcontainers Kafka Infrastructure

Add C# integration test fixtures for Kafka using Testcontainers. The fixtures should handle topic creation, consumer group isolation, cleanup, broker lifecycle, and direct inspection where needed.

The first smoke test should use public Streamlinr APIs against a real Kafka broker. Shortly after, add tests that deliberately restart the broker, interrupt the app during processing, and restart the app with an empty local filesystem.

### Phase 6: Kafka Consumer And Producer Execution

Wrap `Confluent.Kafka` inside internal F# runtime boundaries. Implement consumer and producer actors, controlled polling, pause/resume, produce, flush, and offset tracking.

The first end-to-end milestone is a stateless topology that consumes from Kafka and produces to Kafka. It should already have tests for producer failure, broker restart, shutdown during processing, and retry behavior.

### Phase 7: State Stores And Changelog Recovery

Add state store lifecycle, initial local state, Kafka changelog topics, state registration through topology, and the first stateful operator such as `Aggregate`.

Recovery tests should cover interrupted restore, corrupted local state, missing local state, duplicate changelog replay, container rescheduling without a persistent volume, and rebalance during state access.

### Phase 8: Offset Commit, Checkpointing, And Crash Recovery

Define and implement the ordering between output writes, changelog writes, local state flushes, checkpoints, and Kafka offset commits. Start with at-least-once behavior.

Crash and restart tests are required here. The important cases are the gaps between operations, including restart on a new container with no preserved local state.

### Phase 9: Rebalancing And Task Assignment

Map Kafka group rebalances to actor lifecycle transitions. Implement pause, checkpoint or flush where required, revoke, assign, restore, and resume.

Tests should trigger rebalances during in-flight processing and state access. The diagnostics should make it clear which task owned which partition and what happened during the transition.

### Phase 10: Time, Windows, And Repartition Topics

Add event-time extraction, processing-time separation, tumbling windows, late-event policy, and repartition topics for key-changing operations.

Window tests should include recovery and crash cases, not only correct aggregation output.

### Phase 11: Observability And Incident Diagnostics

Add metrics, structured logging, OpenTelemetry activities, and health checks around the runtime states that operators need during incidents.

This phase should include tests that fail if important adverse scenarios do not produce actionable diagnostics.

### Phase 12: Backpressure

Add bounded queues where needed and propagate pressure back to Kafka consumption through pause/resume or controlled polling. Cover slow output, slow state, producer backpressure, and bounded memory behavior.

### Phase 13: Transactions And Stronger Guarantees

Evaluate Kafka transactions and idempotent producer behavior. If exactly-once becomes a target, define the constraints and failure cases explicitly before adding public API promises.

Tests should cover transaction abort, producer fencing, crash during transaction, and recovery.

### Phase 14: Operational Hardening Toward v1

Finalize the supported public API, delivery guarantees, state compatibility story, container deployment guidance, internal topic naming, package metadata, examples, benchmarks, and chaos/failure suite.

The project should not freeze public concepts until recovery, rebalance, observability, and failure semantics have been exercised against Kafka.

## First Design Priorities

The first implementation work should protect the decisions most likely to drift:

- public C# API versus private F# runtime
- `Confluent.Kafka` contained inside `Streamlinr.Core`
- failure behavior before API breadth
- state and checkpoint ordering
- container-safe recovery without persistent volumes
- Testcontainers Kafka integration tests
- incident-oriented diagnostics
