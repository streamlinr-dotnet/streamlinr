namespace Streamlinr

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

type internal SourceRecord(topic: string, partition: int, offset: int64, key: string, value: string, timestampUtc: Nullable<DateTimeOffset>) =
    member _.Topic = topic
    member _.Partition = partition
    member _.Offset = offset
    member _.Key = key
    member _.Value = value
    member _.TimestampUtc = timestampUtc

type internal SourceRecordBatch(batchId: Guid, sourceId: string, records: IReadOnlyList<SourceRecord>, receivedAtUtc: DateTimeOffset) =
    member _.BatchId = batchId
    member _.SourceId = sourceId
    member _.Records = records
    member _.ReceivedAtUtc = receivedAtUtc

type internal BatchProcessingResult(batchId: Guid, processedRecordCount: int) =
    member _.BatchId = batchId
    member _.ProcessedRecordCount = processedRecordCount

type internal RecordProcessor = delegate of SourceRecord * CancellationToken -> Task

type internal ProcessorPlan(processorId: string, sourceId: string, processor: RecordProcessor) =
    member _.ProcessorId = processorId
    member _.SourceId = sourceId
    member _.Processor = processor

type internal SourcePlan(sourceId: string, topic: string, keyTypeName: string, valueTypeName: string) =
    member _.SourceId = sourceId
    member _.Topic = topic
    member _.KeyTypeName = keyTypeName
    member _.ValueTypeName = valueTypeName

type internal TopologyPlan(topologyName: string, sources: IReadOnlyList<SourcePlan>, processors: IReadOnlyList<ProcessorPlan>) =
    member _.TopologyName = topologyName
    member _.Sources = sources
    member _.Processors = processors

type internal RuntimePlan(applicationId: string, bootstrapServers: string, topologies: IReadOnlyList<TopologyPlan>) =
    member _.ApplicationId = applicationId
    member _.BootstrapServers = bootstrapServers
    member _.Topologies = topologies

type internal ConsumerPlan(applicationId: string, bootstrapServers: string, topic: string, groupId: string, sourceId: string, maxBatchSize: int, pollTimeout: TimeSpan) =
    member _.ApplicationId = applicationId
    member _.BootstrapServers = bootstrapServers
    member _.Topic = topic
    member _.GroupId = groupId
    member _.SourceId = sourceId
    member _.MaxBatchSize = maxBatchSize
    member _.PollTimeout = pollTimeout

type internal RuntimeStatus =
    | Created
    | Starting
    | Running
    | Stopping
    | Stopped
    | Fatal of string

type internal ProcessorError =
    { ProcessorId: string
      BatchId: Guid
      Error: exn }

type internal RuntimeStartError =
    { Message: string }

type internal TopologyStartError =
    { Message: string }

type internal ConsumerStartError =
    { Message: string }

type internal ProcessorMessage =
    | ProcessBatch of SourceRecordBatch * AsyncReplyChannel<Result<BatchProcessingResult, ProcessorError>>
    | StopProcessor of AsyncReplyChannel<unit>

type internal TopologyStatus =
    | TopologyCreated
    | TopologyRunning
    | TopologyStopping
    | TopologyStopped
    | TopologyFatal of string

type internal TopologyMessage =
    | StartTopology of TopologyPlan * AsyncReplyChannel<Result<unit, TopologyStartError>>
    | SourceBatchReceived of SourceRecordBatch
    | SourceFailed of topic: string * error: exn
    | ProcessorFailed of processorId: string * error: exn
    | StopTopology of AsyncReplyChannel<unit>
    | GetTopologyStatus of AsyncReplyChannel<TopologyStatus>

type internal KafkaConsumerMessage =
    | StartConsuming of ConsumerPlan * target: Actor<TopologyMessage> * AsyncReplyChannel<Result<unit, ConsumerStartError>>
    | Poll
    | StopConsuming of AsyncReplyChannel<unit>

type internal RuntimeSupervisorMessage =
    | Start of RuntimePlan * AsyncReplyChannel<Result<unit, RuntimeStartError>>
    | Stop of AsyncReplyChannel<unit>
    | ChildFailed of role: string * error: exn
    | GetStatus of AsyncReplyChannel<RuntimeStatus>

[<RequireQualifiedAccess>]
module internal ProcessorActor =
    let start (processorPlan: ProcessorPlan) (cancellationToken: CancellationToken) =
        let running = Behaviour (fun (context: ActorContext<ProcessorMessage>) ->
            match context.Message with
            | ProcessBatch (batch, reply) ->
                task {
                    try
                        for record in batch.Records do
                            do! processorPlan.Processor.Invoke(record, cancellationToken)

                        reply.Reply(Ok(BatchProcessingResult(batch.BatchId, batch.Records.Count)))
                    with error ->
                        reply.Reply(Error { ProcessorId = processorPlan.ProcessorId; BatchId = batch.BatchId; Error = error })
                }
                |> ignore

                Handled
            | StopProcessor reply ->
                reply.Reply()
                Terminate)

        Actor.start running

[<RequireQualifiedAccess>]
module internal KafkaConsumerActor =
    let private toTimestampUtc (timestamp: Confluent.Kafka.Timestamp) =
        if timestamp.Type = Confluent.Kafka.TimestampType.NotAvailable then
            Nullable<DateTimeOffset>()
        else
            Nullable(DateTimeOffset(timestamp.UtcDateTime))

    let private toSourceRecord (result: Confluent.Kafka.ConsumeResult<string, string>) =
        SourceRecord(
            result.Topic,
            result.Partition.Value,
            result.Offset.Value,
            result.Message.Key,
            result.Message.Value,
            toTimestampUtc result.Message.Timestamp)

    let private buildConsumer (plan: ConsumerPlan) =
        let config =
            Confluent.Kafka.ConsumerConfig(
                BootstrapServers = plan.BootstrapServers,
                GroupId = plan.GroupId,
                AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
                EnableAutoCommit = true)

        Confluent.Kafka.ConsumerBuilder<string, string>(config).Build()

    let private tryCollectBatch (consumer: Confluent.Kafka.IConsumer<string, string>) (plan: ConsumerPlan) =
        try
            let records = ResizeArray<SourceRecord>()
            let mutable keepPolling = true

            while keepPolling && records.Count < plan.MaxBatchSize do
                let result = consumer.Consume(plan.PollTimeout)

                if isNull (box result) then
                    keepPolling <- false
                elif result.IsPartitionEOF then
                    keepPolling <- false
                else
                    records.Add(toSourceRecord result)

                    if plan.MaxBatchSize = 1 then
                        keepPolling <- false

            if records.Count = 0 then
                Ok None
            else
                Ok(Some(SourceRecordBatch(Guid.NewGuid(), plan.SourceId, records, DateTimeOffset.UtcNow)))
        with error ->
            Error error

    let start () =
        let rec notStarted = Behaviour(fun (context: ActorContext<KafkaConsumerMessage>) ->
            match context.Message with
            | StartConsuming (plan, target, reply) ->
                try
                    let consumer = buildConsumer plan
                    consumer.Subscribe plan.Topic
                    reply.Reply(Ok())
                    Actor.post Poll context.Self
                    Become(running plan target consumer)
                with error ->
                    reply.Reply(Error { Message = error.Message })
                    Terminate
            | StopConsuming reply ->
                reply.Reply()
                Terminate
            | _ -> Unhandled)

        and running plan target consumer = Behaviour (fun (context: ActorContext<KafkaConsumerMessage>) ->
            match context.Message with
            | Poll ->
                match tryCollectBatch consumer plan with
                | Ok (Some batch) ->
                    Actor.post (SourceBatchReceived batch) target
                    Actor.post Poll context.Self
                    Handled
                | Ok None ->
                    Actor.post Poll context.Self
                    Handled
                | Error error ->
                    Actor.post (SourceFailed(plan.Topic, error)) target
                    Terminate
            | StopConsuming reply ->
                consumer.Close()
                consumer.Dispose()
                reply.Reply()
                Terminate
            | _ -> Unhandled)

        Actor.start notStarted

[<RequireQualifiedAccess>]
module internal TopologyActor =
    let private validate (plan: TopologyPlan) : Result<unit, TopologyStartError> =
        if plan.Sources.Count <> 1 then
            Error({ Message = "Exactly one source is supported by the initial runtime." } : TopologyStartError)
        elif plan.Processors.Count = 0 then
            Error({ Message = "At least one processor is required." } : TopologyStartError)
        else
            Ok()

    let start (runtimePlan: RuntimePlan) (supervisor: Actor<RuntimeSupervisorMessage>) (cancellationToken: CancellationToken) =
        let rec created = Behaviour(fun (context: ActorContext<TopologyMessage>) ->
            match context.Message with
            | StartTopology (plan, reply) ->
                match validate plan with
                | Error error ->
                    reply.Reply(Error error)
                    Terminate
                | Ok () ->
                    let processors =
                        plan.Processors
                        |> Seq.map (fun processor -> processor.ProcessorId, ProcessorActor.start processor cancellationToken)
                        |> Map.ofSeq

                    let source = plan.Sources[0]
                    let consumer = KafkaConsumerActor.start ()
                    let consumerPlan = ConsumerPlan(runtimePlan.ApplicationId, runtimePlan.BootstrapServers, source.Topic, runtimePlan.ApplicationId, source.SourceId, 1, TimeSpan.FromMilliseconds 100.0)
                    let started = Actor.postAndAsyncReply (fun channel -> StartConsuming(consumerPlan, context.Self, channel)) consumer |> Async.RunSynchronously

                    match started with
                    | Ok () ->
                        reply.Reply(Ok())
                        Become(running processors consumer)
                    | Error error ->
                        reply.Reply(Error { Message = error.Message })
                        Terminate
            | GetTopologyStatus reply ->
                reply.Reply TopologyCreated
                Handled
            | StopTopology reply ->
                reply.Reply()
                Terminate
            | _ -> Unhandled)

        and running processors consumer = Behaviour(fun (context: ActorContext<TopologyMessage>) ->
            match context.Message with
            | SourceBatchReceived batch ->
                task {
                    for KeyValue (processorId, processor) in processors do
                        let! result = Actor.postAndAsyncReply (fun channel -> ProcessBatch(batch, channel)) processor |> Async.StartAsTask

                        match result with
                        | Ok _ -> ()
                        | Error error -> Actor.post (ProcessorFailed(processorId, error.Error)) context.Self
                }
                |> ignore

                Handled
            | SourceFailed (topic, error) ->
                Actor.post (ChildFailed($"source:{topic}", error)) supervisor
                Become(failed error.Message processors consumer)
            | ProcessorFailed (processorId, error) ->
                Actor.post (ChildFailed($"processor:{processorId}", error)) supervisor
                Become(failed error.Message processors consumer)
            | StopTopology reply ->
                Actor.postAndAsyncReply StopConsuming consumer |> Async.RunSynchronously

                for processor in processors.Values do
                    Actor.postAndAsyncReply StopProcessor processor |> Async.RunSynchronously

                reply.Reply()
                Terminate
            | GetTopologyStatus reply ->
                reply.Reply TopologyRunning
                Handled
            | _ -> Unhandled)

        and failed message processors consumer = Behaviour(fun (context: ActorContext<TopologyMessage>) ->
            match context.Message with
            | StopTopology reply ->
                Actor.postAndAsyncReply StopConsuming consumer |> Async.RunSynchronously

                for processor in processors.Values do
                    Actor.postAndAsyncReply StopProcessor processor |> Async.RunSynchronously

                reply.Reply()
                Terminate
            | GetTopologyStatus reply ->
                reply.Reply(TopologyFatal message)
                Handled
            | _ -> Handled)

        Actor.start created

[<RequireQualifiedAccess>]
module internal RuntimeSupervisor =
    let start (completion: TaskCompletionSource<exn option>) (cancellationToken: CancellationToken) =
        let rec created = Behaviour(fun (context: ActorContext<RuntimeSupervisorMessage>) ->
            match context.Message with
            | Start (plan, reply) ->
                if String.IsNullOrWhiteSpace plan.ApplicationId then
                    reply.Reply(Error { Message = "ApplicationId is required." })
                    Handled
                elif String.IsNullOrWhiteSpace plan.BootstrapServers then
                    reply.Reply(Error { Message = "BootstrapServers is required." })
                    Handled
                elif plan.Topologies.Count <> 1 then
                    reply.Reply(Error { Message = "Exactly one topology is supported by the initial runtime." })
                    Handled
                else
                    let topologyActor = TopologyActor.start plan context.Self cancellationToken
                    let started = Actor.postAndAsyncReply (fun channel -> StartTopology(plan.Topologies[0], channel)) topologyActor |> Async.RunSynchronously

                    match started with
                    | Ok () ->
                        reply.Reply(Ok())
                        Become(running topologyActor)
                    | Error error ->
                        reply.Reply(Error { Message = error.Message })
                        Handled
            | GetStatus reply ->
                reply.Reply Created
                Handled
            | Stop reply ->
                reply.Reply()
                completion.TrySetResult None |> ignore
                Terminate
            | _ -> Unhandled)

        and running topologyActor = Behaviour (fun (context: ActorContext<RuntimeSupervisorMessage>) ->
            match context.Message with
            | ChildFailed (_, error) ->
                completion.TrySetResult(Some error) |> ignore
                Become(fatal error.Message topologyActor)
            | GetStatus reply ->
                reply.Reply Running
                Handled
            | Stop reply ->
                Actor.postAndAsyncReply StopTopology topologyActor |> Async.RunSynchronously
                reply.Reply()
                completion.TrySetResult None |> ignore
                Terminate
            | _ -> Unhandled)

        and fatal message topologyActor = Behaviour (fun (context: ActorContext<RuntimeSupervisorMessage>) ->
            match context.Message with
            | GetStatus reply ->
                reply.Reply(Fatal message)
                Handled
            | Stop reply ->
                Actor.postAndAsyncReply StopTopology topologyActor |> Async.RunSynchronously
                reply.Reply()
                Terminate
            | _ -> Handled)

        Actor.start created

type internal Runtime =
    static member RunAsync(plan: RuntimePlan, cancellationToken: CancellationToken) : Task =
        task {
            let completion = TaskCompletionSource<exn option>(TaskCreationOptions.RunContinuationsAsynchronously)
            let supervisor = RuntimeSupervisor.start completion cancellationToken
            let! started = Actor.postAndAsyncReply (fun reply -> Start(plan, reply)) supervisor |> Async.StartAsTask

            match started with
            | Error error -> invalidOp error.Message
            | Ok () -> ()

            let cancellation = Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)
            let! completed = Task.WhenAny(completion.Task :> Task, cancellation)

            if Object.ReferenceEquals(completed, cancellation) then
                let! _ = Actor.postAndAsyncReply Stop supervisor |> Async.StartAsTask
                ()
            else
                let! completionResult = completion.Task

                match completionResult with
                | None -> ()
                | Some error -> raise error
        }
